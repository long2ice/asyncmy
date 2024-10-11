import struct

from asyncmy.constants.COLUMN import (
    NULL_COLUMN,
    UNSIGNED_CHAR_COLUMN,
    UNSIGNED_INT24_COLUMN,
    UNSIGNED_INT64_COLUMN,
    UNSIGNED_SHORT_COLUMN,
)
from asyncmy.replication import events, row_events
from asyncmy.replication.constants import (
    ANONYMOUS_GTID_LOG_EVENT,
    BEGIN_LOAD_QUERY_EVENT,
    DELETE_ROWS_EVENT_V1,
    DELETE_ROWS_EVENT_V2,
    EXECUTE_LOAD_QUERY_EVENT,
    FORMAT_DESCRIPTION_EVENT,
    GTID_LOG_EVENT,
    HEARTBEAT_LOG_EVENT,
    INTVAR_EVENT,
    JSONB_LITERAL_FALSE,
    JSONB_LITERAL_NULL,
    JSONB_LITERAL_TRUE,
    JSONB_TYPE_DOUBLE,
    JSONB_TYPE_INT16,
    JSONB_TYPE_INT32,
    JSONB_TYPE_INT64,
    JSONB_TYPE_LARGE_ARRAY,
    JSONB_TYPE_LARGE_OBJECT,
    JSONB_TYPE_LITERAL,
    JSONB_TYPE_SMALL_ARRAY,
    JSONB_TYPE_SMALL_OBJECT,
    JSONB_TYPE_STRING,
    JSONB_TYPE_UINT16,
    JSONB_TYPE_UINT32,
    JSONB_TYPE_UINT64,
    PREVIOUS_GTIDS_LOG_EVENT,
    QUERY_EVENT,
    ROTATE_EVENT,
    STOP_EVENT,
    TABLE_MAP_EVENT,
    UNSIGNED_INT24_LENGTH,
    UNSIGNED_INT64_LENGTH,
    UNSIGNED_SHORT_LENGTH,
    UPDATE_ROWS_EVENT_V1,
    UPDATE_ROWS_EVENT_V2,
    WRITE_ROWS_EVENT_V1,
    WRITE_ROWS_EVENT_V2,
    XID_EVENT,
)
from asyncmy.replication.utils import byte2int


class BinLogPacket:
    _event_map = {
        # event
        QUERY_EVENT: events.QueryEvent,
        ROTATE_EVENT: events.RotateEvent,
        FORMAT_DESCRIPTION_EVENT: events.FormatDescriptionEvent,
        XID_EVENT: events.XidEvent,
        INTVAR_EVENT: events.IntvarEvent,
        GTID_LOG_EVENT: events.GtidEvent,
        STOP_EVENT: events.StopEvent,
        BEGIN_LOAD_QUERY_EVENT: events.BeginLoadQueryEvent,
        EXECUTE_LOAD_QUERY_EVENT: events.ExecuteLoadQueryEvent,
        HEARTBEAT_LOG_EVENT: events.HeartbeatLogEvent,
        # row_event
        UPDATE_ROWS_EVENT_V1: row_events.UpdateRowsEvent,
        WRITE_ROWS_EVENT_V1: row_events.WriteRowsEvent,
        DELETE_ROWS_EVENT_V1: row_events.DeleteRowsEvent,
        UPDATE_ROWS_EVENT_V2: row_events.UpdateRowsEvent,
        WRITE_ROWS_EVENT_V2: row_events.WriteRowsEvent,
        DELETE_ROWS_EVENT_V2: row_events.DeleteRowsEvent,
        TABLE_MAP_EVENT: row_events.TableMapEvent,
        # 5.6 GTID enabled replication events
        ANONYMOUS_GTID_LOG_EVENT: events.NotImplementedEvent,
        PREVIOUS_GTIDS_LOG_EVENT: events.NotImplementedEvent,
    }

    def __init__(
        self,
        packet,
        table_map,
        connection,
        use_checksum,
        allowed_events,
        only_tables,
        ignored_tables,
        only_schemas,
        ignored_schemas,
        freeze_schema,
    ):
        self.read_bytes = 0
        self._data_buffer = b""
        self._packet = packet

        # OK value
        # timestamp
        # event_type
        # server_id
        # log_pos
        # flags
        unpack = struct.unpack("<cIBIIIH", self._packet.read(20))

        # Header
        self.timestamp = unpack[1]
        self.event_type = unpack[2]
        self.server_id = unpack[3]
        self.event_size = unpack[4]
        # position of the next event
        self.log_pos = unpack[5]
        self.flags = unpack[6]

        # MySQL 5.6 and more if binlog-checksum = CRC32
        if use_checksum:
            event_size_without_header = self.event_size - 23
        else:
            event_size_without_header = self.event_size - 19

        self.event = None
        event_class = self._event_map.get(self.event_type, events.NotImplementedEvent)

        if event_class not in allowed_events:
            return
        self.event = event_class(
            self,
            event_size_without_header,
            table_map,
            connection,
            only_tables=only_tables,
            ignored_tables=ignored_tables,
            only_schemas=only_schemas,
            ignored_schemas=ignored_schemas,
            freeze_schema=freeze_schema,
        )
        if self.event.processed is False:
            self.event = None

    async def init(self):
        self.event and await self.event.init()

    def read(self, size):
        size = int(size)
        self.read_bytes += size
        if len(self._data_buffer) > 0:
            data = self._data_buffer[:size]
            self._data_buffer = self._data_buffer[size:]
            if len(data) == size:
                return data
            else:
                return data + self._packet.read(size - len(data))
        return self._packet.read(size)

    def unread(self, data):
        self.read_bytes -= len(data)
        self._data_buffer += data

    def advance(self, size):
        size = int(size)
        self.read_bytes += size
        buffer_len = len(self._data_buffer)
        if buffer_len > 0:
            self._data_buffer = self._data_buffer[size:]
            if size > buffer_len:
                self._packet.advance(size - buffer_len)
        else:
            self._packet.advance(size)

    def read_length_coded_binary(self):
        c = byte2int(self.read(1))
        if c == NULL_COLUMN:
            return None
        if c < UNSIGNED_CHAR_COLUMN:
            return c
        elif c == UNSIGNED_SHORT_COLUMN:
            return self.unpack_uint16(self.read(UNSIGNED_SHORT_LENGTH))
        elif c == UNSIGNED_INT24_COLUMN:
            return self.unpack_int24(self.read(UNSIGNED_INT24_LENGTH))
        elif c == UNSIGNED_INT64_COLUMN:
            return self.unpack_int64(self.read(UNSIGNED_INT64_LENGTH))

    def read_length_coded_string(self):
        """Read a 'Length Coded String' from the data buffer.

        A 'Length Coded String' consists first of a length coded
        (unsigned, positive) integer represented in 1-9 bytes followed by
        that many bytes of binary data.  (For example "cat" would be "3cat".)

        From PyMYSQL source code
        """
        length = self.read_length_coded_binary()
        if length is None:
            return None
        return self.read(length).decode()

    def __getattr__(self, key):
        if hasattr(self._packet, key):
            return getattr(self._packet, key)

        raise AttributeError("%s instance has no attribute '%s'" % (self.__class__, key))

    def read_int_be_by_size(self, size):
        """Read a big endian integer values based on byte number"""
        if size == 1:
            return struct.unpack(">b", self.read(size))[0]
        elif size == 2:
            return struct.unpack(">h", self.read(size))[0]
        elif size == 3:
            return self.read_int24_be()
        elif size == 4:
            return struct.unpack(">i", self.read(size))[0]
        elif size == 5:
            return self.read_int40_be()
        elif size == 8:
            return struct.unpack(">l", self.read(size))[0]

    def read_uint_by_size(self, size):
        """Read a little endian integer values based on byte number"""
        if size == 1:
            return self.read_uint8()
        elif size == 2:
            return self.read_uint16()
        elif size == 3:
            return self.read_uint24()
        elif size == 4:
            return self.read_uint32()
        elif size == 5:
            return self.read_uint40()
        elif size == 6:
            return self.read_uint48()
        elif size == 7:
            return self.read_uint56()
        elif size == 8:
            return self.read_uint64()

    def read_length_coded_pascal_string(self, size):
        """Read a string with length coded using pascal style.
        The string start by the size of the string
        """
        length = self.read_uint_by_size(size)
        return self.read(length)

    def read_variable_length_string(self):
        """Read a variable length string where the first 1-5 bytes stores the
        length of the string.

        For each byte, the first bit being high indicates another byte must be
        read.
        """
        byte = 0x80
        length = 0
        bits_read = 0
        while byte & 0x80 != 0:
            byte = struct.unpack("!B", self.read(1))[0]
            length = length | ((byte & 0x7F) << bits_read)
            bits_read = bits_read + 7
        return self.read(length)

    def read_int24(self):
        a, b, c = struct.unpack("BBB", self.read(3))
        res = a | (b << 8) | (c << 16)
        if res >= 0x800000:
            res -= 0x1000000
        return res

    def read_int24_be(self):
        a, b, c = struct.unpack("BBB", self.read(3))
        res = (a << 16) | (b << 8) | c
        if res >= 0x800000:
            res -= 0x1000000
        return res

    def read_uint8(self):
        return struct.unpack("<B", self.read(1))[0]

    def read_int16(self):
        return struct.unpack("<h", self.read(2))[0]

    def read_uint16(self):
        return struct.unpack("<H", self.read(2))[0]

    def read_uint24(self):
        a, b, c = struct.unpack("<BBB", self.read(3))
        return a + (b << 8) + (c << 16)

    def read_uint32(self):
        return struct.unpack("<I", self.read(4))[0]

    def read_int32(self):
        return struct.unpack("<i", self.read(4))[0]

    def read_uint40(self):
        a, b = struct.unpack("<BI", self.read(5))
        return a + (b << 8)

    def read_int40_be(self):
        a, b = struct.unpack(">IB", self.read(5))
        return b + (a << 8)

    def read_uint48(self):
        a, b, c = struct.unpack("<HHH", self.read(6))
        return a + (b << 16) + (c << 32)

    def read_uint56(self):
        a, b, c = struct.unpack("<BHI", self.read(7))
        return a + (b << 8) + (c << 24)

    def read_uint64(self):
        return struct.unpack("<Q", self.read(8))[0]

    def read_int64(self):
        return struct.unpack("<q", self.read(8))[0]

    def unpack_uint16(self, n):
        return struct.unpack("<H", n[0:2])[0]

    def unpack_int24(self, n):
        try:
            return (
                struct.unpack("B", n[0])[0]
                + (struct.unpack("B", n[1])[0] << 8)
                + (struct.unpack("B", n[2])[0] << 16)
            )
        except TypeError:
            return n[0] + (n[1] << 8) + (n[2] << 16)

    def unpack_int32(self, n):
        try:
            return (
                struct.unpack("B", n[0])[0]
                + (struct.unpack("B", n[1])[0] << 8)
                + (struct.unpack("B", n[2])[0] << 16)
                + (struct.unpack("B", n[3])[0] << 24)
            )
        except TypeError:
            return n[0] + (n[1] << 8) + (n[2] << 16) + (n[3] << 24)

    def read_binary_json(self, size):
        length = self.read_uint_by_size(size)
        payload = self.read(length)
        self.unread(payload)
        t = self.read_uint8()

        return self.read_binary_json_type(t, length)

    def read_binary_json_type(self, t, length):
        large = t in (JSONB_TYPE_LARGE_OBJECT, JSONB_TYPE_LARGE_ARRAY)
        if t in (JSONB_TYPE_SMALL_OBJECT, JSONB_TYPE_LARGE_OBJECT):
            return self.read_binary_json_object(length - 1, large)
        elif t in (JSONB_TYPE_SMALL_ARRAY, JSONB_TYPE_LARGE_ARRAY):
            return self.read_binary_json_array(length - 1, large)
        elif t in (JSONB_TYPE_STRING,):
            return self.read_variable_length_string()
        elif t in (JSONB_TYPE_LITERAL,):
            value = self.read_uint8()
            if value == JSONB_LITERAL_NULL:
                return None
            elif value == JSONB_LITERAL_TRUE:
                return True
            elif value == JSONB_LITERAL_FALSE:
                return False
        elif t == JSONB_TYPE_INT16:
            return self.read_int16()
        elif t == JSONB_TYPE_UINT16:
            return self.read_uint16()
        elif t in (JSONB_TYPE_DOUBLE,):
            return struct.unpack("<d", self.read(8))[0]
        elif t == JSONB_TYPE_INT32:
            return self.read_int32()
        elif t == JSONB_TYPE_UINT32:
            return self.read_uint32()
        elif t == JSONB_TYPE_INT64:
            return self.read_int64()
        elif t == JSONB_TYPE_UINT64:
            return self.read_uint64()

        raise ValueError("Json type %d is not handled" % t)

    def read_binary_json_type_inlined(self, t, large):
        if t == JSONB_TYPE_LITERAL:
            value = self.read_uint32() if large else self.read_uint16()
            if value == JSONB_LITERAL_NULL:
                return None
            elif value == JSONB_LITERAL_TRUE:
                return True
            elif value == JSONB_LITERAL_FALSE:
                return False
        elif t == JSONB_TYPE_INT16:
            return self.read_int32() if large else self.read_int16()
        elif t == JSONB_TYPE_UINT16:
            return self.read_uint32() if large else self.read_uint16()
        elif t == JSONB_TYPE_INT32:
            return self.read_int64() if large else self.read_int32()
        elif t == JSONB_TYPE_UINT32:
            return self.read_uint64() if large else self.read_uint32()

        raise ValueError("Json type %d is not handled" % t)

    def read_binary_json_object(self, length, large):
        if large:
            elements = self.read_uint32()
            size = self.read_uint32()
        else:
            elements = self.read_uint16()
            size = self.read_uint16()

        if size > length:
            raise ValueError("Json length is larger than packet length")

        if large:
            key_offset_lengths = [
                (
                    self.read_uint32(),  # offset (we don't actually need that)
                    self.read_uint16(),  # size of the key
                )
                for _ in range(elements)
            ]
        else:
            key_offset_lengths = [
                (
                    self.read_uint16(),  # offset (we don't actually need that)
                    self.read_uint16(),  # size of key
                )
                for _ in range(elements)
            ]

        value_type_inlined_lengths = [
            self.read_offset_or_inline(self, large) for _ in range(elements)
        ]

        keys = [self.read(x[1]) for x in key_offset_lengths]

        out = {}
        for i in range(elements):
            if value_type_inlined_lengths[i][1] is None:
                data = value_type_inlined_lengths[i][2]
            else:
                t = value_type_inlined_lengths[i][0]
                data = self.read_binary_json_type(t, length)
            out[keys[i]] = data

        return out

    def read_binary_json_array(self, length, large):
        if large:
            elements = self.read_uint32()
            size = self.read_uint32()
        else:
            elements = self.read_uint16()
            size = self.read_uint16()

        if size > length:
            raise ValueError("Json length is larger than packet length")

        values_type_offset_inline = [
            self.read_offset_or_inline(self, large) for _ in range(elements)
        ]

        def _read(x):
            if x[1] is None:
                return x[2]
            return self.read_binary_json_type(x[0], length)

        return [_read(x) for x in values_type_offset_inline]

    @staticmethod
    def read_offset_or_inline(packet, large):
        t = packet.read_uint8()

        if t in (JSONB_TYPE_LITERAL, JSONB_TYPE_INT16, JSONB_TYPE_UINT16):
            return (t, None, packet.read_binary_json_type_inlined(t, large))
        if large and t in (JSONB_TYPE_INT32, JSONB_TYPE_UINT32):
            return (t, None, packet.read_binary_json_type_inlined(t, large))

        if large:
            return (t, packet.read_uint32(), None)
        return (t, packet.read_uint16(), None)
