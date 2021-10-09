import binascii
import struct

from asyncmy.replication.utils import byte2int, int2byte


class BinLogEvent:
    def __init__(
        self,
        from_packet,
        event_size,
        table_map,
        connection,
        only_tables=None,
        ignored_tables=None,
        only_schemas=None,
        ignored_schemas=None,
        freeze_schema=None,
        fail_on_table_metadata_unavailable=False,
    ):
        self.packet = from_packet
        self.table_map = table_map
        self.event_type = self.packet.event_type
        self.timestamp = self.packet.timestamp
        self.event_size = event_size
        self._connection = connection
        self._fail_on_table_metadata_unavailable = fail_on_table_metadata_unavailable
        # The event have been fully processed, if processed is false
        # the event will be skipped
        self._processed = True
        self.complete = True

    @property
    def processed(self):
        return self._processed

    def _read_table_id(self):
        # Table ID is 6 byte
        # pad little-endian number
        table_id = self.packet.read(6) + int2byte(0) + int2byte(0)
        return struct.unpack("<Q", table_id)[0]

    async def init(self):
        pass


class GtidEvent(BinLogEvent):
    """GTID change in binlog event"""

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(GtidEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )

        self.commit_flag = self.packet.read(1) == 1
        self.sid = self.packet.read(16)
        self.gno = struct.unpack("<Q", self.packet.read(8))[0]

    @property
    def gtid(self):
        """GTID = source_id:transaction_id
        Eg: 3E11FA47-71CA-11E1-9E33-C80AA9429562:23
        See: http://dev.mysql.com/doc/refman/5.6/en/replication-gtids-concepts.html"""
        nibbles = binascii.hexlify(self.sid).decode("ascii")
        gtid = "%s-%s-%s-%s-%s:%d" % (
            nibbles[:8],
            nibbles[8:12],
            nibbles[12:16],
            nibbles[16:20],
            nibbles[20:],
            self.gno,
        )
        return gtid


class RotateEvent(BinLogEvent):
    """Change MySQL bin log file

    Attributes:
        position: Position inside next binlog
        next_binlog: Name of next binlog file
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(RotateEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )
        self.position = struct.unpack("<Q", self.packet.read(8))[0]
        self.next_binlog = self.packet.read(event_size - 8).decode()


class FormatDescriptionEvent(BinLogEvent):
    pass


class StopEvent(BinLogEvent):
    pass


class XidEvent(BinLogEvent):
    """A COMMIT event

    Attributes:
        xid: Transaction ID for 2PC
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(XidEvent, self).__init__(from_packet, event_size, table_map, ctl_connection, **kwargs)
        self.xid = struct.unpack("<Q", self.packet.read(8))[0]


class HeartbeatLogEvent(BinLogEvent):
    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(HeartbeatLogEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )
        self.ident = self.packet.read(event_size).decode()


class QueryEvent(BinLogEvent):
    """This evenement is trigger when a query is run of the database.
    Only replicated queries are logged."""

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(QueryEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )

        # Post-header
        self.slave_proxy_id = self.packet.read_uint32()
        self.execution_time = self.packet.read_uint32()
        self.schema_length = byte2int(self.packet.read(1))
        self.error_code = self.packet.read_uint16()
        self.status_vars_length = self.packet.read_uint16()

        # Payload
        self.status_vars = self.packet.read(self.status_vars_length)
        self.schema = self.packet.read(self.schema_length)
        self.packet.advance(1)

        self.query = self.packet.read(
            event_size - 13 - self.status_vars_length - self.schema_length - 1
        ).decode("utf-8")
        # string[EOF]    query


class BeginLoadQueryEvent(BinLogEvent):
    """

    Attributes:
        file_id
        block-data
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(BeginLoadQueryEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )

        # Payload
        self.file_id = self.packet.read_uint32()
        self.block_data = self.packet.read(event_size - 4)


class ExecuteLoadQueryEvent(BinLogEvent):
    """

    Attributes:
        slave_proxy_id
        execution_time
        schema_length
        error_code
        status_vars_length

        file_id
        start_pos
        end_pos
        dup_handling_flags
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(ExecuteLoadQueryEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )

        # Post-header
        self.slave_proxy_id = self.packet.read_uint32()
        self.execution_time = self.packet.read_uint32()
        self.schema_length = self.packet.read_uint8()
        self.error_code = self.packet.read_uint16()
        self.status_vars_length = self.packet.read_uint16()

        # Payload
        self.file_id = self.packet.read_uint32()
        self.start_pos = self.packet.read_uint32()
        self.end_pos = self.packet.read_uint32()
        self.dup_handling_flags = self.packet.read_uint8()


class IntvarEvent(BinLogEvent):
    """

    Attributes:
        type
        value
    """

    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(IntvarEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )

        # Payload
        self.type = self.packet.read_uint8()
        self.value = self.packet.read_uint32()


class NotImplementedEvent(BinLogEvent):
    def __init__(self, from_packet, event_size, table_map, ctl_connection, **kwargs):
        super(NotImplementedEvent, self).__init__(
            from_packet, event_size, table_map, ctl_connection, **kwargs
        )
        self.packet.advance(event_size)
