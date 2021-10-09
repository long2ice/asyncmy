
from .constants.COLUMN import (NULL_COLUMN, UNSIGNED_CHAR_COLUMN,
                               UNSIGNED_INT24_COLUMN, UNSIGNED_INT64_COLUMN,
                               UNSIGNED_SHORT_COLUMN)
from .constants.FIELD_TYPE import VAR_STRING
from .constants.SERVER_STATUS import SERVER_MORE_RESULTS_EXISTS
from .structs import HB, H, I, Q

include "charset.pxd"
from . import errors, structs


cdef class MysqlPacket:
    """
    Representation of a MySQL response packet.
    Provides an interface for reading/parsing the packet results.
    """
    cdef:
        bytes _data
        int _position

    def __init__(self, bytes data, str encoding):
        self._position = 0
        self._data = data

    cpdef bytes get_all_data(self):
        return self._data

    cpdef bytes read(self, int size):
        """
        Read the first 'size' bytes in packet and advance cursor past them.
        :param size: 
        :return: 
        """
        cdef bytes result = self._data[self._position: self._position + size]
        if len(result) != size:
            error = (
                    "Result length not requested length:\n"
                    "Expected=%s.  Actual=%s.  Position: %s.  Data Length: %s"
                    % (size, len(result), self._position, len(self._data))
            )
            raise AssertionError(error)
        self._position += size
        return result

    cpdef bytes read_all(self):
        """Read all remaining data in the packet.

        (Subsequent read() will return errors.)
        """
        cdef bytes result = self._data[self._position:]
        self._position = 0
        return result

    cpdef advance(self, int length):
        """
        Advance the cursor in data buffer 'length' bytes.
        """
        cdef int new_position = self._position + length
        if new_position < 0 or new_position > len(self._data):
            raise Exception(
                "Invalid advance amount (%s) for cursor.  "
                "Position=%s" % (length, new_position)
            )
        self._position = new_position

    cpdef rewind(self, int position=0):
        """
        Set the position of the data buffer cursor to 'position'.
        """
        if position < 0 or position > len(self._data):
            raise Exception("Invalid position to rewind cursor to: %s." % position)
        self._position = position

    cpdef bytes get_bytes(self, int position, int length=1):
        """
        Get 'length' bytes starting at 'position'.

        Position is start of payload (first four packet header bytes are not
        included) starting at index '0'.

        No error checking is done.  If requesting outside end of buffer
        an empty string (or string shorter than 'length') may be returned!
        """
        return self._data[position: (position + length)]

    cpdef int read_uint8(self):
        cdef int result = self._data[self._position]
        self._position += 1
        return result

    cpdef int read_uint16(self):
        cdef int result = H.unpack_from(self._data, self._position)[0]
        self._position += 2
        return result

    cpdef int read_uint24(self):
        cdef tuple result = HB.unpack_from(self._data, self._position)
        self._position += 3
        return result[0] + (result[1] << 16)

    cpdef int read_uint32(self):
        cdef int result = I.unpack_from(self._data, self._position)[0]
        self._position += 4
        return result

    cpdef unsigned long read_uint64(self):
        cdef unsigned long result = Q.unpack_from(self._data, self._position)[0]
        self._position += 8
        return result

    cpdef bytes read_string(self):
        cdef int end_pos = self._data.find(b"\0", self._position)
        if end_pos < 0:
            return None
        cdef bytes result = self._data[self._position: end_pos]
        self._position = end_pos + 1
        return result

    cpdef read_length_encoded_integer(self):
        """
        Read a 'Length Coded Binary' number from the data buffer.

        Length coded numbers can be anywhere from 1 to 9 bytes depending
        on the value of the first byte.
        """
        cdef int c = self.read_uint8()
        if c == NULL_COLUMN:
            return None
        if c < UNSIGNED_CHAR_COLUMN:
            return c
        elif c == UNSIGNED_SHORT_COLUMN:
            return self.read_uint16()
        elif c == UNSIGNED_INT24_COLUMN:
            return self.read_uint24()
        elif c == UNSIGNED_INT64_COLUMN:
            return self.read_uint64()

    cpdef read_length_coded_string(self):
        """
        Read a 'Length Coded String' from the data buffer.

        A 'Length Coded String' consists first of a length coded
        (unsigned, positive) integer represented in 1-9 bytes followed by
        that many bytes of binary data.  (For example "cat" would be "3cat".)
        """
        length = self.read_length_encoded_integer()
        if length is None:
            return None
        return self.read(length)

    cpdef tuple read_struct(self, str fmt):
        s = getattr(structs, fmt[1:])
        result = s.unpack_from(self._data, self._position)
        self._position += len(result)
        return tuple(result)

    cpdef int is_ok_packet(self):
        # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
        return self._data[0] == 0 and len(self._data) >= 7

    cpdef int is_eof_packet(self):
        # http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet
        # Caution: \xFE may be LengthEncodedInteger.
        # If \xFE is LengthEncodedInteger header, 8bytes followed.
        return self._data[0] == 0xFE and len(self._data) < 9

    cpdef int is_auth_switch_request(self):
        # http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
        return self._data[0] == 0xFE

    cpdef int is_extra_auth_data(self):
        # https://dev.mysql.com/doc/internals/en/successful-authentication.html
        return self._data[0] == 1

    cpdef int is_resultset_packet(self):
        field_count = self._data[0]
        return 1 <= field_count <= 250

    cpdef int is_load_local_packet(self):
        return self._data[0] == 0xFB

    cpdef int is_error_packet(self):
        return self._data[0] == 0xFF

    def check_error(self):
        if self.is_error_packet():
            self.raise_for_error()

    cpdef raise_for_error(self):
        self.rewind()
        self.advance(1)  # field_count == error (we already know that)
        errno = self.read_uint16()
        errors.raise_mysql_exception(self._data)

cdef class FieldDescriptorPacket(MysqlPacket):
    """
    A MysqlPacket that represents a specific column's metadata in the result.

    Parsing is automatically done and the results are exported via public
    attributes on the class such as: db, table_name, name, length, type_code.
    """
    cdef:
        bytes catalog, db
        public str table_name, org_table, name, org_name
        public long long charsetnr, length, type_code, flags, scale

    def __init__(self, bytes data, str encoding):
        super(FieldDescriptorPacket, self).__init__(data, encoding)
        self._parse_field_descriptor(encoding)

    cdef _parse_field_descriptor(self, str encoding):
        """
        Parse the 'Field Descriptor' (Metadata) packet.

        This is compatible with MySQL 4.1+ (not compatible with MySQL 4.0).
        """
        self.catalog = self.read_length_coded_string()
        self.db = self.read_length_coded_string()
        self.table_name = self.read_length_coded_string().decode(encoding)
        self.org_table = self.read_length_coded_string().decode(encoding)
        self.name = self.read_length_coded_string().decode(encoding)
        self.org_name = self.read_length_coded_string().decode(encoding)
        (
            self.charsetnr,
            self.length,
            self.type_code,
            self.flags,
            self.scale,
        ) = self.read_struct("<xHIBHBxx")

    cpdef description(self):
        """Provides a 7-item tuple compatible with the Python PEP249 DB Spec."""
        return (
            self.name,
            self.type_code,
            None,  # TODO: display_length; should this be self.length?
            self.get_column_length(),  # 'internal_size'
            self.get_column_length(),  # 'precision'  # TODO: why!?!?
            self.scale,
            self.flags % 2 == 0,
        )

    cdef int get_column_length(self):
        if self.type_code == VAR_STRING:
            mb_len = MB_LENGTH.get(self.charsetnr, 1)
            return self.length // mb_len
        return self.length

    def __str__(self):
        return "%s %r.%r.%r, type=%s, flags=%x" % (
            self.__class__,
            self.db,
            self.table_name,
            self.name,
            self.type_code,
            self.flags,
        )

cdef class OKPacketWrapper:
    """
    OK Packet Wrapper. It uses an existing packet object, and wraps
    around it, exposing useful variables while still providing access
    to the original packet objects variables and methods.
    """
    cdef:
        MysqlPacket packet
        public int affected_rows, server_status, warning_count, has_next
        public bytes message
        public unsigned long insert_id

    def __init__(self, MysqlPacket from_packet):
        if not from_packet.is_ok_packet():
            raise ValueError(
                "Cannot create "
                + str(self.__class__.__name__)
                + " object from invalid packet type"
            )

        self.packet = from_packet
        self.packet.advance(1)

        self.affected_rows = self.packet.read_length_encoded_integer()
        self.insert_id = self.packet.read_length_encoded_integer()

        self.server_status, self.warning_count = self.read_struct("<HH")
        self.message = self.packet.read_all()
        self.has_next = self.server_status & SERVER_MORE_RESULTS_EXISTS

    def __getattr__(self, key):
        return getattr(self.packet, key)

cdef class EOFPacketWrapper:
    """
    EOF Packet Wrapper. It uses an existing packet object, and wraps
    around it, exposing useful variables while still providing access
    to the original packet objects variables and methods.
    """
    cdef:
        MysqlPacket packet
        public int server_status, warning_count, has_next

    def __init__(self, MysqlPacket from_packet):
        if not from_packet.is_eof_packet():
            raise ValueError(
                f"Cannot create '{self.__class__}' object from invalid packet type"
            )

        self.packet = from_packet
        self.warning_count, self.server_status = self.packet.read_struct("<xhh")
        self.has_next = self.server_status & SERVER_MORE_RESULTS_EXISTS

    def __getattr__(self, key):
        return getattr(self.packet, key)

cdef class LoadLocalPacketWrapper:
    """
    Load Local Packet Wrapper. It uses an existing packet object, and wraps
    around it, exposing useful variables while still providing access
    to the original packet objects variables and methods.
    """

    def __init__(self, MysqlPacket from_packet):
        if not from_packet.is_load_local_packet():
            raise ValueError(
                f"Cannot create '{self.__class__}' object from invalid packet type"
            )

        self.packet = from_packet
        self.filename = self.packet.get_all_data()[1:]

    def __getattr__(self, key):
        return getattr(self.packet, key)
