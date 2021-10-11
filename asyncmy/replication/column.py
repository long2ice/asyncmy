import struct

from asyncmy.constants.FIELD_TYPE import (
    BIT,
    BLOB,
    DOUBLE,
    ENUM,
    FLOAT,
    GEOMETRY,
    JSON,
    NEWDECIMAL,
    SET,
    STRING,
    TINY,
    VAR_STRING,
    VARCHAR,
)
from asyncmy.replication.constants import DATETIME2, TIME2, TIMESTAMP2


class Column:
    """Definition of a column"""

    def __init__(self, column_type, column_schema, packet):
        self._parse_column_definition(column_type, column_schema, packet)

    def _parse_column_definition(self, column_type, column_schema, packet):
        self.type = column_type
        self.name = column_schema["COLUMN_NAME"]
        self.collation_name = column_schema["COLLATION_NAME"]
        self.character_set_name = column_schema["CHARACTER_SET_NAME"]
        self.comment = column_schema["COLUMN_COMMENT"]
        self.unsigned = column_schema["COLUMN_TYPE"].find("unsigned") != -1
        self.type_is_bool = False
        self.is_primary = column_schema["COLUMN_KEY"] == "PRI"

        if self.type == VARCHAR:
            self.max_length = struct.unpack("<H", packet.read(2))[0]
        elif self.type == DOUBLE:
            self.size = packet.read_uint8()
        elif self.type == FLOAT:
            self.size = packet.read_uint8()
        elif self.type == TIMESTAMP2:
            self.fsp = packet.read_uint8()
        elif self.type == DATETIME2:
            self.fsp = packet.read_uint8()
        elif self.type == TIME2:
            self.fsp = packet.read_uint8()
        elif self.type == TINY and column_schema["COLUMN_TYPE"] == "tinyint(1)":
            self.type_is_bool = True
        elif self.type == VAR_STRING or self.type == STRING:
            self._read_string_metadata(packet, column_schema)
        elif self.type == BLOB:
            self.length_size = packet.read_uint8()
        elif self.type == GEOMETRY:
            self.length_size = packet.read_uint8()
        elif self.type == JSON:
            self.length_size = packet.read_uint8()
        elif self.type == NEWDECIMAL:
            self.precision = packet.read_uint8()
            self.decimals = packet.read_uint8()
        elif self.type == BIT:
            bits = packet.read_uint8()
            bs = packet.read_uint8()
            self.bits = (bs * 8) + bits
            self.bytes = int((self.bits + 7) / 8)

    def _read_string_metadata(self, packet, column_schema):
        metadata = (packet.read_uint8() << 8) + packet.read_uint8()
        real_type = metadata >> 8
        if real_type == SET or real_type == ENUM:
            self.type = real_type
            self.size = metadata & 0x00FF
            self._read_enum_metadata(column_schema)
        else:
            self.max_length = (((metadata >> 4) & 0x300) ^ 0x300) + (metadata & 0x00FF)

    def _read_enum_metadata(self, column_schema):
        enums = column_schema["COLUMN_TYPE"]
        if self.type == ENUM:
            self.enum_values = [""] + enums.replace("enum(", "").replace(")", "").replace(
                "'", ""
            ).split(",")
        else:
            self.set_values = enums.replace("set(", "").replace(")", "").replace("'", "").split(",")

    @property
    def data(self):
        return dict((k, v) for (k, v) in self.__dict__.items() if not k.startswith("_"))
