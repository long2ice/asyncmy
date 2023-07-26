import struct
from typing import Any, Dict, List, Optional, Set, Type, Union

from asyncmy import Connection
from asyncmy.constants.COMMAND import COM_BINLOG_DUMP, COM_BINLOG_DUMP_GTID, COM_REGISTER_SLAVE
from asyncmy.cursors import DictCursor
from asyncmy.errors import OperationalError
from asyncmy.replication.constants import (
    BINLOG_DUMP_NON_BLOCK,
    BINLOG_THROUGH_GTID,
    MAX_HEARTBEAT,
    ROTATE_EVENT,
    TABLE_MAP_EVENT,
)
from asyncmy.replication.errors import BinLogNotEnabledError
from asyncmy.replication.events import (
    BeginLoadQueryEvent,
    BinLogEvent,
    ExecuteLoadQueryEvent,
    FormatDescriptionEvent,
    GtidEvent,
    HeartbeatLogEvent,
    NotImplementedEvent,
    QueryEvent,
    RotateEvent,
    StopEvent,
    XidEvent,
)
from asyncmy.replication.gtid import Gtid, GtidSet
from asyncmy.replication.packets import BinLogPacket
from asyncmy.replication.row_events import (
    DeleteRowsEvent,
    TableMapEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)


class ReportSlave:
    def __init__(self, value: Union[str, tuple, dict]):
        self._hostname = ""
        self._username = ""
        self._password = ""  # nosec: B105
        self._port = 0
        if isinstance(value, (tuple, list)):
            try:
                self._hostname = value[0]
                self._username = value[1]
                self._password = value[2]
                self._port = int(value[3])
            except IndexError:
                pass
        elif isinstance(value, dict):
            for key in ["hostname", "username", "password", "port"]:
                try:
                    setattr(self, key, value[key])
                except KeyError:
                    pass
        else:
            self._hostname = value

    def encoded(self, server_id: int, master_id: int = 0):
        len_hostname = len(self._hostname.encode())
        len_username = len(self._username.encode())
        len_password = len(self._password.encode())

        packet_len = (
            1
            + 4  # command
            + 1  # server-id
            + len_hostname  # hostname length
            + 1
            + len_username  # username length
            + 1
            + len_password  # password length
            + 2
            + 4  # slave mysql port
            + 4  # replication rank
        )  # master-id

        max_string_len = 257  # one byte for length + 256 chars

        return (
            struct.pack("<i", packet_len)
            + struct.pack("!B", COM_REGISTER_SLAVE)
            + struct.pack("<L", server_id)
            + struct.pack("<%dp" % min(max_string_len, len_hostname + 1), self._hostname.encode())
            + struct.pack("<%dp" % min(max_string_len, len_username + 1), self._username.encode())
            + struct.pack("<%dp" % min(max_string_len, len_password + 1), self._password.encode())
            + struct.pack("<H", self._port)
            + struct.pack("<l", 0)
            + struct.pack("<l", master_id)
        )


class BinLogStream:
    MYSQL_EXPECTED_ERROR_CODES = [2013, 2006]

    def __init__(
        self,
        connection: Connection,
        ctl_connection: Connection,
        server_id: int,
        slave_uuid: Optional[str] = None,
        slave_heartbeat: Optional[int] = None,
        report_slave: Optional[Union[str, tuple, dict]] = None,
        master_log_file: Optional[str] = None,
        master_log_position: Optional[int] = None,
        master_auto_position: Optional[Set[Gtid]] = None,
        resume_stream: bool = False,
        blocking: bool = False,
        skip_to_timestamp: Optional[int] = None,
        only_events: Optional[List[Type[BinLogEvent]]] = None,
        ignored_events: Optional[List[Type[BinLogEvent]]] = None,
        filter_non_implemented_events: bool = True,
        only_tables: Optional[List[str]] = None,
        ignored_tables: Optional[List[str]] = None,
        only_schemas: Optional[List[str]] = None,
        ignored_schemas: Optional[List[str]] = None,
        freeze_schema: bool = False,
    ):
        self._freeze_schema = freeze_schema
        self._ignored_schemas = ignored_schemas
        self._only_schemas = only_schemas
        self._ignored_tables = ignored_tables
        self._only_tables = only_tables
        self._skip_to_timestamp = skip_to_timestamp
        self._blocking = blocking
        self._resume_stream = resume_stream
        self._master_auto_position = master_auto_position
        self._master_log_position = master_log_position
        self._master_log_file = master_log_file
        self._server_id = server_id
        self._slave_heartbeat = slave_heartbeat
        self._slave_uuid = slave_uuid
        self._connection = connection
        self._ctl_connection = ctl_connection
        self._ctl_connection._get_table_information = self._get_table_information
        self._use_checksum = False
        self._connected = False
        self._report_slave = None
        if report_slave:
            self._report_slave = ReportSlave(report_slave)
        self._allowed_events = self._allowed_event_list(
            only_events, ignored_events, filter_non_implemented_events
        )
        self._allowed_events_in_packet = [
            TableMapEvent,
            RotateEvent,
            *self._allowed_events,
        ]
        self._table_map: Dict[str, Any] = {}

    @staticmethod
    def _allowed_event_list(
        only_events: Optional[List[Type[BinLogEvent]]],
        ignored_events: Optional[List[Type[BinLogEvent]]],
        filter_non_implemented_events: bool,
    ):
        if only_events is not None:
            events = set(only_events)
        else:
            events = {
                QueryEvent,
                RotateEvent,
                StopEvent,
                FormatDescriptionEvent,
                XidEvent,
                GtidEvent,
                BeginLoadQueryEvent,
                ExecuteLoadQueryEvent,
                UpdateRowsEvent,
                WriteRowsEvent,
                DeleteRowsEvent,
                TableMapEvent,
                HeartbeatLogEvent,
                NotImplementedEvent,
            }
        if ignored_events is not None:
            for e in ignored_events:
                events.remove(e)
        if filter_non_implemented_events:
            try:
                events.remove(NotImplementedEvent)
            except KeyError:
                pass
        return frozenset(events)

    async def _connect(self):
        await self._connection.connect()
        self._use_checksum = await self._checksum_enable()
        async with self._connection.cursor() as cursor:
            if self._use_checksum:
                await cursor.execute("set @master_binlog_checksum= @@global.binlog_checksum")
            if self._slave_uuid:
                await cursor.execute(f"set @slave_uuid= '{self._slave_uuid}'")
            if self._slave_heartbeat:
                heartbeat = float(min(MAX_HEARTBEAT / 2.0, self._slave_heartbeat))
                if heartbeat > MAX_HEARTBEAT:
                    heartbeat = MAX_HEARTBEAT
                heartbeat = int(heartbeat * 1000000000)
                await cursor.execute(f"set @master_heartbeat_period= {heartbeat}")
            await self._register_slave()
            if not self._master_auto_position:
                if self._master_log_file is None or self._master_log_position is None:
                    await cursor.execute("SHOW MASTER STATUS")
                    master_status = await cursor.fetchone()
                    if master_status is None:
                        raise BinLogNotEnabledError("MySQL binary logging is not enabled.")
                    self._master_log_file, self._master_log_position = master_status[:2]
                prelude = struct.pack("<i", len(self._master_log_file) + 11) + struct.pack(
                    "!B", COM_BINLOG_DUMP
                )

                if self._resume_stream:
                    prelude += struct.pack("<I", self._master_log_position)
                else:
                    prelude += struct.pack("<I", 4)

                flags = 0
                if not self._blocking:
                    flags |= BINLOG_DUMP_NON_BLOCK
                prelude += struct.pack("<H", flags)

                prelude += struct.pack("<I", self._server_id)
                prelude += self._master_log_file.encode()
            else:
                gtid_set = GtidSet(self._master_auto_position)
                encoded_data_size = gtid_set.encoded_length

                header_size = (
                    2
                    + 4  # binlog_flags
                    + 4  # server_id
                    + 4  # binlog_name_info_size
                    + 8  # empty binlog name
                    + 4  # binlog_pos_info_size
                )  # encoded_data_size

                prelude = (
                    b""
                    + struct.pack("<i", header_size + encoded_data_size)
                    + struct.pack("!B", COM_BINLOG_DUMP_GTID)
                )

                flags = 0
                if not self._blocking:
                    flags |= BINLOG_DUMP_NON_BLOCK
                flags |= BINLOG_THROUGH_GTID

                # binlog_flags (2 bytes)
                # see:
                #  https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
                prelude += struct.pack("<H", flags)

                # server_id (4 bytes)
                prelude += struct.pack("<I", self._server_id)
                # binlog_name_info_size (4 bytes)
                prelude += struct.pack("<I", 3)
                # empty_binlog_name (4 bytes)
                prelude += b"\0\0\0"
                # binlog_pos_info (8 bytes)
                prelude += struct.pack("<Q", 4)

                # encoded_data_size (4 bytes)
                prelude += struct.pack("<I", gtid_set.encoded_length)
                # encoded_data
                prelude += gtid_set.encoded()
        self._connection._write_bytes(prelude)
        self._connection._next_seq_id = 1
        self._connected = True

    async def close(self):
        if self._connected:
            await self._connection.ensure_closed()
            self._connected = False

    async def _read(self):
        if not self._connected:
            await self._connect()
        try:
            pkt = await self._connection.read_packet()
        except OperationalError as e:
            code, _ = e.args
            if code in self.MYSQL_EXPECTED_ERROR_CODES:
                await self.close()
                return
            raise e

        if pkt.is_eof_packet():
            await self.close()
            return

        if not pkt.is_ok_packet():
            return

        binlog_event = BinLogPacket(
            pkt,
            self._table_map,
            self._ctl_connection,
            self._use_checksum,
            self._allowed_events_in_packet,
            self._only_tables,
            self._ignored_tables,
            self._only_schemas,
            self._ignored_schemas,
            self._freeze_schema,
        )
        await binlog_event.init()

        if binlog_event.event_type == ROTATE_EVENT:
            self._master_log_position = binlog_event.event.position
            self._master_log_file = binlog_event.event.next_binlog
            self._table_map = {}
        elif binlog_event.log_pos:
            self._master_log_position = binlog_event.log_pos
        if self._skip_to_timestamp and binlog_event.timestamp < self._skip_to_timestamp:
            return

        if binlog_event.event_type == TABLE_MAP_EVENT and binlog_event.event is not None:
            self._table_map[binlog_event.event.table_id] = binlog_event.event.table

        if binlog_event.event is None or (binlog_event.event.__class__ not in self._allowed_events):
            return

        return binlog_event.event

    async def _checksum_enable(self):
        async with self._connection.cursor() as cursor:
            await cursor.execute("SHOW GLOBAL VARIABLES LIKE 'BINLOG_CHECKSUM'")
            result = await cursor.fetchone()
            if result is None:
                return False
            var, value = result[:2]
            if value == "NONE":
                return False
            return True

    async def _register_slave(self):
        if not self._report_slave:
            return
        packet = self._report_slave.encoded(self._server_id)
        self._connection._write_bytes(packet)
        self._connection._next_seq_id = 1
        await self._connection.read_packet()

    async def _get_table_information(self, schema, table):
        async with self._ctl_connection.cursor(DictCursor) as cursor:
            await cursor.execute(
                """
                    SELECT
                        COLUMN_NAME, COLLATION_NAME, CHARACTER_SET_NAME,
                        COLUMN_COMMENT, COLUMN_TYPE, COLUMN_KEY, ORDINAL_POSITION
                    FROM
                        information_schema.columns
                    WHERE
                        table_schema = %s AND table_name = %s
                    ORDER BY ORDINAL_POSITION
                    """,
                (schema, table),
            )

            return cursor.fetchall()

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._connected:
            await self._connect()
        ret = await self._read()
        while ret is None:
            ret = await self._read()
            continue
        return ret
