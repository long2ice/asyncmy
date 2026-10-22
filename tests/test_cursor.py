import asyncio
import datetime
import gc
from collections import OrderedDict
from decimal import Decimal
from enum import Enum

import pytest

import asyncmy
from asyncmy.cursors import Cursor, DictCursor, RE_INSERT_VALUES, SSCursor, SSDictCursor
from conftest import connection_kwargs


@pytest.mark.asyncio
async def test_fetchone(connection):
    async with connection.cursor() as cursor:
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchone()
        assert ret == (1,)


@pytest.mark.asyncio
async def test_fetchall(connection):
    async with connection.cursor() as cursor:
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchall()
        assert ret == ((1,),)


@pytest.mark.asyncio
async def test_dict_cursor(connection):
    async with connection.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("SELECT 1")
        ret = await cursor.fetchall()
        assert ret == [{"1": 1}]


class OrderedSSDictCursor(SSDictCursor):
    dict_type = OrderedDict


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor_class", [SSDictCursor, OrderedSSDictCursor])
async def test_ss_dict_cursor(connection, cursor_class):
    query = "SELECT 1 AS a, 'x' AS b UNION ALL SELECT 2, 'y' UNION ALL SELECT 3, 'z'"
    async with connection.cursor(cursor=cursor_class) as cursor:
        await cursor.execute(query)
        row = await cursor.fetchone()
        assert row == {"a": 1, "b": "x"}
        assert type(row) is cursor_class.dict_type
        rows = await cursor.fetchmany(1)
        assert rows == [{"a": 2, "b": "y"}]
        assert type(rows[0]) is cursor_class.dict_type
        rows = await cursor.fetchall()
        assert rows == [{"a": 3, "b": "z"}]
        assert type(rows[0]) is cursor_class.dict_type
        assert await cursor.fetchone() is None

        await cursor.execute(query)
        rows = [row async for row in cursor]
        assert rows == [
            {"a": 1, "b": "x"},
            {"a": 2, "b": "y"},
            {"a": 3, "b": "z"},
        ]
        assert all(type(row) is cursor_class.dict_type for row in rows)

        await cursor.execute("SELECT 1 AS a WHERE 1=0")
        assert await cursor.fetchone() is None
        assert await cursor.fetchmany(1) == []
        assert await cursor.fetchall() == []


@pytest.mark.asyncio
async def test_insert(connection):
    async with connection.cursor(cursor=DictCursor) as cursor:
        rows = await cursor.execute(
            """INSERT INTO test.asyncmy(id,`decimal`, date, datetime, time, 
            `float`, string, `tinyint`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                1,
                1,
                "2020-08-08",
                "2020-08-08 00:00:00",
                "00:00:00",
                1,
                "1",
                1,
            ),
        )
        assert rows == 1
        await cursor.execute("select * from test.asyncmy where id = %s", (cursor.lastrowid,))
        result = await cursor.fetchall()
        assert result == [
            {
                "id": 1,
                "decimal": Decimal("1.00"),
                "date": datetime.date(2020, 8, 8),
                "datetime": datetime.datetime(2020, 8, 8, 0, 0),
                "time": datetime.timedelta(hours=0, minutes=0, seconds=0),
                "float": 1.0,
                "string": "1",
                "tinyint": 1,
            }
        ]


@pytest.mark.asyncio
async def test_delete(connection):
    async with connection.cursor() as cursor:
        rows = await cursor.execute("delete from test.asyncmy where id = -1")
        assert rows == 0


@pytest.mark.asyncio
async def test_executemany(connection):
    async with connection.cursor(cursor=DictCursor) as cursor:
        rows = await cursor.executemany(
            """INSERT INTO test.asyncmy(`decimal`, date, datetime, time, `float`, 
            string, `tinyint`) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            [
                (
                    1,
                    "2020-08-08",
                    "2020-08-08 00:00:00",
                    "00:00:00",
                    1,
                    "1",
                    1,
                ),
                (
                    1,
                    "2020-08-08",
                    "2020-08-08 00:00:00",
                    "00:00:00",
                    1,
                    "1",
                    1,
                ),
            ],
        )
        assert rows == 2


@pytest.mark.asyncio
async def test_table_ddl(connection):
    async with connection.cursor() as cursor:
        await cursor.execute("drop table if exists test.alter_table")
        create_table_sql = """
            CREATE TABLE test.alter_table
(
    `id` int primary key auto_increment
)
            """
        await cursor.execute(create_table_sql)
        add_column_sql = "alter table test.alter_table add column c varchar(20)"
        await cursor.execute(add_column_sql)
        await cursor.execute("drop table test.alter_table")


class EnumValue(str, Enum):
    VALUE = "1"


@pytest.mark.asyncio
async def test_insert_enum(connection):
    async with connection.cursor(cursor=DictCursor) as cursor:
        rows = await cursor.execute(
            """INSERT INTO test.asyncmy(id, `decimal`, date, datetime, time,
             `float`, string, `tinyint`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                -1,
                1,
                "2020-08-08",
                "2020-08-08 00:00:00",
                "00:00:00",
                1,
                EnumValue.VALUE,
                1,
            ),
        )
        assert rows == 1


def test_executemany_regex():
    query = "INSERT INTO foo (bar, baz) VALUES (%s, %s)"
    match = RE_INSERT_VALUES.match(query)
    assert match is not None
    assert match.group(1) == "INSERT INTO foo (bar, baz) VALUES "
    assert match.group(2) == "(%s, %s)"
    assert match.group(3) == ""

    # Deprecated VALUES syntax
    query = "INSERT INTO foo (bar, baz) VALUES (%s, %s) ON DUPLICATE KEY UPDATE baz=VALUES(baz)"
    match = RE_INSERT_VALUES.match(query)
    assert match is not None
    assert match.group(1) == "INSERT INTO foo (bar, baz) VALUES "
    assert match.group(2) == "(%s, %s)"
    assert match.group(3) == " ON DUPLICATE KEY UPDATE baz=VALUES(baz)"

    # The new syntax
    query = "INSERT INTO foo (bar, baz) VALUES (%s, %s) AS new ON DUPLICATE KEY UPDATE baz=new.baz"
    match = RE_INSERT_VALUES.match(query)
    assert match is not None
    assert match.group(1) == "INSERT INTO foo (bar, baz) VALUES "
    assert match.group(2) == "(%s, %s)"
    assert match.group(3) == " AS new ON DUPLICATE KEY UPDATE baz=new.baz"

    # Test REPLACE VALUES
    query = "REPLACE INTO foo (bar, baz, aa, bb) VALUES (%s, %s, %s, %s)"
    match = RE_INSERT_VALUES.match(query)
    assert match is not None
    assert match.group(1) == "REPLACE INTO foo (bar, baz, aa, bb) VALUES "
    assert match.group(2) == "(%s, %s, %s, %s)"
    assert match.group(3) == ""

    # Test invalid queries
    query = "INSERT INTO foo (bar, baz) VALUES (%s, %s), (%s, %s)"
    match = RE_INSERT_VALUES.match(query)
    assert match is None


@pytest.mark.asyncio
async def test_cursor_is_awaitable(connection):
    """aiomysql's conn.cursor() is a coroutine; `await` must work here too."""
    cursor = await connection.cursor()
    await cursor.execute("SELECT 1")
    assert await cursor.fetchone() == (1,)
    await cursor.close()


@pytest.mark.asyncio
async def test_awaited_cursor_honours_cursor_class(connection):
    cursor = await connection.cursor(DictCursor)
    await cursor.execute("SELECT 1 AS one")
    assert await cursor.fetchone() == {"one": 1}
    await cursor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("cache_size", [0, 8])
async def test_cursor_subclass_keeps_result_and_nextset_hooks(cache_size):
    class HookedCursor(Cursor):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.nextsets = 0
            self.results = 0

        async def nextset(self):
            self.nextsets += 1
            return await super().nextset()

        async def _do_get_result(self):
            await super()._do_get_result()
            self.results += 1
            self._rows = tuple((row[0] * 2,) for row in self._rows)

    conn = await asyncmy.connect(stmt_cache_size=cache_size, **connection_kwargs)
    try:
        async with conn.cursor(HookedCursor) as cursor:
            for value in [3, 7]:
                assert await cursor.execute("SELECT %s AS value", (value,)) == 1
                assert await cursor.fetchone() == (value * 2,)
                assert cursor.description[0][0] == "value"
            assert cursor.results == 2
            assert cursor.nextsets == 2
        assert cursor.nextsets == 3
    finally:
        await conn.ensure_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["execute", "close"])
async def test_buffered_cursor_drains_pending_result_sets(connection, action):
    cursor = connection.cursor()
    await cursor.execute("SELECT 1; SELECT 2; SELECT 3")
    assert await cursor.fetchone() == (1,)
    if action == "execute":
        await cursor.execute("SELECT 4 AS final_value")
        assert await cursor.fetchall() == ((4,),)
        assert cursor.rowcount == cursor.rownumber == 1
        assert cursor.description[0][0] == "final_value"
    await cursor.close()
    async with connection.cursor() as other:
        await other.execute("SELECT 5")
        assert await other.fetchone() == (5,)


@pytest.mark.asyncio
@pytest.mark.parametrize("cache_size", [0, 8])
async def test_buffered_cursor_preserves_warnings_and_resets_result(cache_size, caplog):
    conn = await asyncmy.connect(stmt_cache_size=cache_size, **connection_kwargs)
    try:
        async with conn.cursor() as cursor:
            with caplog.at_level("WARNING", logger="asyncmy"):
                for _ in range(2):
                    await cursor.execute("SELECT CAST(%s AS UNSIGNED) AS value", ("invalid",))
                    assert await cursor.fetchone() == (0,)
            assert any("invalid" in record.message for record in caplog.records)
            await cursor.execute("SELECT %s AS value WHERE 0", (9,))
            assert cursor.rowcount == cursor.rownumber == cursor.lastrowid == 0
            assert cursor.description[0][0] == "value"
            assert await cursor.fetchone() is None
            await cursor.execute("SELECT %s AS value", (11,))
            assert cursor.rownumber == 0
            assert cursor.rowcount == 1
            assert await cursor.fetchone() == (11,)
        with pytest.raises(asyncmy.errors.ProgrammingError, match="Cursor closed"):
            await cursor.execute("SELECT 1")
        await cursor.close()
    finally:
        await conn.ensure_closed()


@pytest.mark.asyncio
async def test_cached_cursor_keeps_previous_rows_and_refreshes_metadata():
    conn = await asyncmy.connect(stmt_cache_size=8, **connection_kwargs)
    try:
        async with conn.cursor() as previous, conn.cursor() as current:
            query = "SELECT %s AS value"
            await previous.execute(query, ("first",))
            for value in ["x", "longer text", "中文", None, "last"]:
                assert await current.execute(query, (value,)) == 1
                assert current.description[0][0] == "value"
                assert await current.fetchone() == (value,)
                assert await current.fetchone() is None
                assert await current.nextset() is None
            assert await previous.fetchall() == (("first",),)
    finally:
        await conn.ensure_closed()


@pytest.mark.asyncio
async def test_cached_cursor_large_response_converts_each_row_once():
    from asyncmy.constants import FIELD_TYPE
    from asyncmy.converters import conversions

    converted = []

    def decode(value):
        converted.append(len(value))
        return value

    conv = conversions.copy()
    for kind in [
        FIELD_TYPE.VARCHAR,
        FIELD_TYPE.VAR_STRING,
        FIELD_TYPE.STRING,
        FIELD_TYPE.BLOB,
        FIELD_TYPE.MEDIUM_BLOB,
        FIELD_TYPE.LONG_BLOB,
    ]:
        conv[kind] = decode
    conn = await asyncmy.connect(stmt_cache_size=8, conv=conv, **connection_kwargs)
    try:
        async with conn.cursor() as cursor:
            for _ in range(2):
                await cursor.execute("SELECT REPEAT(%s, 1048576) AS payload", ("x",))
                assert await cursor.fetchone() == ("x" * 1048576,)
                assert await cursor.fetchone() is None
            assert converted == [1048576, 1048576]
    finally:
        await conn.ensure_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor_type", [SSCursor, SSDictCursor])
async def test_statement_cache_keeps_streaming_cursor_behavior(cursor_type):
    conn = await asyncmy.connect(stmt_cache_size=8, **connection_kwargs)
    try:
        async with conn.cursor(cursor_type) as cursor:
            for _ in range(2):
                await cursor.execute("SELECT %s AS value UNION ALL SELECT %s", (3, 7))
                assert cursor.rowcount == 2**64 - 1
                if cursor_type is SSDictCursor:
                    assert await cursor.fetchone() == {"value": 3}
                    assert await cursor.fetchall() == [{"value": 7}]
                else:
                    assert await cursor.fetchone() == (3,)
                    assert await cursor.fetchall() == [(7,)]
    finally:
        await conn.ensure_closed()


@pytest.mark.asyncio
async def test_cached_cursor_encodes_large_parameters_once():
    encoded = []
    payload = b"x" * 16384

    class Payload(bytes):
        def __bytes__(self):
            encoded.append(True)
            return payload

    conn = await asyncmy.connect(stmt_cache_size=8, **connection_kwargs)
    try:
        async with conn.cursor() as cursor:
            query = "SELECT OCTET_LENGTH(%s)"
            for _ in range(2):
                await cursor.execute(query, (b"x",))
                assert await cursor.fetchone() == (1,)
            await cursor.execute(query, (Payload(),))
            assert await cursor.fetchone() == (len(payload),)
            assert encoded == [True]
    finally:
        await conn.ensure_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["cancel", "timeout"])
async def test_cached_cursor_interrupted_read_closes_connection(failure):
    conn = await asyncmy.connect(
        stmt_cache_size=8,
        read_timeout=0.03 if failure == "timeout" else None,
        **connection_kwargs,
    )
    try:
        async with conn.cursor() as cursor:
            query = "SELECT SLEEP(%s)"
            await cursor.execute(query, (0,))
            if failure == "timeout":
                with pytest.raises(asyncmy.errors.OperationalError, match="read timeout"):
                    await cursor.execute(query, (10,))
            else:
                task = asyncio.create_task(cursor.execute(query, (10,)))
                try:
                    await asyncio.sleep(0.03)
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            assert not conn.connected
            assert conn._stream_broken
    finally:
        conn.close()


def _streaming_query():
    # 4096 rows / 4 MiB per result, without server-specific sequence tables.
    numbers = "(SELECT 0 AS n" + "".join(f" UNION ALL SELECT {n}" for n in range(1, 16)) + ")"
    return (
        "SELECT a.n + 16*b.n + 256*c.n + 1 AS seq, REPEAT('x', 1024) AS payload FROM "
        f"{numbers} a CROSS JOIN {numbers} b CROSS JOIN {numbers} c ORDER BY seq"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch_remaining", ["all", "many"])
async def test_streaming_batches_preserve_position_and_next_result(connection, fetch_remaining):
    payload = "x" * 1024
    async with connection.cursor(SSCursor) as cursor:
        await cursor.execute(f"{_streaming_query()}; SELECT 42")
        assert await cursor.fetchone() == (1, payload)
        assert await cursor.fetchmany(0) == []
        assert await cursor.fetchmany(-1) == []
        assert cursor.rownumber == 1

        assert await cursor.fetchmany(17) == [(i, payload) for i in range(2, 19)]
        assert cursor.rownumber == 18
        cursor.arraysize = 7
        assert await cursor.fetchmany() == [(i, payload) for i in range(19, 26)]
        assert await cursor.fetchone() == (26, payload)
        assert cursor.rownumber == 26

        if fetch_remaining == "all":
            remaining = await cursor.fetchall()
        else:
            # A Python int larger than Py_ssize_t was accepted by range()
            # before batching, and must still drain only this result set.
            remaining = await cursor.fetchmany(2**100)
        assert remaining == [(i, payload) for i in range(27, 4097)]
        assert cursor.rownumber == 4096
        assert await cursor.fetchone() is None
        assert await cursor.fetchmany(512) == []
        assert await cursor.fetchall() == []
        assert cursor.rownumber == 4096

        assert await cursor.nextset()
        assert await cursor.fetchone() == (42,)
        assert await cursor.nextset() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("batch_size", [1, 17, 512])
async def test_streaming_fetchmany_stops_at_requested_size(connection, batch_size):
    payload = "x" * 1024
    async with connection.cursor(SSCursor) as cursor:
        await cursor.execute(_streaming_query())
        expected = 1
        while rows := await cursor.fetchmany(batch_size):
            assert 0 < len(rows) <= batch_size
            assert rows == [(i, payload) for i in range(expected, expected + len(rows))]
            expected += len(rows)
            assert cursor.rownumber == expected - 1
        assert expected == 4097


@pytest.mark.asyncio
async def test_streaming_fetchmany_accepts_index_protocol(connection):
    class Size:
        def __index__(self):
            return 2

    async with connection.cursor(SSCursor) as cursor:
        await cursor.execute("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
        with pytest.raises(TypeError):
            await cursor.fetchmany(1.5)
        assert cursor.rownumber == 0
        assert await cursor.fetchmany(Size()) == [(1,), (2,)]
        assert await cursor.fetchone() == (3,)


class TransformingSSCursor(SSCursor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.read_calls = 0

    def _conv_row(self, row):
        return None if row is None else (row[0] + 100,)

    async def read_next(self):
        self.read_calls += 1
        return await super().read_next()


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch_first_batch", [False, True])
async def test_streaming_batches_preserve_subclass_hooks(connection, fetch_first_batch):
    async with connection.cursor(TransformingSSCursor) as cursor:
        await cursor.execute("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
        rows = await cursor.fetchmany(2) if fetch_first_batch else []
        rows.extend(await cursor.fetchall())
        assert rows == [(101,), (102,), (103,)]
        assert cursor.read_calls == 4  # one per row, plus EOF
        assert cursor.rownumber == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor_class", [SSCursor, SSDictCursor])
@pytest.mark.parametrize("tail", ["SELECT 99", "DO 0; SELECT 99 WHERE 0; SELECT 99"])
async def test_streaming_close_drains_all_results(cursor_class, tail, recwarn):
    conn = await asyncmy.connect(**connection_kwargs)
    try:
        cursor = conn.cursor(cursor_class)
        await cursor.execute(f"{_streaming_query()}; {_streaming_query()}; {tail}")
        assert await cursor.fetchone() is not None
        await cursor.close()
        await cursor.close()

        async with conn.cursor() as next_cursor:
            await next_cursor.execute("SELECT CONNECTION_ID(), 42")
            assert await next_cursor.fetchone() == (conn.thread_id(), 42)
        assert not recwarn.list
    finally:
        conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("fetch_first", [False, True])
async def test_streaming_nextset_discards_unread_rows(fetch_first):
    conn = await asyncmy.connect(**connection_kwargs)
    try:
        async with conn.cursor(SSCursor) as cursor:
            await cursor.execute(f"{_streaming_query()}; SELECT 42")
            if fetch_first:
                assert await cursor.fetchone() == (1, "x" * 1024)
            assert await cursor.nextset()
            assert await cursor.fetchone() == (42,)
            assert await cursor.nextset() is None

        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 43")
            assert await cursor.fetchone() == (43,)
    finally:
        conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("additional_results", [False, True])
async def test_new_command_drains_unfinished_stream(additional_results):
    conn = await asyncmy.connect(**connection_kwargs)
    try:
        stream = conn.cursor(SSCursor)
        query = _streaming_query()
        if additional_results:
            query += f"; {_streaming_query()}; SELECT 99"
        await stream.execute(query)
        assert await stream.fetchone() == (1, "x" * 1024)
        async with conn.cursor() as cursor:
            with pytest.warns(UserWarning, match="Previous unbuffered result was left incomplete"):
                await cursor.execute("SELECT 42")
            assert await cursor.fetchone() == (42,)
            # A stale cursor must not consume another cursor's result.
            await stream.close()
            await cursor.execute("SELECT 43")
            assert await cursor.fetchone() == (43,)
    finally:
        conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("close_connection", [False, True])
async def test_unfinished_stream_finalization(close_connection, recwarn):
    conn = await asyncmy.connect(**connection_kwargs)
    transport = conn._transport
    protocol = conn._proto
    try:
        stream = conn.cursor(SSCursor)
        await stream.execute(_streaming_query())
        assert await stream.fetchone() == (1, "x" * 1024)
        if close_connection:
            conn.close()
        del stream, conn
        gc.collect()
        assert transport.is_closing()
        await asyncio.wait_for(protocol.wait_closed(), timeout=5)
        assert not recwarn.list
    finally:
        transport.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("on_class", [False, True])
async def test_cached_scalar_respects_write_hook(monkeypatch, on_class):
    from asyncmy.connection import Connection

    conn = await asyncmy.connect(stmt_cache_size=8, **connection_kwargs)
    try:
        async with conn.cursor() as cursor:
            for value in range(3):
                await cursor.execute("SELECT %s", (value,))
                assert await cursor.fetchone() == (value,)
            calls = []
            original = Connection._write_bytes

            def write(*args):
                data = args[-1]
                calls.append(data)
                return original(conn, data)

            monkeypatch.setattr(Connection if on_class else conn, "_write_bytes", write)
            for value in [7, -1, 2**64 - 1]:
                await cursor.execute("SELECT %s", (value,))
                assert await cursor.fetchone() == (value,)
            assert len(calls) == 3
    finally:
        await conn.ensure_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["plain", "queued", "invalid_fd", "tls"])
async def test_scalar_transport_order_and_tls(mode):
    import sys

    if sys.platform == "win32" and mode == "plain":
        pytest.skip("POSIX socket fast path")
    try:
        conn = await asyncmy.connect(stmt_cache_size=8, ssl=mode == "tls", **connection_kwargs)
    except asyncmy.errors.OperationalError:
        if mode == "tls":
            pytest.skip("server does not accept TLS connections")
        raise

    class InvalidSocket:
        def fileno(self):
            return 99999999

    class Transport:
        def __init__(self, transport):
            self.transport = transport
            self.writes = []

        def __getattr__(self, name):
            return getattr(self.transport, name)

        def get_extra_info(self, name, default=None):
            if mode == "invalid_fd" and name == "socket":
                return InvalidSocket()
            return self.transport.get_extra_info(name, default)

        def get_write_buffer_size(self):
            return 1 if mode == "queued" else self.transport.get_write_buffer_size()

        def write(self, data):
            self.writes.append(data)
            self.transport.write(data)

    transport = Transport(conn._transport)
    conn._transport = transport
    try:
        async with conn.cursor() as cursor:
            for _ in range(3):
                await cursor.execute("SELECT %s", (1,))
                assert await cursor.fetchone() == (1,)
            transport.writes.clear()
            for value in [-1, 7, 2**64 - 1]:
                await cursor.execute("SELECT %s", (value,))
                assert await cursor.fetchone() == (value,)
            assert len(transport.writes) == (0 if mode == "plain" else 3)
    finally:
        await conn.ensure_closed()


@pytest.mark.asyncio
async def test_cached_cursor_respects_prepared_execute_hook(monkeypatch):
    from asyncmy.connection import PreparedStatement

    conn = await asyncmy.connect(stmt_cache_size=8, **connection_kwargs)
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT %s", (1,))
            calls = []
            original = PreparedStatement.execute

            async def execute(self, args=()):
                calls.append(args)
                return await original(self, args)

            monkeypatch.setattr(PreparedStatement, "execute", execute)
            for value in [3, 7]:
                await cursor.execute("SELECT %s", (value,))
                assert await cursor.fetchone() == (value,)
            assert calls == [(3,), (7,)]
    finally:
        await conn.ensure_closed()
