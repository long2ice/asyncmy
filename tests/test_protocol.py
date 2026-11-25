import struct
import sys

import pytest

from asyncmy.cursors import Cursor, SSCursor
from asyncmy.protocol import MysqlPacket, parse_rows_from_buffer
from asyncmy.protocol import _parse_cached_binary_result  # type: ignore[attr-defined]
from asyncmy.protocol import pack_binary_params


def integer_payload(raw):
    if raw is None:
        return b"\xfb"
    if len(raw) < 251:
        return bytes([len(raw)]) + raw
    return b"\xfc" + len(raw).to_bytes(2, "little") + raw


def read_integer(raw, buffered):
    payload = integer_payload(raw)
    converters = ((0, None, int),)
    if not buffered:
        return MysqlPacket(payload, "utf8").read_row(converters)[0]
    packet = bytearray(len(payload).to_bytes(3, "little") + b"\x00" + payload)
    rows = []
    pos, seq = parse_rows_from_buffer(packet, 0, len(packet), converters, 0, rows)
    assert (pos, seq) == (len(packet), 1)
    return rows[0][0]


@pytest.mark.parametrize("buffered", [False, True])
@pytest.mark.parametrize(
    "raw",
    [
        b"0",
        b"-0",
        b"+0",
        b"255",
        b"256",
        b"-256",
        b"2147483647",
        b"2147483648",
        b"-2147483648",
        b"-2147483649",
        b"9223372036854775807",
        b"9223372036854775808",
        b"-9223372036854775808",
        b"-9223372036854775809",
        b"18446744073709551615",
        b"18446744073709551616",
        b"-18446744073709551615",
        b"-18446744073709551616",
        b"99999999999999999999",
        b"00000000000000000001",
        b"+123",
        b"+000123",
        b" \t-123\r\n",
        b"1_000",
        b"123456789012345678901234567890",
        b"1" * 300,
    ],
)
def test_text_integer_matches_builtin(raw, buffered):
    value = read_integer(raw, buffered)
    assert type(value) is int
    assert value == int(raw)


@pytest.mark.parametrize("buffered", [False, True])
@pytest.mark.parametrize("raw", [b"", b"-", b"+", b"12x", b"1.2", b"\xff", b"1\x002"])
def test_invalid_text_integer_raises_value_error(raw, buffered):
    with pytest.raises(ValueError):
        read_integer(raw, buffered)


@pytest.mark.parametrize("buffered", [False, True])
def test_null_integer_stays_none(buffered):
    assert read_integer(None, buffered) is None


def test_text_integer_preserves_custom_converter_and_adjacent_cells():
    seen = []

    def custom(raw):
        seen.append(raw)
        return raw.decode() + "!"

    payload = b"\x03123\x03123\xfb\x06\xe4\xb8\xad\xe6\x96\x87\x03-42"
    converters = (
        (0, None, int),
        (0, None, custom),
        (0, None, int),
        (1, "utf8", None),
        (0, None, int),
    )
    row = MysqlPacket(payload, "utf8").read_row(converters)
    assert row == (123, "123!", None, "中文", -42)
    assert seen == [b"123"]


def test_buffered_integer_rows_across_sequence_wrap():
    values = [-(2**63), 2**64 - 1, 2**64, 0, 12345678] * 60
    packets = []
    for seq, value in enumerate(values):
        payload = integer_payload(str(value).encode())
        packets.append(len(payload).to_bytes(3, "little") + bytes([seq % 256]) + payload)
    buffer = bytearray(b"".join(packets))
    rows = []
    pos, seq = parse_rows_from_buffer(buffer, 0, len(buffer), ((0, None, int),), 0, rows)
    assert (pos, seq) == (len(buffer), len(values) % 256)
    assert rows == [(value,) for value in values]


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor_cls", [Cursor, SSCursor])
async def test_text_integer_mysql_boundaries(connection, cursor_cls):
    expected = [(-(2**63), 2**64 - 1), (2**63 - 1, 2**63), (0, 0), (-12345678, None)]
    async with connection.cursor() as cursor:
        await cursor.execute(
            "CREATE TEMPORARY TABLE test.integer_fastpath "
            "(signed_value BIGINT, unsigned_value BIGINT UNSIGNED)"
        )
        try:
            await cursor.executemany("INSERT INTO test.integer_fastpath VALUES (%s,%s)", expected)
            async with connection.cursor(cursor_cls) as reader:
                await reader.execute("SELECT * FROM test.integer_fastpath")
                assert list(await reader.fetchall()) == expected
        finally:
            await cursor.execute("DROP TEMPORARY TABLE test.integer_fastpath")


def _wire_packets(payloads, sequence=254):
    return b"".join(
        len(payload).to_bytes(3, "little") + bytes([(sequence + i) % 256]) + payload
        for i, payload in enumerate(payloads)
    )


@pytest.mark.parametrize("deprecate_eof", [False, True])
@pytest.mark.parametrize("count", [0, 1, 300])
def test_cached_binary_result_rows_status_and_sequence_wrap(deprecate_eof, count):
    column = b"cached column definition"
    metadata = [column] if deprecate_eof else [column, b"\xfe\x00\x00\x02\x00"]
    rows = [b"\x00\x00" + struct.pack("<q", i - 123) for i in range(count)]
    end = b"\xfe\x00\x00\x0a\x00\x03\x00" if deprecate_eof else b"\xfe\x03\x00\x0a\x00"
    wire = _wire_packets(metadata + rows + [end])
    prefix = b"consumed prefix"
    buffer = bytearray(prefix + wire + b"next result")
    result = _parse_cached_binary_result(
        buffer,
        len(prefix),
        len(buffer),
        254,
        len(metadata),
        ((8, False, 0, None, int),),
        deprecate_eof,
        (column,),
    )
    assert result == (
        len(prefix) + len(wire),
        (254 + len(metadata) + count + 1) % 256,
        tuple((i - 123,) for i in range(count)),
        10 if deprecate_eof else 0,
        3,
        8,
    )


@pytest.mark.parametrize("deprecate_eof", [False, True])
def test_cached_binary_result_incomplete_data_does_not_run_converters(deprecate_eof):
    seen = []

    def convert(value):
        seen.append(value)
        return value.upper()

    column = b"column"
    metadata = [column] if deprecate_eof else [column, b"\xfe\x00\x00\x02\x00"]
    end = b"\xfe\x00\x00\x02\x00\x00\x00" if deprecate_eof else b"\xfe\x00\x00\x02\x00"
    wire = _wire_packets(metadata + [b"\x00\x00\x03abc", end])
    specs = ((253, False, 0, None, convert),)
    for length in range(len(wire)):
        assert (
            _parse_cached_binary_result(
                bytearray(wire), 0, length, 254, len(metadata), specs, deprecate_eof, (column,)
            )
            is None
        )
        assert not seen
    result = _parse_cached_binary_result(
        bytearray(wire), 0, len(wire), 254, len(metadata), specs, deprecate_eof, (column,)
    )
    assert result[2] == ((b"ABC",),)
    assert seen == [b"abc"]


@pytest.mark.parametrize("failure", ["metadata", "sequence", "error", "unusual_ok"])
def test_cached_binary_result_requires_generic_reader(failure):
    metadata = b"column"
    row = b"\x00\x00" + struct.pack("<q", 123)
    end = b"\xfe\x00\x00\x02\x00\x00\x00"
    if failure == "metadata":
        metadata = b"changed column"
    elif failure == "error":
        row = b"\xff\x28\x04#42000error"
    elif failure == "unusual_ok":
        end = b"\xfe\xfc\x00\x00\x00\x02\x00\x00\x00"
    wire = bytearray(_wire_packets([metadata, row, end]))
    if failure == "sequence":
        wire[len(metadata) + 4 + 3] = 99
    assert (
        _parse_cached_binary_result(
            wire, 0, len(wire), 254, 1, ((8, False, 0, None, int),), True, (b"column",)
        )
        is None
    )


@pytest.mark.parametrize(
    "value",
    [
        None,
        False,
        True,
        -(2**63),
        -1,
        0,
        1,
        2**63 - 1,
        2**63,
        2**64 - 1,
        -0.0,
        1.25,
        float("inf"),
        float("-inf"),
        float("nan"),
    ],
)
def test_single_numeric_parameter_wire_format(value):
    if value is None:
        expected = b"\x01\x01\x06\x00"
    elif type(value) is bool:
        expected = b"\x00\x01\x01\x00" + bytes([value])
    elif type(value) is int:
        unsigned = value >= 2**63
        expected = b"\x00\x01\x08" + bytes([0x80 if unsigned else 0])
        expected += struct.pack("<Q" if unsigned else "<q", value)
    else:
        expected = b"\x00\x01\x05\x00" + struct.pack("<d", value)
    assert pack_binary_params((value,), "utf8") == expected


@pytest.mark.parametrize("value", [-(2**63) - 1, 2**64, 2**100])
def test_single_integer_parameter_rejects_overflow(value):
    with pytest.raises(ValueError, match="out of 64-bit range"):
        pack_binary_params((value,), "utf8")


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX socket fast path")
@pytest.mark.parametrize(
    ("value", "params"),
    [
        (None, b"\x01\x01\x06\x00"),
        (True, b"\x00\x01\x01\x00\x01"),
        (-1, b"\x00\x01\x08\x00" + struct.pack("<q", -1)),
        (2**64 - 1, b"\x00\x01\x08\x80" + struct.pack("<Q", 2**64 - 1)),
        (1.25, b"\x00\x01\x05\x00" + struct.pack("<d", 1.25)),
    ],
)
def test_scalar_execute_socket_wire(value, params):
    import socket

    from asyncmy.protocol import _send_scalar_execute

    sender, receiver = socket.socketpair()
    try:
        sender.setblocking(False)
        header = struct.pack("<IBI", 42, 0, 1)
        payload = b"\x17" + header + params
        expected = len(payload).to_bytes(3, "little") + b"\x00" + payload
        assert _send_scalar_execute(header, (value,), sender.fileno()) is None
        assert receiver.recv(100) == expected
    finally:
        sender.close()
        receiver.close()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX socket fast path")
def test_scalar_execute_backpressure_keeps_unsent_packet():
    import socket

    from asyncmy.protocol import _send_scalar_execute

    sender, receiver = socket.socketpair()
    try:
        sender.setblocking(False)
        receiver.setblocking(False)
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        queued = 0
        while True:
            try:
                queued += sender.send(b"x" * 4096)
            except BlockingIOError:
                break
        header = struct.pack("<IBI", 42, 0, 1)
        params = b"\x00\x01\x08\x00" + struct.pack("<q", 7)
        payload = b"\x17" + header + params
        expected = len(payload).to_bytes(3, "little") + b"\x00" + payload
        assert _send_scalar_execute(header, (7,), sender.fileno()) == expected
        received = bytearray()
        while True:
            try:
                received.extend(receiver.recv(8192))
            except BlockingIOError:
                break
        assert received == b"x" * queued
        assert _send_scalar_execute(header, ("text",), sender.fileno()) is False
    finally:
        sender.close()
        receiver.close()


def test_text_row_inline_c_parsers_and_fallbacks():
    import datetime
    from decimal import Decimal

    from asyncmy.converters import convert_date, convert_datetime, convert_time, convert_timedelta

    def cell(b):
        if b is None:
            return b"\xfb"
        return bytes([len(b)]) + b

    payload = b"".join(
        [
            cell(b"42"),
            cell(b"-3.125"),
            cell(b"123.4500"),
            cell(b"2024-02-29 23:59:58.12"),
            cell(b"2024-02-29"),
            cell(b"-25:06:17.5"),
            cell(b"15:06:17.000042"),
            cell("你好".encode()),
            cell(None),
        ]
    )
    converters = (
        (0, None, int),
        (0, None, float),
        (2, "ascii", Decimal),
        (0, None, convert_datetime),
        (0, None, convert_date),
        (0, None, convert_timedelta),
        (0, None, convert_time),
        (1, "utf8", None),
        (0, None, float),
    )
    packet = bytearray(len(payload).to_bytes(3, "little") + b"\x00" + payload)
    rows = []
    pos, seq = parse_rows_from_buffer(packet, 0, len(packet), converters, 0, rows, True)
    assert (pos, seq) == (len(packet), 1)
    assert rows == [
        (
            42,
            -3.125,
            Decimal("123.4500"),
            datetime.datetime(2024, 2, 29, 23, 59, 58, 120000),
            datetime.date(2024, 2, 29),
            datetime.timedelta(days=-2, seconds=82422, microseconds=500000),
            datetime.time(15, 6, 17, 42),
            "你好",
            None,
        )
    ]

    # Non-canonical / invalid values must fall back cleanly to Python semantics.
    edge_payload = b"".join(
        [
            cell(b" 1.5 "),
            cell(b"1e500"),
            cell(b"0000-00-00 00:00:00"),
            cell(b"2023-02-29"),
            cell(b"invalid"),
            cell(b"-01:00:00"),
        ]
    )
    edge_convs = (
        (0, None, float),
        (0, None, float),
        (0, None, convert_datetime),
        (0, None, convert_date),
        (0, None, convert_timedelta),
        (0, None, convert_time),
    )
    row = MysqlPacket(edge_payload, "utf8").read_row(edge_convs)
    assert row == (1.5, float("inf"), "0000-00-00 00:00:00", "2023-02-29", "invalid", "-01:00:00")

    with pytest.raises(ValueError):
        MysqlPacket(cell(b"not-a-float"), "utf8").read_row(((0, None, float),))


def test_wide_row_exceeding_stack_column_buffer():
    # Exercises the heap malloc/free path when column count > 64.
    ncols = 70
    payload = b"".join(bytes([len(str(i))]) + str(i).encode() for i in range(ncols))
    converters = tuple((0, None, int) for _ in range(ncols))
    packet = bytearray(len(payload).to_bytes(3, "little") + b"\x00" + payload)
    rows = []
    pos, seq = parse_rows_from_buffer(packet, 0, len(packet), converters, 0, rows, True)
    assert (pos, seq) == (len(packet), 1)
    assert rows == [tuple(range(ncols))]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX socket fast path")
def test_send_query_packet_wire_and_fallback():
    import socket

    from asyncmy.protocol import _send_query_packet

    sender, receiver = socket.socketpair()
    try:
        sender.setblocking(False)
        for sql, expected_bytes in [
            ("SELECT 1", b"SELECT 1"),
            ("SELECT '中文'", "SELECT '中文'".encode()),
            (b"SELECT 2", b"SELECT 2"),
        ]:
            assert _send_query_packet(sql, "utf8", sender.fileno()) is None
            expected = (
                (len(expected_bytes) + 1).to_bytes(3, "little") + b"\x00\x03" + expected_bytes
            )
            assert receiver.recv(128) == expected
        # Oversized queries and non-ASCII on latin1 fall back cleanly.
        assert _send_query_packet("x" * 5000, "utf8", sender.fileno()) is False
        assert _send_query_packet("中文", "latin1", sender.fileno()) is False
    finally:
        sender.close()
        receiver.close()


def test_format_executemany_batches_matches_legacy_and_splits():
    import datetime
    from decimal import Decimal

    from asyncmy.errors import ProgrammingError
    from asyncmy.protocol import _format_executemany_batches

    rows = [
        (
            1,
            2**64 - 1,
            None,
            True,
            False,
            1.25,
            "a'b\"c\\d\0\n\r\x1a中",
            b"raw\x00'\\x",
            bytearray(b"ba'"),
            datetime.datetime(2024, 1, 2, 3, 4, 5, 67),
            datetime.datetime(2024, 1, 2, 3, 4, 5),
            datetime.date(2024, 1, 2),
            datetime.timedelta(days=-2, seconds=3, microseconds=456789),
            datetime.timedelta.min,
            datetime.time(6, 7, 8, 9),
            Decimal("12.340"),
        ),
        (
            2,
            -(2**63),
            None,
            False,
            True,
            0.0,
            "plain",
            b"ok",
            bytearray(b"ok"),
            datetime.datetime(2024, 1, 2, 3, 4, 5),
            datetime.datetime(2024, 1, 2, 3, 4, 5),
            datetime.date(2024, 1, 2),
            datetime.timedelta(0),
            datetime.timedelta.max,
            datetime.time(6, 7, 8),
            Decimal("0.00"),
        ),
    ]
    tpl = "(" + ", ".join(["%s"] * 16) + ")"
    batches = _format_executemany_batches(
        b"INSERT INTO t VALUES ", tpl, b" ON DUPLICATE KEY UPDATE x=1", rows, 1024000, "utf8", False
    )
    assert len(batches) == 1
    sql = batches[0].decode("utf8")
    assert "'a\\'b\\\"c\\\\d\\0\\n\\r\\Z中'" in sql
    assert "_binary'raw\\0\\'\\\\x'" in sql
    assert "_binary'ba\\''" in sql
    assert "'2024-01-02 03:04:05.000067'" in sql
    assert "'2024-01-02 03:04:05'" in sql
    assert "'-47:59:56.543211'" in sql
    assert "'-23999999976:00:00'" in sql
    assert "'23999999999:59:59.999999'" in sql
    assert "1.25e0" in sql
    assert "12.340" in sql
    assert sql.endswith(" ON DUPLICATE KEY UPDATE x=1")

    # Small max_stmt_length forces splitting into two batches with postfix on each.
    split = _format_executemany_batches(
        b"INSERT INTO t VALUES ", tpl, b" ON DUPLICATE KEY UPDATE x=1", rows, 300, "utf8", False
    )
    assert len(split) == 2
    assert all(b.startswith(b"INSERT INTO t VALUES ") for b in split)
    assert all(b.endswith(b" ON DUPLICATE KEY UPDATE x=1") for b in split)

    # NO_BACKSLASH_ESCAPES doubles single quotes for str.
    nbe = _format_executemany_batches(
        b"INSERT INTO t VALUES ", "(%s)", b"", [("a'\\b",)], 1024, "utf8", True
    )
    assert nbe == [b"INSERT INTO t VALUES ('a''\\b')"]

    # Unsupported templates / encodings / inf / nan fall back or raise properly.
    assert (
        _format_executemany_batches(b"I ", "(%(a)s)", b"", [{"a": 1}], 1024, "utf8", False) is None
    )
    assert _format_executemany_batches(b"I ", "(%s)", b"", [(1,)], 1024, "latin1", False) is None
    with pytest.raises(ProgrammingError):
        _format_executemany_batches(b"I ", "(%s)", b"", [(float("inf"),)], 1024, "utf8", False)
    with pytest.raises(ProgrammingError):
        _format_executemany_batches(b"I ", "(%s)", b"", [(float("nan"),)], 1024, "utf8", False)
