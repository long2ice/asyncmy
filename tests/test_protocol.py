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
