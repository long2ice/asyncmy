import asyncio
import re
import ssl as ssl_module

import pytest

from asyncmy.connection import Connection
from asyncmy.connection import _MySQLProtocol  # type: ignore[attr-defined]  # Private protocol helper.
from asyncmy.constants.CR import CR_SERVER_GONE_ERROR, CR_SERVER_LOST
from asyncmy.errors import InterfaceError, InternalError, OperationalError
from conftest import connection_kwargs


@pytest.mark.asyncio
async def test_connect():
    connection = Connection(**connection_kwargs)
    await connection.connect()
    assert connection._connected
    assert re.match(
        r"\d+\.\d+\.\d+([^0-9].*)?",
        connection.get_server_info(),
    )
    assert connection.get_proto_info() == 10
    assert connection.get_host_info() != "Not Connected"
    await connection.ensure_closed()


def _feed_protocol(proto, data):
    while data:
        view = proto.get_buffer(len(data))
        count = min(len(view), len(data))
        view[:count] = data[:count]
        del view
        proto.buffer_updated(count)
        data = data[count:]


@pytest.mark.asyncio
async def test_connection_lost_drops_exception_traceback():
    proto = _MySQLProtocol(asyncio.get_running_loop())

    def fail(held):
        try:
            raise OSError("cause")
        except OSError:
            raise ssl_module.SSLError("lost")

    try:
        fail(proto)
    except ssl_module.SSLError as e:
        exc = e
    assert exc.__traceback__ is not None and exc.__context__ is not None

    proto.connection_lost(exc)

    # The traceback frames (which hold the transport and, through it, the
    # protocol) must not be kept, or every dropped connection waits for gc.
    assert proto.exc is exc
    assert proto.exc.__traceback__ is None
    assert proto.exc.__context__ is None
    assert proto.exc.__cause__ is None
    assert "lost" in str(proto.exc)


@pytest.mark.asyncio
@pytest.mark.parametrize("split", range(1, 12))
async def test_packet_read_across_header_and_payload_fragments(split):
    conn = Connection()
    proto = _MySQLProtocol(asyncio.get_running_loop())
    assert isinstance(proto, asyncio.BufferedProtocol)
    conn._proto = proto
    conn._next_seq_id = 0
    payload = b"payload!"
    wire = len(payload).to_bytes(3, "little") + b"\x00" + payload
    _feed_protocol(proto, wire[:split])
    reader = asyncio.create_task(conn.read_packet())
    try:
        await asyncio.sleep(0)
        assert not reader.done()
        _feed_protocol(proto, wire[split:])
        packet = await asyncio.wait_for(reader, timeout=5)
        assert packet.get_all_data() == payload
        assert conn._next_seq_id == 1
        assert proto.pos == proto.length
    finally:
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


@pytest.mark.asyncio
async def test_packet_read_grows_and_compacts_receive_buffer():
    conn = Connection()
    proto = _MySQLProtocol(asyncio.get_running_loop())
    conn._proto = proto
    conn._next_seq_id = 0
    payload = b"x" * (1024 * 1024 + 17)
    wire = len(payload).to_bytes(3, "little") + b"\x00" + payload
    # Leave part of the body buffered while the reader awaits the rest.
    _feed_protocol(proto, wire[:123])
    reader = asyncio.create_task(conn.read_packet())
    try:
        await asyncio.sleep(0)
        _feed_protocol(proto, wire[123:])
        assert len(proto.buffer) > 256 * 1024
        packet = await asyncio.wait_for(reader, timeout=5)
        assert packet.get_all_data() == payload
        _feed_protocol(proto, b"\x02\x00\x00\x01ok")
        assert (await conn.read_packet()).get_all_data() == b"ok"
        assert proto.pos == proto.length
    finally:
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


def _feed_protocol_like_transport(proto, data):
    # Mirrors asyncio.protocols._feed_data_to_buffered_proto (used by the
    # Windows ProactorEventLoop): the view returned by the previous call is
    # still referenced when the next get_buffer() is requested.
    while data:
        view = proto.get_buffer(len(data))
        count = min(len(view), len(data))
        view[:count] = data[:count]
        proto.buffer_updated(count)
        data = data[count:]


@pytest.mark.asyncio
async def test_packet_read_while_transport_holds_previous_view():
    conn = Connection()
    proto = _MySQLProtocol(asyncio.get_running_loop())
    conn._proto = proto
    conn._next_seq_id = 0
    # A full ProactorEventLoop read arrives while part of the packet is already
    # buffered: the next get_buffer() must grow without resizing the live view.
    payload = b"x" * (256 * 1024 + 100)
    wire = len(payload).to_bytes(3, "little") + b"\x00" + payload
    _feed_protocol_like_transport(proto, wire[:100])
    reader = asyncio.create_task(conn.read_packet())
    try:
        await asyncio.sleep(0)
        _feed_protocol_like_transport(proto, wire[100:])
        packet = await asyncio.wait_for(reader, timeout=5)
        assert packet.get_all_data() == payload
        assert proto.pos == proto.length
    finally:
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("consumed_prefix", [False, True])
async def test_packet_read_from_multiple_tls_records(consumed_prefix):
    conn = Connection()
    proto = _MySQLProtocol(asyncio.get_running_loop())
    conn._proto = proto
    conn._next_seq_id = 0
    if consumed_prefix:
        _feed_protocol(proto, b"\x02\x00\x00\x00ok")
        assert (await conn.read_packet()).get_all_data() == b"ok"

    payloads = [b"\x01\x03", b"\x00\x00\x00\x02\x00\x00\x00"]
    records = [
        len(payload).to_bytes(3, "little") + bytes([conn._next_seq_id + i]) + payload
        for i, payload in enumerate(payloads)
    ]
    buffer = proto.get_buffer(sum(map(len, records)))
    offset = 0
    for record in records:
        # asyncio's TLS transport fills one application buffer with multiple
        # SSL reads. Later reads receive a slice of the original buffer.
        target = buffer if offset == 0 else buffer[offset:]
        target[: len(record)] = record
        offset += len(record)
        del target
    del buffer
    proto.buffer_updated(offset)

    for payload in payloads:
        assert (await conn.read_packet()).get_all_data() == payload
    assert proto.pos == proto.length


@pytest.mark.asyncio
@pytest.mark.parametrize("sequence", [0, 2])
async def test_packet_reader_checks_sequence_number(sequence):
    conn = Connection()
    conn._proto = _MySQLProtocol(asyncio.get_running_loop())
    conn._next_seq_id = 1
    _feed_protocol(conn._proto, b"\x01\x00\x00" + bytes([sequence]) + b"x")
    error = OperationalError if sequence == 0 else InternalError
    with pytest.raises(error):
        await conn.read_packet()


@pytest.mark.asyncio
async def test_packet_read_without_protocol_is_rejected():
    conn = Connection()
    with pytest.raises(InterfaceError, match="Not connected"):
        await conn.read_packet()
    with pytest.raises(InterfaceError, match="Not connected"):
        await conn._read_bytes(1)


@pytest.mark.asyncio
@pytest.mark.parametrize("close_transport", [False, True])
async def test_closed_stream_is_detected_before_connection_lost_callback(close_transport):
    conn = Connection(**connection_kwargs)
    await conn.connect()
    try:
        if close_transport:
            conn._transport.close()
        else:
            conn.close()
            assert not conn.connected
        # No event-loop turn has delivered connection_lost() yet. A pool
        # checkout must still reject the stream immediately.
        assert conn._stream_broken
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_read_timeout():
    with pytest.raises(OperationalError):
        connection = Connection(read_timeout=1, **connection_kwargs)
        await connection.connect()
        async with connection.cursor() as cursor:
            await cursor.execute("DO SLEEP(3)")


@pytest.mark.asyncio
@pytest.mark.parametrize("event", ["data", "eof", "error"])
async def test_read_timeout_does_not_lose_early_notification(mocker, event):
    loop = asyncio.get_running_loop()
    conn = Connection(read_timeout=0.1)
    proto = _MySQLProtocol(loop)
    conn._proto = proto
    wait_for = asyncio.wait_for

    async def scheduled_wait_for(awaitable, timeout):
        # Python 3.9/3.10 schedule the coroutine as a separate task. Keep
        # that scheduling gap deterministic on newer Python versions too.
        return await wait_for(asyncio.ensure_future(awaitable), timeout)

    mocker.patch("asyncio.wait_for", side_effect=scheduled_wait_for)
    if event == "data":
        loop.call_soon(proto.buffer_updated, 4)
        await conn._fill_buffer(4)
        assert proto.length - proto.pos == 4
    else:
        if event == "eof":
            loop.call_soon(proto.eof_received)
        else:
            loop.call_soon(proto.connection_lost, OSError("peer disconnected"))
        with pytest.raises(OperationalError) as caught:
            await conn._fill_buffer(4)
        assert caught.value.args[0] == CR_SERVER_LOST
        assert "read timeout" not in str(caught.value)


@pytest.mark.asyncio
async def test_read_timeout_waits_for_remaining_bytes():
    loop = asyncio.get_running_loop()
    conn = Connection(read_timeout=1)
    proto = _MySQLProtocol(loop)
    conn._proto = proto
    proto.buffer_updated(2)
    read = asyncio.create_task(conn._fill_buffer(4))
    try:
        await asyncio.sleep(0)
        assert not read.done()
        proto.buffer_updated(2)
        await asyncio.wait_for(read, timeout=5)
    finally:
        read.cancel()
        await asyncio.gather(read, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["oserror", "closed_runtime", "unrelated_runtime"])
async def test_ping_write_errors(mocker, failure):
    conn = Connection()
    transport = mocker.Mock(spec=asyncio.Transport)
    transport.is_closing.return_value = False
    conn._transport = transport
    conn._connected = True
    error = BrokenPipeError("broken pipe") if failure == "oserror" else RuntimeError("write failed")

    def fail_write(data):
        if failure == "closed_runtime":
            transport.is_closing.return_value = True
        raise error

    transport.write.side_effect = fail_write
    try:
        if failure == "unrelated_runtime":
            with pytest.raises(RuntimeError) as caught:
                await conn.ping(reconnect=False)
            assert caught.value is error
            transport.close.assert_not_called()
        else:
            with pytest.raises(OperationalError) as caught:
                await conn.ping(reconnect=False)
            assert caught.value.args[0] == CR_SERVER_GONE_ERROR
            assert caught.value.__cause__ is error
            assert conn._transport is None
            assert not conn.connected
            transport.close.assert_called_once()
    finally:
        conn.close()


@pytest.mark.parametrize("reconnect", [False, True])
def test_ping_closed_uvloop_transport(reconnect):
    uvloop = pytest.importorskip("uvloop")

    async def check():
        conn = Connection(**connection_kwargs)
        await conn.connect()
        try:
            transport = conn._transport
            transport.close()
            await conn._proto.wait_closed()
            assert transport.is_closing()
            if reconnect:
                await conn.ping(reconnect=True)
                assert conn.connected
                assert conn._transport is not transport
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT 1")
                    assert await cursor.fetchone() == (1,)
            else:
                with pytest.raises(OperationalError) as caught:
                    await conn.ping(reconnect=False)
                assert caught.value.args[0] == CR_SERVER_GONE_ERROR
                assert not conn.connected
                assert conn._transport is None
        finally:
            await conn.ensure_closed()

    loop = uvloop.new_event_loop()
    try:
        loop.run_until_complete(check())
    finally:
        loop.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["closing", "oserror", "closed_runtime", "unrelated_runtime"])
async def test_ensure_closed_write_errors(mocker, failure):
    conn = Connection()
    transport = mocker.Mock(spec=asyncio.Transport)
    transport.is_closing.return_value = failure == "closing"
    conn._transport = transport
    conn._connected = True
    error = BrokenPipeError("broken pipe") if failure == "oserror" else RuntimeError("write failed")

    def fail_write(data):
        if failure == "closed_runtime":
            transport.is_closing.return_value = True
        raise error

    transport.write.side_effect = fail_write
    try:
        if failure == "unrelated_runtime":
            with pytest.raises(RuntimeError) as caught:
                await conn.ensure_closed()
            assert caught.value is error
            transport.close.assert_not_called()
        else:
            await conn.ensure_closed()
            if failure == "closing":
                transport.write.assert_not_called()
            transport.close.assert_called_once()
            assert conn._transport is None
            assert not conn.connected
    finally:
        conn.close()


@pytest.mark.parametrize("use_uvloop", [False, True])
def test_ensure_closed_after_transport_closed(use_uvloop):
    if use_uvloop:
        uvloop = pytest.importorskip("uvloop")
        loop = uvloop.new_event_loop()
    else:
        loop = asyncio.new_event_loop()

    async def check():
        conn = Connection(**connection_kwargs)
        await conn.connect()
        try:
            transport = conn._transport
            transport.close()
            await conn._proto.wait_closed()
            assert transport.is_closing()
            await conn.ensure_closed()
            assert not conn.connected
            assert conn._transport is None
        finally:
            await conn.ensure_closed()

    try:
        loop.run_until_complete(check())
    finally:
        loop.close()


@pytest.mark.asyncio
async def test_ssl_true_builds_a_context():
    """`ssl=True` must actually enable TLS, not silently fall back to plaintext."""
    connection = Connection(ssl=True)
    assert isinstance(connection._ssl_context, ssl_module.SSLContext)


@pytest.mark.asyncio
async def test_ssl_context_is_passed_through():
    context = ssl_module.create_default_context()
    assert Connection(ssl=context)._ssl_context is context


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, False])
async def test_ssl_disabled(value):
    assert Connection(ssl=value)._ssl_context is None


@pytest.mark.asyncio
async def test_ssl_rejects_unusable_value():
    """A CA path passed as a bare string used to disable TLS silently."""
    with pytest.raises(ValueError):
        Connection(ssl="/path/to/ca.pem")


@pytest.mark.asyncio
async def test_ping_reconnect_refreshes_password(mocker):
    class ReconnectAttempt(Exception):
        pass

    password_creator = mocker.Mock(return_value="fresh-password")
    connection = Connection(password_creator=password_creator)
    connection._connected = True
    mocker.patch.object(
        connection,
        "_execute_command",
        side_effect=OperationalError("connection lost"),
    )
    mocker.patch.object(
        connection._loop,
        "create_connection",
        side_effect=ReconnectAttempt,
    )

    with pytest.raises(ReconnectAttempt):
        await connection.ping(reconnect=True)

    password_creator.assert_called_once_with()


@pytest.mark.asyncio
async def test_transaction(connection):
    await connection.begin()
    await connection.query(
        """INSERT INTO test.asyncmy(`decimal`, date, datetime, `float`,
         string, `tinyint`) VALUES (%s,'%s','%s',%s,'%s',%s)"""
        % (
            1,
            "2020-08-08",
            "2020-08-08 00:00:00",
            1,
            "1",
            1,
        ),
        True,
    )
    await connection.rollback()


@pytest.mark.asyncio
async def test_tls_connection_is_marked_secure():
    """caching_sha2_password full auth sends the password in the clear over a
    secure channel; if _secure stays False it takes the RSA branch and the
    server rejects it with 1045."""
    kwargs = {k: v for k, v in connection_kwargs.items() if k != "ssl"}
    connection = Connection(ssl=True, **kwargs)
    try:
        await connection.connect()
    except OperationalError:
        pytest.skip("server does not accept TLS connections")
    assert connection._secure
    await connection.ensure_closed()


@pytest.mark.asyncio
async def test_full_auth_over_tls():
    """Regression test: clearing the server's auth cache forces full auth."""
    admin = Connection(**connection_kwargs)
    await admin.connect()
    try:
        await admin.query("FLUSH PRIVILEGES")
    except Exception:
        await admin.ensure_closed()
        pytest.skip("cannot flush the server auth cache")
    await admin.ensure_closed()

    kwargs = {k: v for k, v in connection_kwargs.items() if k != "ssl"}
    connection = Connection(ssl=True, **kwargs)
    try:
        await connection.connect()
    except OperationalError as e:
        if e.args[0] == 1045:
            raise
        pytest.skip("server does not accept TLS connections")
    await connection.ensure_closed()


def _wire_split_packet(payload: bytes, seq: int) -> tuple[bytes, int]:
    max_len = 0xFFFFFF
    chunks = []
    offset = 0
    while True:
        chunk = payload[offset : offset + max_len]
        chunks.append(len(chunk).to_bytes(3, "little") + bytes([seq & 0xFF]) + chunk)
        seq = (seq + 1) & 0xFF
        offset += len(chunk)
        if len(chunk) < max_len:
            break
    return b"".join(chunks), seq


def _column_def_packet(name: bytes, field_type: int, charsetnr: int) -> bytes:
    def lenenc(value: bytes) -> bytes:
        return bytes([len(value)]) + value

    return (
        lenenc(b"def")
        + lenenc(b"")
        + lenenc(b"")
        + lenenc(b"")
        + lenenc(name)
        + lenenc(name)
        + b"\x0c"
        + charsetnr.to_bytes(2, "little")
        + (0xFFFFFFFF).to_bytes(4, "little")
        + bytes([field_type])
        + (0).to_bytes(2, "little")
        + b"\x00\x00\x00"
    )


def test_check_packet_is_eof_deprecate_eof_boundaries():
    from asyncmy.connection import MySQLResult
    from asyncmy.protocol import MysqlPacket

    conn = Connection()
    conn._deprecate_eof = True

    result = MySQLResult(conn)
    assert result._check_packet_is_eof(MysqlPacket(b"\xfe\x00\x00\x0a\x00\x03\x00", "utf8"))
    assert result.server_status == 0x000A
    assert result.warning_count == 3
    assert result.has_next == 8

    # OK packets carrying info/session state may exceed 9 bytes.
    result = MySQLResult(conn)
    assert result._check_packet_is_eof(
        MysqlPacket(b"\xfe\x00\x00\x02\x00\x01\x00session-info", "utf8")
    )
    assert result.server_status == 0x0002
    assert result.warning_count == 1
    assert result.has_next == 0

    # Too short to be a PROTOCOL_41 OK packet, or >= MAX_PACKET_LEN (large row).
    for payload in [
        b"",
        b"\xfe",
        b"\xfe\x00\x00\x02\x00",
        b"\xfe" + b"\x00" * (0xFFFFFF - 1),
        b"\xfe" + (2**24).to_bytes(8, "little") + b"x" * (2**24),
    ]:
        result = MySQLResult(conn)
        assert not result._check_packet_is_eof(MysqlPacket(payload, "utf8"))


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["buffered", "unbuffered", "unbuffered_drain"])
async def test_deprecate_eof_large_first_column_row_wire(mode):
    from asyncmy.connection import MySQLResult
    from asyncmy.constants.FIELD_TYPE import LONG, LONG_BLOB

    size = 2**24
    conn = Connection()
    proto = _MySQLProtocol(asyncio.get_running_loop())
    conn._proto = proto
    conn._next_seq_id = 1
    conn._deprecate_eof = True

    row1 = b"\xfe" + size.to_bytes(8, "little") + b"x" * size + b"\x011"
    row2 = b"\x05small\x012"
    eof_ok = b"\xfe\x00\x00\x02\x00\x00\x00extra-info"
    payloads = [
        b"\x02",  # 2 columns
        _column_def_packet(b"v", LONG_BLOB, 45),
        _column_def_packet(b"id", LONG, 63),
        row1,
        row2,
        eof_ok,
    ]
    wire = bytearray()
    seq = 1
    for payload in payloads:
        part, seq = _wire_split_packet(payload, seq)
        wire.extend(part)
    next_stmt_packet = b"\x02\x00\x00\x01ok"
    _feed_protocol(proto, bytes(wire) + next_stmt_packet)

    result = MySQLResult(conn)
    if mode == "buffered":
        await result.read()
        assert len(result.rows) == 2
        assert len(result.rows[0][0]) == size
        assert result.rows[0][0][:4] == "xxxx"
        assert result.rows[0][0][-4:] == "xxxx"
        assert result.rows[0][1] == 1
        assert result.rows[1] == ("small", 2)
    elif mode == "unbuffered":
        await result.init_unbuffered_query()
        first = await result._read_rowdata_packet_unbuffered()
        second = await result._read_rowdata_packet_unbuffered()
        end = await result._read_rowdata_packet_unbuffered()
        assert len(first[0]) == size
        assert first[1] == 1
        assert second == ("small", 2)
        assert end is None
        assert not result.unbuffered_active
    else:
        await result.init_unbuffered_query()
        await result._finish_unbuffered_query()
        assert not result.unbuffered_active

    # The terminator was consumed cleanly and the next command stays in sync.
    assert conn._next_seq_id == seq
    conn._next_seq_id = 1
    assert (await conn.read_packet()).get_all_data() == b"ok"
    assert proto.pos == proto.length


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor_name", ["Cursor", "SSCursor"])
async def test_large_first_column_with_deprecate_eof(cursor_name):
    from asyncmy.cursors import Cursor, SSCursor

    cursor_cls = Cursor if cursor_name == "Cursor" else SSCursor
    size = 2**24  # first column >= 16 MiB starts with 0xFE length prefix

    conn = Connection(**connection_kwargs)
    await conn.connect()
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT @@max_allowed_packet")
            max_allowed_packet = (await cursor.fetchone())[0]
        if max_allowed_packet <= size + 1024:
            pytest.skip(f"max_allowed_packet ({max_allowed_packet}) too small for >=16 MiB row")

        sql = f"SELECT REPEAT('x', {size}) AS v, 1 AS id UNION ALL SELECT 'small' AS v, 2 AS id"
        async with conn.cursor(cursor_cls) as cursor:
            await cursor.execute(sql)
            rows = await cursor.fetchall()
            assert len(rows) == 2
            assert len(rows[0][0]) == size
            assert rows[0][1] == 1
            assert rows[1] == ("small", 2)

            await cursor.execute("SELECT 1")
            assert list(await cursor.fetchall()) == [(1,)]

        if cursor_cls is SSCursor:
            # Closing an unbuffered cursor early must drain past >=16 MiB rows.
            async with conn.cursor(SSCursor) as cursor:
                await cursor.execute(sql)
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
                assert await cursor.fetchone() == (1,)
    finally:
        await conn.ensure_closed()
