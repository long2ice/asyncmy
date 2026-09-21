import asyncio
import re
import ssl as ssl_module

import pytest

from asyncmy.connection import Connection
from asyncmy.connection import _MySQLProtocol  # type: ignore[attr-defined]  # Private protocol helper.
from asyncmy.constants.CR import CR_SERVER_GONE_ERROR, CR_SERVER_LOST
from asyncmy.errors import OperationalError
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
