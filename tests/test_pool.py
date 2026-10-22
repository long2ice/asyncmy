import asyncio

import pytest
from conftest import connection_kwargs

import asyncmy
from asyncmy.connection import Connection


@pytest.mark.asyncio
async def test_pool(pool):
    assert pool.minsize == 1
    assert pool.maxsize == 10
    assert pool.size == 1
    assert pool.freesize == 1


@pytest.mark.asyncio
async def test_pool_cursor(pool):
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            ret = await cursor.fetchone()
            assert ret == (1,)


@pytest.mark.asyncio
async def test_acquire(pool):
    conn = await pool.acquire()
    assert isinstance(conn, Connection)
    assert pool.freesize == 0
    assert pool.size == 1
    assert conn.connected
    await pool.release(conn)
    assert pool.freesize == 1
    assert pool.size == 1


@pytest.mark.asyncio
async def test_release_without_waiters_completes_immediately(mocker):
    pool = await asyncmy.create_pool(minsize=1, maxsize=1, **connection_kwargs)
    try:
        conn = await pool.acquire()
        wakeup = mocker.spy(pool, "_wakeup")
        released = pool.release(conn)
        assert released.done()
        await released
        wakeup.assert_not_called()
        assert pool.freesize == 1
    finally:
        pool.close()
        await pool.wait_closed()


@pytest.mark.asyncio
async def test_acquire_free_connection_respects_condition_lock():
    pool = await asyncmy.create_pool(minsize=1, maxsize=1, **connection_kwargs)
    waiter = None
    try:
        conn = pool._free[0]
        async with pool.cond:
            waiter = asyncio.ensure_future(pool.acquire())
            await asyncio.sleep(0)
            assert not waiter.done()
            assert pool.freesize == 1
        acquired = await asyncio.wait_for(waiter, timeout=5)
        assert acquired is conn
        assert pool.freesize == 0
        await pool.release(acquired)
    finally:
        if waiter is not None:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_after_release", [False, True])
async def test_release_wakes_acquirer_after_cancelled_waiter(cancel_after_release):
    pool = await asyncmy.create_pool(minsize=1, maxsize=1, **connection_kwargs)
    waiters = []
    try:
        conn = await pool.acquire()
        waiters = [asyncio.ensure_future(pool.acquire()) for _ in range(2)]
        await asyncio.sleep(0)
        assert all(not waiter.done() for waiter in waiters)

        if cancel_after_release:
            released = pool.release(conn)
            assert released.done()
        waiters[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiters[0]
        if not cancel_after_release:
            released = pool.release(conn)
        await released

        acquired = await asyncio.wait_for(waiters[1], timeout=5)
        assert acquired is conn
        await pool.release(acquired)
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
async def test_release_notifies_while_condition_is_locked():
    pool = await asyncmy.create_pool(minsize=1, maxsize=1, **connection_kwargs)
    waiters = []
    try:
        conn = await pool.acquire()
        waiters = [asyncio.ensure_future(pool.acquire()) for _ in range(2)]
        await asyncio.sleep(0)

        async with pool.cond:
            notified = pool.cond._waiters[0]
            released = pool.release(conn)
            assert released.done()
            await asyncio.sleep(0)
            assert notified.done()
            assert not waiters[0].done()  # Must still reacquire the condition lock.
            waiters[0].cancel()
            await asyncio.sleep(0)

        with pytest.raises(asyncio.CancelledError):
            await waiters[0]
        acquired = await asyncio.wait_for(waiters[1], timeout=5)
        assert acquired is conn
        await pool.release(acquired)
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
async def test_coalesced_release_wakes_all_available_acquirers():
    pool = await asyncmy.create_pool(minsize=4, maxsize=4, **connection_kwargs)
    waiters = []
    try:
        held = [await pool.acquire() for _ in range(4)]
        waiters = [asyncio.ensure_future(pool.acquire()) for _ in range(4)]
        await asyncio.sleep(0)
        assert all(not waiter.done() for waiter in waiters)
        for conn in held:
            await pool.release(conn)
        acquired = await asyncio.wait_for(asyncio.gather(*waiters), timeout=5)
        assert set(acquired) == set(held)
        assert pool.freesize == 0
        for conn in acquired:
            await pool.release(conn)
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
async def test_reacquired_connection_notifies_when_available_again():
    pool = await asyncmy.create_pool(minsize=1, maxsize=1, **connection_kwargs)
    waiter = None
    try:
        conn = await pool.acquire()
        waiter = asyncio.ensure_future(pool.acquire())
        await asyncio.sleep(0)
        await pool.release(conn)
        reacquired = await pool.acquire()
        assert reacquired is conn
        await asyncio.sleep(0)
        assert not waiter.done()
        await pool.release(reacquired)
        acquired = await asyncio.wait_for(waiter, timeout=5)
        assert acquired is conn
        await pool.release(acquired)
    finally:
        if waiter is not None:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
async def test_close_rejects_parked_acquirers_and_wakes_shutdown():
    pool = await asyncmy.create_pool(minsize=1, maxsize=1, **connection_kwargs)
    waiters = []
    try:
        conn = await pool.acquire()
        waiters = [asyncio.ensure_future(pool.acquire()) for _ in range(3)]
        await asyncio.sleep(0)
        pool.close()
        shutdown = asyncio.create_task(pool.wait_closed())
        waiters.append(shutdown)
        await asyncio.sleep(0)
        await pool.release(conn)
        for waiter in waiters[:-1]:
            with pytest.raises(RuntimeError, match="after closing pool"):
                await asyncio.wait_for(waiter, timeout=5)
        await asyncio.wait_for(shutdown, timeout=5)
        assert pool.size == 0
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
async def test_cancel_notified_shutdown_wakes_next_waiter():
    pool = await asyncmy.create_pool(minsize=1, maxsize=1, **connection_kwargs)
    waiters = []
    try:
        conn = await pool.acquire()
        pool.close()
        waiters = [asyncio.create_task(pool.wait_closed()) for _ in range(2)]
        await asyncio.sleep(0)
        await pool.release(conn)
        assert pool.cond._waiters[0].done()
        waiters[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiters[0]
        await asyncio.wait_for(waiters[1], timeout=5)
        assert pool.size == 0
    finally:
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
async def test_acquire_rejects_broken_connection_behind_healthy_one():
    pool = await asyncmy.create_pool(minsize=2, maxsize=2, **connection_kwargs)
    try:
        healthy, broken = pool._free
        broken.close()
        first = await pool.acquire()
        assert first is healthy
        second = await pool.acquire()
        assert second is not broken
        assert second.connected
        await pool.release(first)
        await pool.release(second)
    finally:
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
async def test_acquire_recycles_expired_connection():
    pool = await asyncmy.create_pool(minsize=1, maxsize=1, pool_recycle=60, **connection_kwargs)
    try:
        expired = pool._free[0]
        expired._last_usage = pool._loop.time() - 120
        conn = await pool.acquire()
        assert conn is not expired
        assert conn.connected
        assert not expired.connected
        await pool.release(conn)
    finally:
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
async def test_acquire_replenishes_minimum_with_healthy_free_connection():
    pool = await asyncmy.create_pool(minsize=2, maxsize=2, **connection_kwargs)
    try:
        first = await pool.acquire()
        second = await pool.acquire()
        first.close()
        await pool.release(first)
        await pool.release(second)
        assert pool.size == 1

        conn = await pool.acquire()
        assert conn.connected
        assert pool.size == pool.minsize
        await pool.release(conn)
    finally:
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
async def test_wait_closed_wakes_on_in_transaction_release():
    """Releasing the last connection while it is inside a transaction must
    wake a parked wait_closed(), see #154."""
    pool = await asyncmy.create_pool(minsize=0, maxsize=4, autocommit=True, **connection_kwargs)
    conn = await pool.acquire()
    await conn.begin()
    async with conn.cursor() as cursor:
        await cursor.execute("SELECT 1")
        await cursor.fetchall()

    pool.close()
    waiter = asyncio.create_task(pool.wait_closed())
    await asyncio.sleep(0.1)  # let wait_closed() park on the condition
    assert not waiter.done()

    await pool.release(conn)
    await asyncio.wait_for(waiter, timeout=5)
    assert pool.size == 0


@pytest.mark.asyncio
async def test_wait_closed_wakes_on_disconnected_release():
    """Releasing a no-longer-connected connection must also wake wait_closed()."""
    pool = await asyncmy.create_pool(minsize=0, maxsize=4, autocommit=True, **connection_kwargs)
    conn = await pool.acquire()
    conn.close()

    pool.close()
    waiter = asyncio.create_task(pool.wait_closed())
    await asyncio.sleep(0.1)
    assert not waiter.done()

    await pool.release(conn)
    await asyncio.wait_for(waiter, timeout=5)
    assert pool.size == 0


@pytest.mark.asyncio
async def test_terminate_wakes_every_wait_closed_waiter():
    """terminate() must wake every wait_closed() parked on the condition, not
    just one of them, see #157.

    Unlike release(), which frees exactly one connection and so should only
    wake one waiter, terminate() invalidates the wait condition for all of
    them at once (size drops to 0 immediately), so it needs notify_all()
    rather than notify().
    """
    pool = await asyncmy.create_pool(minsize=1, maxsize=3, **connection_kwargs)
    conn = await pool.acquire()  # held, never released
    pool.close()

    waiters = [asyncio.create_task(pool.wait_closed()) for _ in range(3)]
    await asyncio.sleep(0.1)  # let them all park on the condition
    assert all(not w.done() for w in waiters)

    pool.terminate()
    done, pending = await asyncio.wait(waiters, timeout=5)

    assert not pending, f"{len(pending)} waiter(s) never woke up after terminate()"
    assert len(done) == 3
    for waiter in done:
        waiter.result()
    del conn


@pytest.mark.asyncio
async def test_cancel_wait_closed_closes_in_flight_connection(mocker):
    pool = await asyncmy.create_pool(minsize=2, maxsize=2, **connection_kwargs)
    victim = pool._free[0]
    transport = victim._transport
    entered = asyncio.Event()

    async def stalled_close():
        entered.set()
        await asyncio.Future()

    mocker.patch.object(victim, "ensure_closed", side_effect=stalled_close)
    pool.close()
    waiter = asyncio.create_task(pool.wait_closed())
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        pool.terminate()
        await asyncio.wait_for(pool.wait_closed(), timeout=5)
        assert pool.size == 0
        assert pool._closed
        assert transport.is_closing()
        assert victim._transport is None
    finally:
        victim.close()
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_first", [False, True])
async def test_concurrent_wait_closed_waits_for_in_flight_close(mocker, cancel_first):
    pool = await asyncmy.create_pool(minsize=1, maxsize=1, **connection_kwargs)
    conn = pool._free[0]
    original_close = conn.ensure_closed
    entered = asyncio.Event()
    proceed = asyncio.Event()

    async def slow_close():
        entered.set()
        await proceed.wait()
        await original_close()

    close = mocker.patch.object(conn, "ensure_closed", side_effect=slow_close)
    pool.close()
    first = asyncio.create_task(pool.wait_closed())
    waiters = [first]
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        waiters.extend(asyncio.create_task(pool.wait_closed()) for _ in range(2))
        await asyncio.sleep(0)
        assert all(not waiter.done() for waiter in waiters)
        assert not pool._closed
        if cancel_first:
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
        else:
            proceed.set()
        await asyncio.wait_for(asyncio.gather(*waiters[1:]), timeout=5)
        if not cancel_first:
            await first
        assert conn._transport is None
        assert pool.size == 0
        assert pool._closed
        close.assert_awaited_once()
    finally:
        proceed.set()
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        conn.close()
        pool.terminate()
        await pool.wait_closed()


@pytest.mark.asyncio
async def test_acquire_context_is_lazy_and_single_use(pool):
    free = pool.freesize
    context = pool.acquire()
    assert pool.freesize == free
    enter = context.__aenter__()
    assert pool.freesize == free
    conn = await enter
    assert conn in pool._used
    with pytest.raises(RuntimeError, match="already awaited"):
        await context
    await context.__aexit__(None, None, None)
    assert pool.freesize == free
    abandoned = pool.acquire()
    abandoned.close()
    with pytest.raises(RuntimeError):
        await abandoned
    assert pool.freesize == free


@pytest.mark.asyncio
@pytest.mark.parametrize("on_class", [False, True])
async def test_acquire_context_respects_acquire_hook(monkeypatch, on_class):
    from asyncmy.pool import Pool

    async with asyncmy.create_pool(minsize=1, maxsize=1, **connection_kwargs) as pool:
        original = pool._acquire
        calls = []

        async def acquire(*args):
            calls.append(True)
            return await original()

        monkeypatch.setattr(Pool if on_class else pool, "_acquire", acquire)
        async with pool.acquire() as conn:
            assert conn in pool._used
        assert calls == [True]


@pytest.mark.asyncio
async def test_pending_context_cannot_be_awaited_twice(pool):
    held = [await pool.acquire() for _ in range(pool.maxsize)]
    context = pool.acquire()

    async def borrow():
        return await context

    task = asyncio.create_task(borrow())
    try:
        await asyncio.sleep(0)
        with pytest.raises(RuntimeError, match="already awaited"):
            await context
        pool.release(held.pop())
        conn = await asyncio.wait_for(task, timeout=2)
        pool.release(conn)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        for conn in held:
            pool.release(conn)


@pytest.mark.asyncio
async def test_context_accepts_future_from_checkout_hook(monkeypatch):
    async with asyncmy.create_pool(minsize=1, maxsize=1, **connection_kwargs) as pool:
        conn = await pool.acquire()
        future = asyncio.get_running_loop().create_future()
        future.set_result(conn)
        monkeypatch.setattr(pool, "_acquire", lambda: future)
        async with pool.acquire() as borrowed:
            assert borrowed is conn
        assert pool.freesize == 1


@pytest.mark.asyncio
async def test_release_hook_does_not_suppress_body_exception(monkeypatch):
    async with asyncmy.create_pool(minsize=1, maxsize=1, **connection_kwargs) as pool:
        release = pool.release

        async def custom_release(conn):
            await release(conn)
            return True

        monkeypatch.setattr(pool, "release", custom_release)
        with pytest.raises(RuntimeError, match="body failure"):
            async with pool.acquire():
                raise RuntimeError("body failure")
        assert pool.freesize == 1
