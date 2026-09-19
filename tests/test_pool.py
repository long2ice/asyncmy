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
    del conn
