# cython: freethreading_compatible=True
# Declares the module safe to import without re-enabling the GIL on
# free-threaded CPython. Module-level state (encoders/decoders, error_map,
# charset tables, escape table) is built during import and read-only after;
# per-connection state lives on the instances. It does NOT make a single
# Connection/Cursor safe to share between threads.
import asyncio
import collections
from typing import Deque, Set

from asyncmy.connection import Connection, connect
from asyncmy.contexts import _PoolAcquireContextManager, _PoolContextManager


class Pool(asyncio.AbstractServer):
    """Connection pool, just from aiomysql"""

    def __init__(
            self, minsize: int, maxsize: int, pool_recycle: int = 3600, echo: bool = False, **kwargs
    ):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize:
            raise ValueError("maxsize should be not less than minsize")
        self._minsize = minsize
        self._loop = asyncio.get_event_loop()
        self._conn_kwargs = {**kwargs, "echo": echo}
        self._acquiring = 0
        self._free: Deque[Connection] = collections.deque(maxlen=maxsize)
        self._cond = asyncio.Condition()
        self._used: Set[Connection] = set()
        self._terminated: Set[Connection] = set()
        self._closing = False
        self._closed = False
        self._echo = echo
        self._recycle = int(pool_recycle)

    @property
    def echo(self):
        return self._echo

    @property
    def cond(self):
        return self._cond

    @property
    def minsize(self):
        return self._minsize

    @property
    def maxsize(self):
        return self._free.maxlen

    @property
    def size(self):
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        return len(self._free)

    async def clear(self):
        """Close all free connections in pool."""
        async with self._cond:
            while self._free:
                conn = self._free.popleft()
                await conn.ensure_closed()
            self._cond.notify()

    def close(self):
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow one to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    def terminate(self):
        """Terminate pool.

        Close pool with instantly closing all acquired connections also.
        """

        self.close()

        for conn in list(self._used):
            conn.close()
            self._terminated.add(conn)

        self._used.clear()
        # size <= freesize now for every waiter parked in wait_closed(), but
        # nothing else will tell them: schedule a wake so all of them (not
        # just one, unlike release()'s _wakeup()) re-check the predicate.
        self._loop.create_task(self._wakeup_all())

    async def wait_closed(self):
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called " "after .close()")

        async with self._cond:
            # Serialize drainers so none can report closure while another is
            # still closing a connection removed from _free.
            while self.size:
                while self._free:
                    conn = self._free.popleft()
                    try:
                        await conn.ensure_closed()
                    except asyncio.CancelledError:
                        # The connection is no longer tracked by the pool.
                        # Close it before propagating the shutdown deadline.
                        conn.close()
                        raise
                    except Exception:
                        # peer may already be gone; fall back to abrupt close
                        conn.close()
                if self.size:
                    await self._cond.wait()

            self._closed = True
            self._cond.notify_all()

    def acquire(self):
        """Acquire free connection from the pool."""
        coro = self._acquire()
        return _PoolAcquireContextManager(coro, self)

    async def _acquire(self):
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        async with self._cond:
            while True:
                await self.fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    self._used.add(conn)
                    return conn
                else:
                    await self._cond.wait()

    async def fill_free_pool(self, override_min: bool = False):
        # iterate over free connections and remove timeouted ones
        free_size = len(self._free)
        n = 0
        while n < free_size:
            conn = self._free[-1]
            if conn._stream_broken:
                self._free.pop()
                conn.close()

            elif self._recycle > -1 and self._loop.time() - conn.last_usage > self._recycle:
                self._free.pop()
                try:
                    await conn.ensure_closed()
                except Exception:
                    # peer may already be gone; fall back to abrupt close
                    conn.close()

            else:
                self._free.rotate()
            n += 1

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await connect(**self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
            finally:
                # decrementing _acquiring changes size, so waiters must be
                # notified even when connect() raised
                self._acquiring -= 1
                self._cond.notify()
        if self._free:
            return

        if override_min and self.size < self.maxsize:
            self._acquiring += 1
            try:
                conn = await connect(**self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
            finally:
                self._acquiring -= 1
                self._cond.notify()

    async def _wakeup(self):
        async with self._cond:
            self._cond.notify()

    async def _wakeup_all(self):
        async with self._cond:
            self._cond.notify_all()

    def release(self, conn: Connection):
        """
        Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """
        fut = self._loop.create_future()
        fut.set_result(None)

        if conn in self._terminated:
            self._terminated.remove(conn)
            return fut
        self._used.remove(conn)
        if conn.connected:
            in_trans = conn.get_transaction_status()
            if in_trans:
                # connection with an open transaction is discarded, see #36
                conn.close()
            elif self._closing:
                conn.close()
            else:
                self._free.append(conn)
        # removing from _used changed size, so wait_closed()/acquire() waiters
        # must be woken on every path
        return self._loop.create_task(self._wakeup())

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        await self.wait_closed()


def create_pool(
        minsize: int = 1, maxsize: int = 10, echo = False, pool_recycle: int = 3600, **kwargs
):
    coro = _create_pool(
        minsize = minsize, maxsize = maxsize, echo = echo, pool_recycle = pool_recycle, **kwargs
    )
    return _PoolContextManager(coro)

async def _create_pool(
        minsize: int = 1, maxsize: int = 10, echo = False, pool_recycle: int = 3600, **kwargs
):
    pool = Pool(
        minsize = minsize, maxsize = maxsize, echo = echo, pool_recycle = pool_recycle, **kwargs
    )
    if minsize > 0:
        async with pool.cond:
            await pool.fill_free_pool(False)
    return pool
