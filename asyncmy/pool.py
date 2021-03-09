import asyncio
import collections
from asyncio import Condition
from typing import Deque, Set

from asyncmy.connection import Connection, connect
from asyncmy.contexts import _PoolAcquireContextManager, _PoolContextManager


class Pool(asyncio.AbstractServer):
    def __init__(self, minsize: int = 1, maxsize: int = 10, loop=None, **kwargs):
        self._maxsize = maxsize
        self._minsize = minsize
        self._connection_kwargs = kwargs
        self._terminated: Set[Connection] = set()
        self._used: Set[Connection] = set()
        self._cond = Condition(loop=loop)
        self._closing = False
        self._closed = False
        self._free: Deque[Connection] = collections.deque(maxlen=maxsize)
        self._loop = loop

        if maxsize <= 0:
            raise ValueError("maxsize is expected to be greater than zero")

        if minsize < 0:
            raise ValueError("minsize is expected to be greater or equal to zero")

        if minsize > maxsize:
            raise ValueError("minsize is greater than max_size")

    @property
    def maxsize(self):
        return self._maxsize

    @property
    def minsize(self):
        return self._minsize

    @property
    def freesize(self):
        return len(self._free)

    @property
    def size(self):
        return self.freesize + len(self._used)

    @property
    def cond(self):
        return self._cond

    async def release(self, connection: Connection):
        """Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """
        fut = self._loop.create_future()
        fut.set_result(None)

        if connection in self._terminated:
            self._terminated.remove(connection)
            return fut
        self._used.remove(connection)
        if connection.connected:
            in_trans = connection.get_transaction_status()
            if in_trans:
                connection.close()
                return fut
            if self._closing:
                connection.close()
            else:
                self._free.append(connection)
            fut = self._loop.create_task(self._wakeup())
        return fut

    async def _wakeup(self):
        async with self._cond:
            self._cond.notify()

    def _wait(self):
        return len(self._terminated) > 0

    def acquire(self):
        return _PoolAcquireContextManager(self._acquire(), self)

    async def _acquire(self) -> Connection:
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        async with self._cond:
            while True:
                await self.initialize()
                if self._free:
                    conn = self._free.popleft()
                    self._used.add(conn)
                    return conn
                else:
                    await self._cond.wait()

    async def initialize(self):
        free_size = len(self._free)
        n = 0
        while n < free_size:
            conn = self._free[-1]
            if conn._reader.at_eof() or conn._reader.exception():
                self._free.pop()
                conn.close()
            n += 1
        while self.size < self.minsize:
            conn = await connect(**self._connection_kwargs)
            self._free.append(conn)
            self._cond.notify()

    async def clear(self):
        """Close all free connections in pool."""
        async with self._cond:
            while self._free:
                conn = self._free.popleft()
                await conn.ensure_closed()
            self._cond.notify()

    async def wait_closed(self):
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called " "after .close()")

        while self._free:
            conn = self._free.popleft()
            conn.close()

        async with self._cond:
            while self.size > self.freesize:
                await self._cond.wait()

        self._closed = True

    def close(self):
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    async def terminate(self):
        """Terminate pool.

        Close pool with instantly closing all acquired connections also.
        """

        self.close()

        for conn in self._used:
            await conn.close()
            self._terminated.add(conn)

        self._used.clear()


def create_pool(minsize: int = 1, maxsize: int = 10, loop=None, **kwargs) -> _PoolContextManager:
    coro = _create_pool(minsize=minsize, maxsize=maxsize, loop=loop, **kwargs)
    return _PoolContextManager(coro)


async def _create_pool(minsize: int = 1, maxsize: int = 10, loop=None, **kwargs) -> Pool:
    if loop is None:
        loop = asyncio.get_event_loop()
    pool = Pool(minsize, maxsize, loop, **kwargs)
    if minsize > 0:
        async with pool.cond:
            await pool.initialize()
    return pool
