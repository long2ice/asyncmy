import asyncio
import collections
from asyncio import Condition
from collections.abc import Coroutine
from typing import Deque, Set

from asyncmy.connection import Connection, connect


class _ContextManager(Coroutine):
    __slots__ = ("_coro", "_obj")

    def __init__(self, coro):
        self._coro = coro
        self._obj = None

    def send(self, value):
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None):
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self):
        return self._coro.close()

    @property
    def gi_frame(self):
        return self._coro.gi_frame

    @property
    def gi_running(self):
        return self._coro.gi_running

    @property
    def gi_code(self):
        return self._coro.gi_code

    def __next__(self):
        return self.send(None)

    def __iter__(self):
        return self._coro.__await__()

    def __await__(self):
        return self._coro.__await__()

    async def __aenter__(self):
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type, exc, tb):
        await self._obj.close()
        self._obj = None


class _PoolContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb):
        self._obj.close()
        await self._obj.wait_closed()
        self._obj = None


class _PoolAcquireContextManager(_ContextManager):
    __slots__ = ("_coro", "_conn", "_pool")

    def __init__(self, coro, pool):
        super().__init__(coro)
        self._coro = coro
        self._conn = None
        self._pool = pool

    async def __aenter__(self):
        self._conn = await self._coro
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        try:
            await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


class Pool(asyncio.AbstractServer):
    def __init__(self, minsize: int = 1, maxsize: int = 10, loop=None, **kwargs):
        self._maxsize = maxsize
        self._minsize = minsize
        self._connection_kwargs = kwargs
        self._terminated: Set[Connection] = set()
        self._used: Set[Connection] = set()
        self._cond = Condition()
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
            if self._closing:
                await connection.close()
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
        while self.size < self.minsize:
            conn = await connect(**self._connection_kwargs)
            self._free.append(conn)
            self._cond.notify()

    async def clear(self):
        """Close all free connections in pool."""
        async with self._cond:
            while self._free:
                conn = self._free.popleft()
                await conn.close()
            self._cond.notify()

    async def wait_closed(self):
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called " "after .close()")

        while self._free:
            conn = self._free.popleft()
            await conn.close()

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
