from collections.abc import Coroutine
from typing import Any, Iterator


class _ContextManager(Coroutine):
    __slots__ = ("_coro", "_obj")

    def __init__(self, coro: Coroutine) -> None:
        self._coro = coro
        self._obj: Any = None

    def send(self, value) -> Any:
        return self._coro.send(value)

    def throw(self, typ, val=None, tb=None) -> Any:
        if val is None:
            return self._coro.throw(typ)
        elif tb is None:
            return self._coro.throw(typ, val)
        else:
            return self._coro.throw(typ, val, tb)

    def close(self) -> None:
        return self._coro.close()

    @property
    def gi_frame(self) -> Any:
        return self._coro.gi_frame  # type:ignore[attr-defined]

    @property
    def gi_running(self) -> Any:
        return self._coro.gi_running  # type:ignore[attr-defined]

    @property
    def gi_code(self) -> Any:
        return self._coro.gi_code  # type:ignore[attr-defined]

    def __next__(self) -> Any:
        return self.send(None)

    def __iter__(self) -> Iterator:
        return self._coro.__await__()

    def __await__(self) -> Any:
        return self._coro.__await__()

    async def __aenter__(self) -> Any:
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._obj.close()
        self._obj = None


class _PoolContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._obj.close()
        await self._obj.wait_closed()
        self._obj = None


class _PoolAcquireContextManager(_ContextManager):
    __slots__ = ("_coro", "_conn", "_pool")

    def __init__(self, coro, pool) -> None:
        super().__init__(coro)
        self._coro = coro
        self._conn = None
        self._pool = pool

    async def __aenter__(self) -> Any:
        self._conn = await self._coro
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


class _ConnectionContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self._obj.close()
        else:
            await self._obj.ensure_closed()
        self._obj = None
