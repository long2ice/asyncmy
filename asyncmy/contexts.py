from __future__ import annotations
from collections.abc import Coroutine
from typing import Any, Generic, TypeVar
from collections.abc import Generator, Iterator

_T = TypeVar("_T")


class _ContextManager(Coroutine[Any, Any, _T], Generic[_T]):
    """Awaitable that is also an async context manager yielding ``_T``.

    ``await connect(...)`` and ``async with connect(...)`` both hand back the
    same object, so the parameter is what lets callers get a real type out of
    either spelling.
    """

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

    def __await__(self) -> Generator[Any, None, _T]:
        return self._coro.__await__()

    async def __aenter__(self) -> _T:
        self._obj = await self._coro
        return self._obj

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._obj.close()
        self._obj = None


class _PoolContextManager(_ContextManager[_T]):
    async def __aexit__(self, exc_type, exc, tb) -> None:
        self._obj.close()
        await self._obj.wait_closed()
        self._obj = None


class _PoolAcquireContextManager(_ContextManager[_T]):
    __slots__ = ("_conn", "_pool")

    def __init__(self, coro, pool) -> None:
        self._coro = coro
        self._obj = None
        self._conn: Any = None
        self._pool = pool

    async def __aenter__(self) -> _T:
        self._conn = await self._coro
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            await self._pool.release(self._conn)
        finally:
            self._pool = None
            self._conn = None


class _ConnectionContextManager(_ContextManager[_T]):
    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            self._obj.close()
        else:
            await self._obj.ensure_closed()
        self._obj = None
