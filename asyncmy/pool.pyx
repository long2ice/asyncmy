# cython: freethreading_compatible=True
# Declares the module safe to import without re-enabling the GIL on
# free-threaded CPython. Module-level state (encoders/decoders, error_map,
# charset tables, escape table) is built during import and read-only after;
# per-connection state lives on the instances. It does NOT make a single
# Connection/Cursor safe to share between threads.
cimport cython
import asyncio
import collections
from collections.abc import Coroutine
from cpython.object cimport PyObject_GenericGetDict
from cpython.ref cimport PyObject, Py_INCREF
from typing import Deque, Set

from asyncmy.connection import Connection, connect
from asyncmy._compat cimport _class_method_is
from asyncmy.connection cimport _ConnectionState, _ProtocolState
from asyncmy.contexts import _PoolContextManager


cdef inline _take_free_connection(_PoolState pool):
    cdef _ConnectionState state
    cdef _ProtocolState proto
    cdef bint healthy
    cdef double now
    if pool._free and len(pool._free) + len(pool._used) + pool._acquiring >= pool._minsize:
        conn = pool._free[0]
        if type(conn) is Connection:
            state = conn
            proto = state._proto
            healthy = (state._transport is not None and not state._transport.is_closing()
                       and proto is not None and not proto.eof and proto.exc is None)
            if healthy and pool._recycle > -1:
                now = pool._loop.time()
                healthy = now - state._last_usage <= pool._recycle
        else:
            healthy = (not conn._stream_broken and (pool._recycle <= -1 or
                       pool._loop.time() - conn.last_usage <= pool._recycle))
        if healthy:
            pool._free.popleft()
            pool._used.add(conn)
            return conn
    return None


cdef class _PoolState:
    cdef:
        dict __dict__
        object __weakref__
        public object _loop, _free, _cond, _wakeup_handle, _release_done
        public dict _conn_kwargs
        public set _used, _terminated
        public Py_ssize_t _minsize, _acquiring
        public long _recycle
        public bint _closing, _closed, _echo


class Pool(_PoolState, asyncio.AbstractServer):
    """Connection pool, just from aiomysql"""

    @cython.locals(self=_PoolState)
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
        self._wakeup_handle = None
        self._release_done = self._loop.create_future()
        self._release_done.set_result(None)
        self._used: Set[Connection] = set()
        self._terminated: Set[Connection] = set()
        self._closing = False
        self._closed = False
        self._echo = echo
        self._recycle = int(pool_recycle)

    @property
    @cython.locals(self=_PoolState)
    def echo(self):
        return self._echo

    @property
    @cython.locals(self=_PoolState)
    def cond(self):
        return self._cond

    @property
    @cython.locals(self=_PoolState)
    def minsize(self):
        return self._minsize

    @property
    @cython.locals(self=_PoolState)
    def maxsize(self):
        return self._free.maxlen

    @property
    @cython.locals(self=_PoolState)
    def size(self):
        return self.freesize + len(self._used) + self._acquiring

    @property
    @cython.locals(self=_PoolState)
    def freesize(self):
        return len(self._free)

    @cython.locals(self=_PoolState)
    async def clear(self):
        """Close all free connections in pool."""
        async with self._cond:
            while self._free:
                conn = self._free.popleft()
                await conn.ensure_closed()
            self._cond.notify()

    @cython.locals(self=_PoolState)
    def close(self):
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow one to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    @cython.locals(self=_PoolState)
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
        # just one, unlike release()) re-check the predicate.
        self._loop.create_task(self._wakeup_all())

    @cython.locals(self=_PoolState)
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
                    try:
                        await self._cond.wait()
                    except BaseException:
                        self._cond.notify()
                        raise

            self._closed = True
            if self._wakeup_handle is not None:
                self._wakeup_handle.cancel()
                self._wakeup_handle = None
            self._cond.notify_all()

    @cython.locals(self=_PoolState)
    def acquire(self):
        """Acquire free connection from the pool."""
        return _PoolAcquireContext(self)

    @cython.locals(self=_PoolState)
    async def _acquire(self):
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        # No task can change pool state during this checkout: it contains no
        # await. Respect the lock if a drainer or connection creator holds it.
        if not self._cond.locked():
            conn = _take_free_connection(self)
            if conn is not None:
                return conn
        async with self._cond:
            while True:
                if self._closing:
                    self._cond.notify()
                    raise RuntimeError("Cannot acquire connection after closing pool")
                # Only the connection being checked out needs validation.
                # Leave the full sweep and replenishment to the slow path.
                conn = _take_free_connection(self)
                if conn is not None:
                    return conn
                await self.fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    self._used.add(conn)
                    return conn
                else:
                    try:
                        await self._cond.wait()
                    except BaseException:
                        # Older asyncio versions do not forward a notification
                        # when a notified task is cancelled before taking it.
                        self._cond.notify()
                        raise

    @cython.locals(self=_PoolState)
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

    @cython.locals(self=_PoolState)
    async def _wakeup(self):
        async with self._cond:
            self._cond.notify()

    @cython.locals(self=_PoolState)
    async def _wakeup_all(self):
        async with self._cond:
            self._cond.notify_all()

    @cython.locals(self=_PoolState)
    def _notify_available(self):
        self._wakeup_handle = None
        available = len(self._free)
        if self._closing:
            available = len(self._cond._waiters)
        elif not available:
            available = self.maxsize - self.size
        for waiter in self._cond._waiters:
            if available <= 0:
                break
            if not waiter.done():
                waiter.set_result(False)
                available -= 1

    @cython.locals(self=_PoolState)
    def release(self, conn: Connection):
        """
        Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """
        cdef bint connected, in_trans
        if conn in self._terminated:
            self._terminated.remove(conn)
            return self._release_done
        self._used.remove(conn)
        if type(conn) is Connection:
            connected = (<_ConnectionState>conn)._connected
            in_trans = (<_ConnectionState>conn).server_status & 1
        else:
            connected = conn.connected
            in_trans = conn.get_transaction_status() if connected else False
        if connected:
            if in_trans:
                # connection with an open transaction is discarded, see #36
                conn.close()
            elif self._closing:
                conn.close()
            else:
                self._free.append(conn)
        # Coalesce availability checks until the current ready tasks finish.
        # A releaser may immediately borrow the same connection again; waking
        # an acquirer in that case only makes it park on the condition again.
        # No task or condition-lock coroutine is needed for notification.
        if self._cond._waiters:
            if self._closing:
                if self._wakeup_handle is not None:
                    self._wakeup_handle.cancel()
                self._notify_available()
            elif self._wakeup_handle is None:
                self._wakeup_handle = self._loop.call_soon(self._notify_available)
        return self._release_done

    @cython.locals(self=_PoolState)
    async def __aenter__(self):
        return self

    @cython.locals(self=_PoolState)
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        await self.wait_closed()



cdef object _original_acquire = Pool._acquire


async def _completed_acquire():
    return None


async def _await_checkout(awaitable):
    return await awaitable


@cython.freelist(64)
cdef class _PoolAcquireContext:
    """Lazy checkout iterator; the uncontended path creates no coroutine."""
    cdef _PoolState _pool
    cdef object _coro, _conn
    cdef bint _started, _finished, _context, _awaited

    def __init__(self, pool):
        self._pool = pool
        self._coro = self._conn = None
        self._started = self._finished = self._context = self._awaited = False

    cdef _get_coro(self):
        if self._coro is None:
            if self._finished:
                self._coro = _completed_acquire()
                try:
                    self._coro.send(None)
                except StopIteration:
                    pass
            else:
                self._coro = self._pool._acquire()
                if not hasattr(self._coro, "send"):
                    self._coro = _await_checkout(self._coro)
        return self._coro

    cdef _try_fast(self, value):
        cdef _PoolState pool = self._pool
        if (not self._started and self._coro is None and value is None and type(pool) is Pool
                and _class_method_is(pool, "_acquire", _original_acquire)
                and "_acquire" not in <dict>PyObject_GenericGetDict(pool, NULL)
                and not pool._closing and not pool._cond.locked()):
            conn = _take_free_connection(pool)
            if conn is not None:
                self._started = self._finished = True
                self._awaited = True
                if self._context:
                    self._conn = conn
                return conn
        return None

    cdef _send(self, value):
        conn = self._try_fast(value)
        if conn is not None:
            raise StopIteration(conn)
        self._started = True
        self._awaited = True
        try:
            return self._get_coro().send(value)
        except StopIteration as exc:
            self._finished = True
            if self._context:
                self._conn = exc.value
            raise

    def send(self, value):
        return self._send(value)

    def __next__(self):
        return self._send(None)

    def __iter__(self):
        return self

    def __await__(self):
        if self._awaited:
            raise RuntimeError("cannot reuse already awaited pool acquire")
        self._awaited = True
        return self

    def throw(self, typ, val=None, tb=None):
        self._started = True
        self._awaited = True
        coro = self._get_coro()
        if val is None:
            return coro.throw(typ)
        if tb is None:
            return coro.throw(typ, val)
        return coro.throw(typ, val, tb)

    def close(self):
        self._started = self._finished = True
        if self._coro is not None:
            return self._coro.close()

    @property
    def gi_frame(self):
        return self._get_coro().gi_frame

    @property
    def gi_running(self):
        return self._get_coro().gi_running

    @property
    def gi_code(self):
        return self._get_coro().gi_code

    def __aenter__(self):
        self._context = True
        return self

    def __aexit__(self, exc_type, exc, tb):
        try:
            released = self._pool.release(self._conn)
        except BaseException:
            self._pool = None
            self._conn = None
            raise
        if released is self._pool._release_done:
            self._pool = None
            self._conn = None
            return released
        return self._finish_release(released)

    async def _finish_release(self, released):
        try:
            await released
        finally:
            self._pool = None
            self._conn = None


cdef int _pool_context_send(object context, object value, PyObject** result) except -1:
    cdef _PoolAcquireContext ctx = context
    cdef int status = 0
    result[0] = NULL
    item = ctx._try_fast(value)
    if item is None:
        try:
            item = ctx._send(value)
            status = 1
        except StopIteration as exc:
            item = exc.value
    Py_INCREF(item)
    result[0] = <PyObject*>item
    return status


cdef extern from *:
    """
    static void asyncmy_set_context_send(PyObject *type, void *func) {
    /* PyIter_Send's public result protocol avoids constructing a
       StopIteration exception for an immediate checkout. Cython does not
       currently generate am_send for ordinary extension iterators. */
    #if PY_VERSION_HEX >= 0x030A0000 && CYTHON_COMPILING_IN_CPYTHON && !defined(Py_LIMITED_API)
        ((PyTypeObject*)type)->tp_as_async->am_send = (sendfunc)func;
        PyType_Modified((PyTypeObject*)type);
    #endif
    }
    """
    void asyncmy_set_context_send(object type, void* func)


asyncmy_set_context_send(_PoolAcquireContext, <void*>_pool_context_send)
Coroutine.register(_PoolAcquireContext)

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
