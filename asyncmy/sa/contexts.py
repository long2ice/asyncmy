from asyncmy.contexts import _ContextManager


class _SAConnectionContextManager(_ContextManager):
    async def __aiter__(self):
        result = await self._coro
        return result


class _TransactionContextManager(_ContextManager):
    async def __aexit__(self, exc_type, exc, tb):
        if exc_type:
            await self._obj.rollback()
        else:
            if self._obj.is_active:
                await self._obj.commit()
        self._obj = None
