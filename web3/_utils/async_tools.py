from functools import singledispatch, wraps
import asyncio
import inspect
import types
from typing import Any, Callable, Generator


def _is_awaitable(co: Generator[Any, None, Any]) -> bool:
    return inspect.isawaitable(co)


@singledispatch
def sync(co: Any):
    raise TypeError('Called with unsupported argument: {}'.format(co))


@sync.register(asyncio.Future)
@sync.register(types.GeneratorType)
def sync_co(co: Generator[Any, None, Any]) -> Any:
    if not _is_awaitable(co):
        raise TypeError('Called with unsupported argument: {}'.format(co))
    return asyncio.get_event_loop().run_until_complete(co)


@sync.register(types.FunctionType)
@sync.register(types.MethodType)
def sync_fu(f: Callable[..., Any]) -> Callable[..., Any]:
    if not asyncio.iscoroutinefunction(f):
        raise TypeError('Called with unsupported argument: {}'.format(f))

    @wraps(f)
    def run(*args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(f(*args, **kwargs))
    return run


sync.register(types.CoroutineType)(sync_co)
