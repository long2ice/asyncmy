import asyncio
import functools
from asyncio import iscoroutine

from benchmark import conn_mysqlclient, data, sql


def cleanup(f):
    @functools.wraps(f)
    def decorator(*args, **kwargs):
        cur = conn_mysqlclient.cursor()
        cur.execute("truncate table test.asyncmy")
        if iscoroutine(f):
            return asyncio.get_event_loop().run_until_complete(f(*args, **kwargs))
        else:
            return f(*args, **kwargs)

    return decorator


def fill_data(f):
    @functools.wraps(f)
    def decorator(*args, **kwargs):
        cur = conn_mysqlclient.cursor()
        cur.executemany(sql, data)
        if iscoroutine(f):
            return asyncio.get_event_loop().run_until_complete(f(*args, **kwargs))
        else:
            return f(*args, **kwargs)

    return decorator
