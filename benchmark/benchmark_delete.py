import asyncio
import time
from rich.pretty import pprint

import aiomysql
import asyncmy
import MySQLdb
import pymysql

from benchmark import COUNT, connection_kwargs
from benchmark.decorators import cleanup, fill_data

count = int(COUNT / 5)


@cleanup
@fill_data
async def delete_asyncmy():
    conn = await asyncmy.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(count):
            ret = await cur.execute("delete from test.asyncmy where `id`=%s", (i + 1,))
            assert ret == 1
        return time.time() - t


@cleanup
@fill_data
async def delete_aiomysql():
    conn = await aiomysql.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(count):
            ret = await cur.execute("delete from test.asyncmy where `id`=%s", (i + 1,))
            assert ret == 1
        return time.time() - t


@cleanup
@fill_data
def delete_mysqlclient():
    conn = MySQLdb.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(count):
        ret = cur.execute("delete from test.asyncmy where `id`=%s", (i + 1,))
        assert ret == 1
    return time.time() - t


@cleanup
@fill_data
def delete_pymysql():
    conn = pymysql.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(count):
        ret = cur.execute("delete from test.asyncmy where `id`=%s", (i + 1,))
        assert ret == 1
    return time.time() - t


def benchmark_delete():
    loop = asyncio.get_event_loop()
    delete_asyncmy_ret = loop.run_until_complete(delete_asyncmy())
    delete_mysqlclient_ret = delete_mysqlclient()
    delete_pymysql_ret = delete_pymysql()
    delete_aiomysql_ret = loop.run_until_complete(delete_aiomysql())
    return sorted(
        {
            "mysqlclient": delete_mysqlclient_ret,
            "asyncmy": delete_asyncmy_ret,
            "pymysql": delete_pymysql_ret,
            "aiomysql": delete_aiomysql_ret,
        }.items(),
        key=lambda x: x[1],
    )


if __name__ == "__main__":
    pprint(benchmark_delete())
