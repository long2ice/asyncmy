import asyncio
import time
from rich.pretty import pprint

import aiomysql
import asyncmy

from benchmark import (
    COUNT,
    conn_mysqlclient,
    conn_pymysql,
    connection_kwargs,
)
from benchmark.decorators import cleanup, fill_data


@cleanup
@fill_data
async def select_asyncmy():
    conn = await asyncmy.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(COUNT):
            await cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
            res = await cur.fetchall()
            assert len(res) == 1
        return time.time() - t


@cleanup
@fill_data
async def select_aiomysql():
    conn = await aiomysql.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(COUNT):
            await cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
            res = await cur.fetchall()
            assert len(res) == 1
        return time.time() - t


@cleanup
@fill_data
def select_mysqlclient():
    cur = conn_mysqlclient.cursor()
    t = time.time()
    for i in range(COUNT):
        cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
        res = cur.fetchall()
        assert len(res) == 1
    return time.time() - t


@cleanup
@fill_data
def select_pymysql():
    cur = conn_pymysql.cursor()
    t = time.time()
    for i in range(COUNT):
        cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
        res = cur.fetchall()
        assert len(res) == 1
    return time.time() - t


def benchmark_select():
    loop = asyncio.get_event_loop()
    select_mysqlclient_ret = select_mysqlclient()
    select_asyncmy_ret = loop.run_until_complete(select_asyncmy())
    select_pymysql_ret = select_pymysql()
    select_aiomysql_ret = loop.run_until_complete(select_aiomysql())
    return sorted(
        {
            "mysqlclient": select_mysqlclient_ret,
            "asyncmy": select_asyncmy_ret,
            "pymysql": select_pymysql_ret,
            "aiomysql": select_aiomysql_ret,
        }.items(),
        key=lambda x: x[1],
    )


if __name__ == "__main__":
    pprint(benchmark_select())
