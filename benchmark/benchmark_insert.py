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
    data,
    sql,
)
from benchmark.decorators import cleanup


@cleanup
async def insert_asyncmy():
    conn = await asyncmy.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        ret = await cur.executemany(sql, data)
        assert ret == COUNT
        return time.time() - t


@cleanup
async def insert_aiomysql():
    conn = await aiomysql.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        ret = await cur.executemany(sql, data)
        assert ret == COUNT
        return time.time() - t


@cleanup
def insert_mysqlclient():
    cur = conn_mysqlclient.cursor()
    t = time.time()
    ret = cur.executemany(sql, data)
    assert ret == COUNT
    return time.time() - t


@cleanup
def insert_pymysql():
    cur = conn_pymysql.cursor()
    t = time.time()
    ret = cur.executemany(sql, data)
    assert ret == COUNT
    return time.time() - t


def benchmark_insert():
    loop = asyncio.get_event_loop()
    insert_mysqlclient_ret = insert_mysqlclient()
    insert_asyncmy_ret = loop.run_until_complete(insert_asyncmy())
    insert_pymysql_ret = insert_pymysql()
    insert_aiomysql_ret = loop.run_until_complete(insert_aiomysql())
    return sorted(
        {
            "mysqlclient": insert_mysqlclient_ret,
            "asyncmy": insert_asyncmy_ret,
            "pymysql": insert_pymysql_ret,
            "aiomysql": insert_aiomysql_ret,
        }.items(),
        key=lambda x: x[1],
    )


if __name__ == "__main__":
    pprint(benchmark_insert())
