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
async def update_asyncmy():
    conn = await asyncmy.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(count):
            await cur.execute(
                "update test.asyncmy set `string`=%s where `id` = %s",
                (
                    "update",
                    i + 1,
                ),
            )
        return time.time() - t


@cleanup
@fill_data
async def update_aiomysql():
    conn = await aiomysql.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(count):
            await cur.execute(
                "update test.asyncmy set `string`=%s where `id` = %s",
                (
                    "update",
                    i + 1,
                ),
            )
        return time.time() - t


@cleanup
@fill_data
def update_mysqlclient():
    conn = MySQLdb.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(count):
        cur.execute(
            "update test.asyncmy set `string`=%s where `id` = %s",
            (
                "update",
                i + 1,
            ),
        )
    return time.time() - t


@cleanup
@fill_data
def update_pymysql():
    conn = pymysql.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(count):
        cur.execute(
            "update test.asyncmy set `string`=%s where `id` = %s",
            (
                "update",
                i + 1,
            ),
        )
    return time.time() - t


def benchmark_update():
    loop = asyncio.get_event_loop()
    update_mysqlclient_ret = update_mysqlclient()
    update_asyncmy_ret = loop.run_until_complete(update_asyncmy())
    update_pymysql_ret = update_pymysql()
    update_aiomysql_ret = loop.run_until_complete(update_aiomysql())
    return sorted(
        {
            "mysqlclient": update_mysqlclient_ret,
            "asyncmy": update_asyncmy_ret,
            "pymysql": update_pymysql_ret,
            "aiomysql": update_aiomysql_ret,
        }.items(),
        key=lambda x: x[1],
    )


if __name__ == "__main__":
    pprint(benchmark_update())
