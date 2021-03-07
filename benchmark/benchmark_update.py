import asyncio
import time

import aiomysql
import MySQLdb
import pymysql

import asyncmy
from benchmark import connection_kwargs


async def update_asyncmy():
    t = time.time()
    pool = await asyncmy.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for i in range(10000):
                await cur.execute(
                    "update test.asyncmy set `string`=%s where `string` != %s limit 1",
                    (
                        "update",
                        "update",
                    ),
                )
    print("asyncmy:", time.time() - t)


async def update_aiomysql():
    pool = await aiomysql.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            t = time.time()
            for i in range(10000):
                await cur.execute(
                    "update test.asyncmy set `string`=%s where `string` != %s limit 1",
                    (
                        "update",
                        "update",
                    ),
                )
            print("aiomysql:", time.time() - t)


def update_mysqlclient():
    conn = MySQLdb.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(10000):
        cur.execute(
            "update test.asyncmy set `string`=%s where `string` != %s limit 1",
            (
                "update",
                "update",
            ),
        )
    print("mysqlclient:", time.time() - t)


def update_pymysql():
    conn = pymysql.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(10000):
        cur.execute(
            "update test.asyncmy set `string`=%s where `string` != %s limit 1",
            (
                "update",
                "update",
            ),
        )
    print("pymysql:", time.time() - t)


if __name__ == "__main__":
    """
    mysqlclient: 4.354475021362305
    asyncmy: 4.9126691818237305
    pymysql: 4.584356069564819
    aiomysql: 4.210179090499878
    """
    import uvloop

    uvloop.install()
    loop = asyncio.get_event_loop()

    update_mysqlclient()
    loop.run_until_complete(update_asyncmy())
    update_pymysql()
    loop.run_until_complete(update_aiomysql())
