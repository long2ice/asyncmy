import asyncio
import time

import aiomysql

import asyncmy
from benchmark import conn_mysqlclient, conn_pymysql, connection_kwargs


async def select_asyncmy():
    t = time.time()
    pool = await asyncmy.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for i in range(10000):
                await cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
                res = cur.fetchall()
                assert len(res) == 1
    print("asyncmy:", time.time() - t)


async def select_aiomysql():
    pool = await aiomysql.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            t = time.time()
            for i in range(10000):
                await cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
                res = await cur.fetchall()
                assert len(res) == 1
            print("aiomysql:", time.time() - t)


def select_mysqlclient():
    cur = conn_mysqlclient.cursor()
    t = time.time()
    for i in range(10000):
        cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
        res = cur.fetchall()
        assert len(res) == 1
    print("mysqlclient:", time.time() - t)


def select_pymysql():
    cur = conn_pymysql.cursor()
    t = time.time()
    for i in range(10000):
        cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
        res = cur.fetchall()
        assert len(res) == 1
    print("pymysql:", time.time() - t)


if __name__ == "__main__":
    """
    mysqlclient: 1.1456818580627441
    asyncmy: 1.9517629146575928
    pymysql: 2.184417963027954
    aiomysql: 2.4154791831970215
    """
    import uvloop

    uvloop.install()
    loop = asyncio.get_event_loop()

    select_mysqlclient()
    loop.run_until_complete(select_asyncmy())
    select_pymysql()
    loop.run_until_complete(select_aiomysql())
