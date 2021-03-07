import asyncio
import time

import aiomysql
import MySQLdb
import pymysql

import asyncmy

connection_kwargs = dict(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="123456",
    database="test",
)


async def benchmark_asyncmy():
    t = time.time()
    pool = await asyncmy.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for _ in range(10000):
                await cur.execute("SELECT 1,2,3,4,5")
                res = cur.fetchall()
                assert len(res) == 1
                assert res[0] == (1, 2, 3, 4, 5)
    print("asyncmy", time.time() - t)


async def benchmark_aiomysql():
    pool = await aiomysql.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            t = time.time()
            for _ in range(10000):
                await cur.execute("SELECT 1,2,3,4,5")
                res = await cur.fetchall()
                assert len(res) == 1
                assert res[0] == (1, 2, 3, 4, 5)
            print("aiomysql", time.time() - t)


def benchmark_mysqlclient():
    conn = MySQLdb.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for _ in range(10000):
        cur.execute("SELECT 1,2,3,4,5")
        res = cur.fetchall()
        assert len(res) == 1
        assert res[0] == (1, 2, 3, 4, 5)
    print("mysqlclient", time.time() - t)


def benchmark_pymysql():
    conn = pymysql.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for _ in range(10000):
        cur.execute("SELECT 1,2,3,4,5")
        res = cur.fetchall()
        assert len(res) == 1
        assert res[0] == (1, 2, 3, 4, 5)
    print("pymysql", time.time() - t)


if __name__ == "__main__":
    """
    pymysql 1.5898151397705078
    mysqlclient 0.5127310752868652
    aiomysql 1.7445728778839111
    asyncmy 1.369239091873169
    """
    import uvloop

    uvloop.install()
    loop = asyncio.get_event_loop()
    benchmark_pymysql()
    benchmark_mysqlclient()
    loop.run_until_complete(benchmark_aiomysql())
    loop.run_until_complete(benchmark_asyncmy())
