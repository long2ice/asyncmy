import asyncio
import time

import aiomysql
import pymysql
import MySQLdb
from asyncmy.connections import Connection


async def benchmark_asyncmy():
    t = time.time()
    connection = Connection(user="root", password="123456")
    await connection.connect()
    with connection.cursor() as cursor:
        for _ in range(100000):
            await cursor.execute("SELECT 1,2,3,4,5")
            res = cursor.fetchall()
            assert len(res) == 1
            assert res[0] == (1, 2, 3, 4, 5)
    print('asyncmy', time.time() - t)


async def benchmark_aiomysql():
    pool = await aiomysql.create_pool(user='root', password='123456')
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            t = time.time()
            for _ in range(100000):
                await cur.execute("SELECT 1,2,3,4,5")
                res = await cur.fetchall()
                assert len(res) == 1
                assert res[0] == (1, 2, 3, 4, 5)
            print("aiomysql", time.time() - t)


def benchmark_mysqlclient():
    conn = MySQLdb.connect(user='root', host='localhost', password='123456')
    cur = conn.cursor()
    t = time.time()
    for _ in range(100000):
        cur.execute("SELECT 1,2,3,4,5")
        res = cur.fetchall()
        assert len(res) == 1
        assert res[0] == (1, 2, 3, 4, 5)
    print('mysqlclient', time.time() - t)


def benchmark_pymysql():
    conn = pymysql.connect(user='root', host='localhost', password='123456')
    cur = conn.cursor()
    t = time.time()
    for _ in range(100000):
        cur.execute("SELECT 1,2,3,4,5")
        res = cur.fetchall()
        assert len(res) == 1
        assert res[0] == (1, 2, 3, 4, 5)
    print("pymysql", time.time() - t)


if __name__ == '__main__':
    import uvloop
    uvloop.install()
    loop = asyncio.get_event_loop()
    benchmark_pymysql()
    benchmark_mysqlclient()
    loop.run_until_complete(benchmark_aiomysql())
    loop.run_until_complete(benchmark_asyncmy())
