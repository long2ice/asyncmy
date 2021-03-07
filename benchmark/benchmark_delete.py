import asyncio
import time

import aiomysql
import MySQLdb
import pymysql

import asyncmy
from benchmark import connection_kwargs


async def delete_asyncmy():
    t = time.time()
    pool = await asyncmy.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for i in range(10000):
                ret = await cur.execute("delete from test.asyncmy limit 1")
                assert ret == 1
    print("asyncmy:", time.time() - t)


async def delete_aiomysql():
    pool = await aiomysql.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            t = time.time()
            for i in range(10000):
                ret = await cur.execute("delete from test.asyncmy limit 1")
                assert ret == 1
            print("aiomysql:", time.time() - t)


def delete_mysqlclient():
    conn = MySQLdb.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(10000):
        ret = cur.execute("delete from test.asyncmy limit 1")
        assert ret == 1
    print("mysqlclient:", time.time() - t)


def delete_pymysql():
    conn = pymysql.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(10000):
        ret = cur.execute("delete from test.asyncmy limit 1")
        assert ret == 1
    print("pymysql:", time.time() - t)


if __name__ == "__main__":
    """
    mysqlclient: 3.2380189895629883
    asyncmy: 3.498440980911255
    pymysql: 3.875216007232666
    aiomysql: 3.5140841007232666
    """
    import uvloop

    uvloop.install()
    loop = asyncio.get_event_loop()

    delete_mysqlclient()
    loop.run_until_complete(delete_asyncmy())
    delete_pymysql()
    loop.run_until_complete(delete_aiomysql())
