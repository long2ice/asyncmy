import time

import aiomysql
import faker

import asyncmy
from benchmark import INSERT_COUNT, conn_mysqlclient, conn_pymysql, connection_kwargs

faker = faker.Faker()
data = [
    (
        1,
        faker.date_time().date(),
        faker.date_time(),
        1,
        faker.name(),
        1,
    )
    for _ in range(INSERT_COUNT)
]
sql = """INSERT INTO test.asyncmy(`decimal`, `date`, `datetime`, `float`, `string`, `tinyint`) VALUES (%s,%s,%s,%s,%s,%s)"""


async def insert_asyncmy():
    pool = await asyncmy.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            t = time.time()
            ret = await cur.executemany(sql, data)
            assert ret == INSERT_COUNT
            return time.time() - t


async def insert_aiomysql():
    pool = await aiomysql.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            t = time.time()
            ret = await cur.executemany(sql, data)
            assert ret == INSERT_COUNT
            return time.time() - t


def insert_mysqlclient():
    cur = conn_mysqlclient.cursor()
    t = time.time()
    ret = cur.executemany(sql, data)
    assert ret == INSERT_COUNT
    return time.time() - t


def insert_pymysql():
    cur = conn_pymysql.cursor()
    t = time.time()
    ret = cur.executemany(sql, data)
    assert ret == INSERT_COUNT
    return time.time() - t
