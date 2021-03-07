import time

import aiomysql
import MySQLdb
import pymysql

import asyncmy
from benchmark import COUNT, connection_kwargs


async def delete_asyncmy():
    pool = await asyncmy.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            t = time.time()
            for i in range(COUNT):
                ret = await cur.execute("delete from test.asyncmy limit 1")
                assert ret == 1
    return time.time() - t


async def delete_aiomysql():
    pool = await aiomysql.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            t = time.time()
            for i in range(COUNT):
                ret = await cur.execute("delete from test.asyncmy limit 1")
                assert ret == 1
            return time.time() - t


def delete_mysqlclient():
    conn = MySQLdb.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(COUNT):
        ret = cur.execute("delete from test.asyncmy limit 1")
        assert ret == 1
    return time.time() - t


def delete_pymysql():
    conn = pymysql.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(COUNT):
        ret = cur.execute("delete from test.asyncmy limit 1")
        assert ret == 1
    return time.time() - t
