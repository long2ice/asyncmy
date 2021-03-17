import time

import aiomysql
import MySQLdb
import pymysql

import asyncmy
from benchmark import COUNT, connection_kwargs
from benchmark.decorators import cleanup, fill_data


@cleanup
@fill_data
async def delete_asyncmy():
    conn = await asyncmy.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(COUNT):
            ret = await cur.execute("delete from test.asyncmy where `id`=%s", (i + 1,))
            assert ret == 1
        return time.time() - t


@cleanup
@fill_data
async def delete_aiomysql():
    conn = await aiomysql.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(COUNT):
            ret = await cur.execute("delete from test.asyncmy where `id`=%s", (i + 1,))
            assert ret == 1
        return time.time() - t


@cleanup
@fill_data
def delete_mysqlclient():
    conn = MySQLdb.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(COUNT):
        ret = cur.execute("delete from test.asyncmy where `id`=%s", (i + 1,))
        assert ret == 1
    return time.time() - t


@cleanup
@fill_data
def delete_pymysql():
    conn = pymysql.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(COUNT):
        ret = cur.execute("delete from test.asyncmy where `id`=%s", (i + 1,))
        assert ret == 1
    return time.time() - t
