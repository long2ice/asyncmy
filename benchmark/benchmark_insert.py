import time

import aiomysql

import asyncmy
from benchmark import COUNT, conn_mysqlclient, conn_pymysql, connection_kwargs, data, sql
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
