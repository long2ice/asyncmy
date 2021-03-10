import time

import aiomysql

import asyncmy
from benchmark import COUNT, conn_mysqlclient, conn_pymysql, connection_kwargs


async def select_asyncmy():
    conn = await asyncmy.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(COUNT):
            await cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
            res = await cur.fetchall()
            assert len(res) == 1
        return time.time() - t


async def select_aiomysql():
    conn = await aiomysql.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(COUNT):
            await cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
            res = await cur.fetchall()
            assert len(res) == 1
        return time.time() - t


def select_mysqlclient():
    cur = conn_mysqlclient.cursor()
    t = time.time()
    for i in range(COUNT):
        cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
        res = cur.fetchall()
        assert len(res) == 1
    return time.time() - t


def select_pymysql():
    cur = conn_pymysql.cursor()
    t = time.time()
    for i in range(COUNT):
        cur.execute("SELECT * from test.asyncmy where id = %s", (i + 1,))
        res = cur.fetchall()
        assert len(res) == 1
    return time.time() - t
