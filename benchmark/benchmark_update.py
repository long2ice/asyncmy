import time

import aiomysql
import MySQLdb
import pymysql

import asyncmy
from benchmark import COUNT, connection_kwargs
from benchmark.decorators import cleanup, fill_data


@cleanup
@fill_data
async def update_asyncmy():
    conn = await asyncmy.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(COUNT):
            await cur.execute(
                "update test.asyncmy set `string`=%s where `id` = %s",
                (
                    "update",
                    i + 1,
                ),
            )
        return time.time() - t


@cleanup
@fill_data
async def update_aiomysql():
    conn = await aiomysql.connect(**connection_kwargs)
    async with conn.cursor() as cur:
        t = time.time()
        for i in range(COUNT):
            await cur.execute(
                "update test.asyncmy set `string`=%s where `id` = %s",
                (
                    "update",
                    i + 1,
                ),
            )
        return time.time() - t


@cleanup
@fill_data
def update_mysqlclient():
    conn = MySQLdb.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(COUNT):
        cur.execute(
            "update test.asyncmy set `string`=%s where `id` = %s",
            (
                "update",
                i + 1,
            ),
        )
    return time.time() - t


@cleanup
@fill_data
def update_pymysql():
    conn = pymysql.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(COUNT):
        cur.execute(
            "update test.asyncmy set `string`=%s where `id` = %s",
            (
                "update",
                i + 1,
            ),
        )
    return time.time() - t
