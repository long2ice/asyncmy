import time

import aiomysql
import MySQLdb
import pymysql

import asyncmy
from benchmark import COUNT, connection_kwargs


async def update_asyncmy():
    pool = await asyncmy.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            t = time.time()
            for i in range(COUNT):
                await cur.execute(
                    "update test.asyncmy set `string`=%s where `string` != %s limit 1",
                    (
                        "update",
                        "update",
                    ),
                )
    return time.time() - t


async def update_aiomysql():
    pool = await aiomysql.create_pool(**connection_kwargs)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            t = time.time()
            for i in range(COUNT):
                await cur.execute(
                    "update test.asyncmy set `string`=%s where `string` != %s limit 1",
                    (
                        "update",
                        "update",
                    ),
                )
            return time.time() - t


def update_mysqlclient():
    conn = MySQLdb.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(COUNT):
        cur.execute(
            "update test.asyncmy set `string`=%s where `string` != %s limit 1",
            (
                "update",
                "update",
            ),
        )
    return time.time() - t


def update_pymysql():
    conn = pymysql.connect(**connection_kwargs)
    cur = conn.cursor()
    t = time.time()
    for i in range(COUNT):
        cur.execute(
            "update test.asyncmy set `string`=%s where `string` != %s limit 1",
            (
                "update",
                "update",
            ),
        )
    return time.time() - t
