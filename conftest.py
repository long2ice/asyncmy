import asyncio
import os

import pytest

import asyncmy
from asyncmy import connect
from asyncmy.cursors import DictCursor

connection_kwargs = dict(
    host="127.0.0.1",
    port=3306,
    user="root",
    password=os.getenv("MYSQL_PASS") or "123456",
)


@pytest.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    res = policy.new_event_loop()
    asyncio.set_event_loop(res)
    res._close = res.close
    res.close = lambda: None

    yield res

    res._close()


@pytest.fixture(scope="session")
async def connection():
    conn = await connect(**connection_kwargs)
    yield conn
    await conn.close()


@pytest.fixture(scope="session", autouse=True)
async def initialize_tests(connection):
    async with connection.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("create database if not exists test")
        await cursor.execute(
            """CREATE TABLE if not exists test.asyncmy
    (
        `id`       int primary key auto_increment,
        `decimal`  decimal(10, 2),
        `date`     date,
        `datetime` datetime,
        `float`    float,
        `string`   varchar(200),
        `tinyint`  tinyint
    )"""
        )


@pytest.fixture(scope="function", autouse=True)
async def truncate_table(connection):
    async with connection.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("truncate table test.asyncmy")


@pytest.fixture(scope="session")
async def pool():
    pool = await asyncmy.create_pool(**connection_kwargs)
    yield pool
    pool.close()
    await pool.wait_closed()
