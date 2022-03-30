import asyncio
import os

import pytest_asyncio

import asyncmy
from asyncmy import connect
from asyncmy.cursors import DictCursor

connection_kwargs = dict(
    host="127.0.0.1",
    port=3306,
    user="root",
    password=os.getenv("MYSQL_PASS") or "123456",
    echo=True,
)


@pytest_asyncio.fixture(scope="session")
def event_loop():
    policy = asyncio.get_event_loop_policy()
    res = policy.new_event_loop()
    asyncio.set_event_loop(res)
    res._close = res.close
    res.close = lambda: None

    yield res

    res._close()


@pytest_asyncio.fixture(scope="session")
async def connection():
    conn = await connect(**connection_kwargs)
    yield conn
    await conn.ensure_closed()


@pytest_asyncio.fixture(scope="session", autouse=True)
async def initialize_tests(connection):
    async with connection.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("create database if not exists test")
        await cursor.execute(
            """CREATE TABLE  IF NOT EXISTS test.`asyncmy`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `decimal` decimal(10,2) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `datetime` datetime DEFAULT NULL,
  `time` time DEFAULT NULL,
  `float` float DEFAULT NULL,
  `string` varchar(200) DEFAULT NULL,
  `tinyint` tinyint DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `asyncmy_string_index` (`string`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"""
        )


@pytest_asyncio.fixture(scope="function", autouse=True)
async def truncate_table(connection):
    async with connection.cursor(cursor=DictCursor) as cursor:
        await cursor.execute("truncate table test.asyncmy")


@pytest_asyncio.fixture(scope="session")
async def pool():
    pool = await asyncmy.create_pool(**connection_kwargs)
    yield pool
    pool.close()
    await pool.wait_closed()
