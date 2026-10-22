import os

import pytest_asyncio
from asyncmy.cursors import DictCursor

import asyncmy
from asyncmy import connect

connection_kwargs = dict(
    host=os.getenv("MYSQL_HOST") or "127.0.0.1",
    port=int(os.getenv("MYSQL_PORT") or 3306),
    user=os.getenv("MYSQL_USER") or "root",
    password=os.getenv("MYSQL_PASS") or "123456",
    echo=True,
)


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
