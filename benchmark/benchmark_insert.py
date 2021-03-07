import asyncio
import time

import aiomysql
import faker

import asyncmy
from benchmark import conn_mysqlclient, conn_pymysql, connection_kwargs

faker = faker.Faker()
count = 100000
data = [
    (
        1,
        faker.date_time().date(),
        faker.date_time(),
        1,
        faker.name(),
        1,
    )
    for _ in range(count)
]
sql = """INSERT INTO test.asyncmy(`decimal`, `date`, `datetime`, `float`, `string`, `tinyint`) VALUES (%s,%s,%s,%s,%s,%s)"""


async def insert_asyncmy():
    pool = await asyncmy.create_pool(**connection_kwargs)
    t = time.time()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            ret = await cur.executemany(sql, data)
            assert ret == count
            print("asyncmy:", time.time() - t)


async def insert_aiomysql():
    pool = await aiomysql.create_pool(**connection_kwargs)
    t = time.time()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            ret = await cur.executemany(sql, data)
            assert ret == count
            print("aiomysql:", time.time() - t)


def insert_mysqlclient():
    cur = conn_mysqlclient.cursor()
    t = time.time()
    ret = cur.executemany(sql, data)
    assert ret == count
    print("mysqlclient:", time.time() - t)


def insert_pymysql():
    cur = conn_pymysql.cursor()
    t = time.time()
    ret = cur.executemany(sql, data)
    assert ret == count
    print("pymysql:", time.time() - t)


if __name__ == "__main__":
    """
    mysqlclient: 2.502872943878174
    asyncmy: 1.5797967910766602
    pymysql: 1.9640929698944092
    aiomysql: 1.7420449256896973
    """
    import uvloop

    uvloop.install()
    loop = asyncio.get_event_loop()

    cur = conn_mysqlclient.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS test.`asyncmy` (
  `id` int NOT NULL AUTO_INCREMENT,
  `decimal` decimal(10,2) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `datetime` datetime DEFAULT NULL,
  `float` float DEFAULT NULL,
  `string` varchar(200) DEFAULT NULL,
  `tinyint` tinyint DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `asyncmy_string_index` (`string`)
) ENGINE=InnoDB AUTO_INCREMENT=400001 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""
    )
    cur.execute("truncate table test.asyncmy")

    insert_mysqlclient()
    loop.run_until_complete(insert_asyncmy())
    insert_pymysql()
    loop.run_until_complete(insert_aiomysql())
