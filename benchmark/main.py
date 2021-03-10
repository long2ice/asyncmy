import asyncio

import uvloop
from rich.pretty import pprint

from benchmark import conn_mysqlclient
from benchmark.benchmark_delete import (
    delete_aiomysql,
    delete_asyncmy,
    delete_mysqlclient,
    delete_pymysql,
)
from benchmark.benchmark_insert import (
    insert_aiomysql,
    insert_asyncmy,
    insert_mysqlclient,
    insert_pymysql,
)
from benchmark.benchmark_select import (
    select_aiomysql,
    select_asyncmy,
    select_mysqlclient,
    select_pymysql,
)
from benchmark.benchmark_update import (
    update_aiomysql,
    update_asyncmy,
    update_mysqlclient,
    update_pymysql,
)

uvloop.install()
loop = asyncio.get_event_loop()

if __name__ == "__main__":
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

    insert_mysqlclient_ret = insert_mysqlclient()
    insert_asyncmy_ret = loop.run_until_complete(insert_asyncmy())
    insert_pymysql_ret = insert_pymysql()
    insert_aiomysql_ret = loop.run_until_complete(insert_aiomysql())

    select_mysqlclient_ret = select_mysqlclient()
    select_asyncmy_ret = loop.run_until_complete(select_asyncmy())
    select_pymysql_ret = select_pymysql()
    select_aiomysql_ret = loop.run_until_complete(select_aiomysql())

    update_mysqlclient_ret = update_mysqlclient()
    update_asyncmy_ret = loop.run_until_complete(update_asyncmy())
    update_pymysql_ret = update_pymysql()
    update_aiomysql_ret = loop.run_until_complete(update_aiomysql())

    delete_mysqlclient_ret = delete_mysqlclient()
    delete_asyncmy_ret = loop.run_until_complete(delete_asyncmy())
    delete_pymysql_ret = delete_pymysql()
    delete_aiomysql_ret = loop.run_until_complete(delete_aiomysql())

    ret = {
        "select": sorted(
            {
                "mysqlclient": select_mysqlclient_ret,
                "asyncmy": select_asyncmy_ret,
                "pymysql": select_pymysql_ret,
                "aiomysql": select_aiomysql_ret,
            }.items(),
            key=lambda x: x[1],
        ),
        "insert": sorted(
            {
                "mysqlclient": insert_mysqlclient_ret,
                "asyncmy": insert_asyncmy_ret,
                "pymysql": insert_pymysql_ret,
                "aiomysql": insert_aiomysql_ret,
            }.items(),
            key=lambda x: x[1],
        ),
        "update": sorted(
            {
                "mysqlclient": update_mysqlclient_ret,
                "asyncmy": update_asyncmy_ret,
                "pymysql": update_pymysql_ret,
                "aiomysql": update_aiomysql_ret,
            }.items(),
            key=lambda x: x[1],
        ),
        "delete": sorted(
            {
                "mysqlclient": delete_mysqlclient_ret,
                "asyncmy": delete_asyncmy_ret,
                "pymysql": delete_pymysql_ret,
                "aiomysql": delete_aiomysql_ret,
            }.items(),
            key=lambda x: x[1],
        ),
    }
    pprint(ret)
