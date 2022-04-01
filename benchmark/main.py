import asyncio
from rich.pretty import pprint
from benchmark.benchmark_delete import benchmark_delete
from benchmark.benchmark_insert import benchmark_insert, conn_mysqlclient
from benchmark.benchmark_select import benchmark_select
from benchmark.benchmark_update import benchmark_update

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
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"""
    )
    cur.execute("truncate table test.asyncmy")

    ret = {
        "select": benchmark_select(),
        "insert": benchmark_insert(),
        "update": benchmark_update(),
        "delete": benchmark_delete(),
    }
    pprint(ret)
