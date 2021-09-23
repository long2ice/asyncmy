import MySQLdb
import pymysql
import uvloop

uvloop.install()

connection_kwargs = dict(
    host="127.0.0.1", port=3306, user="root", password="123456", autocommit=True
)
conn_mysqlclient = MySQLdb.connect(**connection_kwargs)
conn_pymysql = pymysql.connect(**connection_kwargs)
COUNT = 50000

data = [
    (
        1,
        "2021-01-01",
        "2020-07-16 22:49:54",
        1,
        "asyncmy",
        1,
    )
    for _ in range(COUNT)
]
sql = """INSERT INTO test.asyncmy(`decimal`, `date`, `datetime`, `float`, `string`, `tinyint`) VALUES (%s,%s,%s,%s,%s,%s)"""
