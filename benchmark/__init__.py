import faker
import MySQLdb
import pymysql

connection_kwargs = dict(
    host="127.0.0.1", port=3306, user="root", password="123456", autocommit=True
)
conn_mysqlclient = MySQLdb.connect(**connection_kwargs)
conn_pymysql = pymysql.connect(**connection_kwargs)
COUNT = 50000
faker = faker.Faker()

data = [
    (
        1,
        faker.date_time().date(),
        faker.date_time(),
        1,
        faker.name(),
        1,
    )
    for _ in range(COUNT)
]
sql = """INSERT INTO test.asyncmy(`decimal`, `date`, `datetime`, `float`, `string`, `tinyint`) VALUES (%s,%s,%s,%s,%s,%s)"""
