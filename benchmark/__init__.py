import MySQLdb
import pymysql

connection_kwargs = dict(
    host="127.0.0.1", port=3306, user="root", password="123456", autocommit=True
)
conn_mysqlclient = MySQLdb.connect(**connection_kwargs)
conn_pymysql = pymysql.connect(**connection_kwargs)
