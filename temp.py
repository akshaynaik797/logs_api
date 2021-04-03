import mysql.connector

from common import logs_conn_data

q = "SELECT * FROM sentmaillogs where sno is not null limit 10 "
params = []


with mysql.connector.connect(**logs_conn_data) as con:
    cur = con.cursor()
    cur.execute(q, params)
    result = cur.fetchall()
    pass