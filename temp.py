import mysql.connector

from common import p_conn_data

insurer = 'vipul'
q = "select email_ids.email_ids from email_ids inner join IC_name on email_ids.ic = IC_name.IC and IC_name.IC_name=%s"
params = []
with mysql.connector.connect(**p_conn_data) as con:
    cur = con.cursor()
    cur.execute(q, (insurer,))
    result = cur.fetchall()
    email_list = [i[0] for i in result]
    q = "SELECT * FROM settlement_mails where sender in "
    q = q + '(' + ','.join(['%s' for i in email_list]) + ')'
    params.extend(email_list)
    cur.execute(q, params)
    result = cur.fetchall()
    pass


with mysql.connector.connect(**logs_conn_data) as con:
    cur = con.cursor()
    cur.execute(q, params)
    result = cur.fetchall()
    pass