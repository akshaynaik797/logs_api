import mysql.connector
from datetime import datetime
conn_data = {'host': "iclaimdev.caq5osti8c47.ap-south-1.rds.amazonaws.com",
                  'user': "admin",
                  'password': "Welcome1!",
                  'database': 'python_rep'}

def check_date():
    from datetime import datetime
    fields = ('table_id','table_name','active','flag','id','subject','date','hospital')
    records = []
    record_dict = {}
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q = 'select * from mail_storage_tables'
        cur.execute(q)
        records = cur.fetchall()
    for i in records:
        temp = {}
        for key, value in zip(fields, i):
            temp[key] = value
        record_dict[temp['table_name']] = temp
    with mysql.connector.connect(**conn_data) as con:
        for i in record_dict:
            table_name = record_dict[i]['table_name']
            cur = con.cursor()
            q = f"select id, subject, date from {table_name} order by sno desc limit 2"
            cur.execute(q)
            records = cur.fetchall()
            if len(records) == 2:
                mid, subject, date = records[0]
                date1 = datetime.strptime(records[0][2], '%d/%m/%Y %H:%M:%S')
                date2 = datetime.strptime(records[1][2], '%d/%m/%Y %H:%M:%S')
                flag = ''
                if date1 >= date2:
                    flag = 'VALID'
                else:
                    flag = 'INVALID'

                q = f"update mail_storage_tables set flag=%s, id=%s, subject=%s, date=%s where table_name=%s"
                cur.execute(q, (flag, mid, subject, date, table_name))
        con.commit()


if __name__ == '__main__':
    check_date()