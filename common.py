import random
from time import sleep
from datetime import datetime

import mysql.connector
from apscheduler.schedulers.background import BackgroundScheduler

from alerts_ import trigger

conf_conn_data = {'host': "iclaimdev.caq5osti8c47.ap-south-1.rds.amazonaws.com",
                  'user': "admin",
                  'password': "Welcome1!",
                  'database': 'configuration'}

logs_conn_data = {'host': "iclaimdev.caq5osti8c47.ap-south-1.rds.amazonaws.com",
                  'user': "admin",
                  'password': "Welcome1!",
                  'database': 'test_iclaimLogs'}


def get_db_info(hospital_id):
    field_list, records = ('srno', 'environment', 'hospitalID', 'host', 'dbName', 'port', 'userName', 'password',
                           'cDate'), dict()
    with mysql.connector.connect(**conf_conn_data) as con:
        cur = con.cursor()
        q = 'SELECT * FROM dbConfiguration where hospitalID=%s;'
        cur.execute(q, (hospital_id,))
        result = cur.fetchall()
    for i in result:
        datadict = dict()
        for key, value in zip(field_list, i):
            datadict[key] = value
        records[datadict['hospitalID']] = datadict


def update_data_sms(**kwargs):
    if 'Type_Ref' in kwargs:
        with mysql.connector.connect(**logs_conn_data) as con:
            cur = con.cursor()
            for key, value in kwargs.items():
                if key != 'Type_Ref':
                    q = "update hospitalTLog set %s='%s' where Type_Ref='%s'" % (key, value, kwargs['Type_Ref'])
                    cur.execute(q)
            con.commit()


def sms_scheduler():
    with open('logs/status.log', 'a') as fp:
        print(str(datetime.now()), 'no_records', sep=',', file=fp)
    field_list, records = ('PatientID_TreatmentID', 'Type_Ref', 'Type', 'status', 'HospitalID', 'cdate', 'person_name',
                            'smsTrigger', 'pushTrigger', 'lock', 'error',
                            'errorDescription', 'insurerID', 'fStatus', 'fLock'), dict()
    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        q = "SELECT PatientID_TreatmentID, Type_Ref, Type, status, HospitalID, cdate, person_name,smsTrigger, pushTrigger, `lock`, error, errorDescription, 'insurerID', 'fStatus', 'fLock' FROM hospitalTLog where smsTrigger='0' and `lock`='0' and error='0';"
        cur.execute(q)
        result = cur.fetchall()
    if len(result) > 0:
        for i in result:
            datadict = dict()
            for key, value in zip(field_list, i):
                datadict[key] = value
            records[datadict['Type_Ref']] = datadict
        for key, value in records.items():
            with mysql.connector.connect(**logs_conn_data) as con:
                cur = con.cursor()
                q = "update hospitalTLog set `lock`=1 where Type_Ref=%s"
                cur.execute(q, (key,))
                con.commit()
            with open('logs/status.log', 'a') as fp:
                print(str(datetime.now()), 'found record', value['Type_Ref'], value['HospitalID'], value['Type'], value['status'], sep=',', file=fp)
            trigger(value['Type_Ref'], value['HospitalID'], value['Type'], value['status'])
            with open('logs/status.log', 'a') as fp:
                print(str(datetime.now()), 'processed record', value['Type_Ref'], value['HospitalID'], value['Type'], value['status'], sep=',', file=fp)
            with mysql.connector.connect(**logs_conn_data) as con:
                cur = con.cursor()
                q = "update hospitalTLog set `lock`=0 where Type_Ref=%s"
                cur.execute(q, (key,))
                con.commit()


def run_sms_scheduler():
    sched = BackgroundScheduler(daemon=False)
    sched.add_job(sms_scheduler, 'interval', seconds=5, max_instances=1)
    sched.start()


def fun():
    with open('1.txt', 'a') as fp:
        print(str(random.randint(1000, 10000)), file=fp)



if __name__ == '__main__':
    sms_scheduler()
