import random
import os
import re
from time import sleep
from datetime import datetime

import mysql.connector
from apscheduler.schedulers.background import BackgroundScheduler
import openpyxl
import xlrd

from alerts_ import trigger
from make_log import log_exceptions

conf_conn_data = {'host': "database-iclaim.caq5osti8c47.ap-south-1.rds.amazonaws.com",
                  'user': "admin",
                  'password': "Welcome1!",
                  'database': 'configuration'}

logs_conn_data = {'host': "database-iclaim.caq5osti8c47.ap-south-1.rds.amazonaws.com",
                  'user': "admin",
                  'password': "Welcome1!",
                  'database': 'portals'}

p_conn_data = {'host': "database-iclaim.caq5osti8c47.ap-south-1.rds.amazonaws.com",
                  'user': "admin",
                  'password': "Welcome1!",
                  'database': 'python'}

def get_data(excel_file, **kwargs):
    _, ext = os.path.splitext(excel_file)
    data = []
    if ext == '.xlsx':
        wb_obj = openpyxl.load_workbook(excel_file)
        sheet_obj = wb_obj.active
        table = []
        for cnt, i in enumerate(sheet_obj.rows):
            row = []
            for cell in i:
                row.append(str(cell.value))
            table.append(row)
        fields = table[0]
        for row in table[1:]:
            tmp = {}
            for k, v in zip(fields, row):
                tmp[k] = v
            data.append(tmp)

    if ext == '.xls':
        wb_obj = xlrd.open_workbook(excel_file)
        sheet_obj = wb_obj.sheet_by_index(0)
        table = []
        for row in range(sheet_obj.nrows):
            tmp = []
            for cell in sheet_obj.row(row):
                tmp.append(str(cell.value))
            table.append(tmp)
        fields = table[0]
        for row in table[1:]:
            tmp = {}
            for k, v in zip(fields, row):
                tmp[k] = v
            data.append(tmp)
    return data


def get_mappings(hospital_id):
    q, map_dict = "select  vFields, hFields from settlementDuesMap where HospitalID=%s", {}
    with mysql.connector.connect(**p_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, [hospital_id])
        r = cur.fetchall()
        for k, v in r:
            map_dict[k] = v
    return map_dict

def get_db_conf(**kwargs):
    fields = ('host', 'database', 'port', 'user', 'password')
    if 'env' not in kwargs:
        kwargs['env'] = 'live'
    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        q = 'SELECT host, dbName, port, userName, password FROM dbConfiguration where hospitalID=%s and environment=%s limit 1;'
        cur.execute(q, (kwargs['hosp'], kwargs['env']))
        result = cur.fetchone()
        if result is not None:
            conf_data = dict()
            for key, value in zip(fields, result):
                conf_data[key] = value
            return conf_data

def insert_in_settlementdueslist(excel_file, hospital_id):
    fields = ['claimID', 'BillNo', 'BillDate', 'CompanyType', 'CompanyName', 'PatientName',
              'MemberID', 'BalanceAmt', 'Flag']
    q = 'insert into settlementDuesList (' + ', '.join(['HospitalID'] + fields) + ')'
    q = q + ' values (' + ('%s, ' * q.count(',')) + '%s) ON DUPLICATE KEY UPDATE HospitalID=%s, BillNo=%s, ' \
                                                    'BillDate=%s, CompanyType=%s, CompanyName=%s, PatientName=%s, ' \
                                                    'MemberID=%s, BalanceAmt=%s, Flag=%s;'
    data = get_data(excel_file)
    map_dict = get_mappings(hospital_id)
    for row in data:
        try:
            tmp = {}
            for i in map_dict:
                if map_dict[i] in row:
                    tmp[i] = row[map_dict[i]].strip()
            for i in fields:
                if i not in tmp:
                    tmp[i] = ''
            tmp['Flag'] = 'N'
            params = [hospital_id] + [tmp[i] for i in fields] + [hospital_id] + [tmp[i] for i in fields[1:]]
            with mysql.connector.connect(**p_conn_data) as con:
                cur = con.cursor()
                cur.execute(q, params)
                con.commit()
        except:
            log_exceptions(row=row)
    return True

def comparesettlementdata_lib(hospital_id, **kwargs):
    r1 = []
    total, found = 0, 0
    a = ["HospitalID", "BillNo", "BillDate", "CompanyType", "CompanyName", "PatientName", "MemberID",
              "claimID", "BalanceAmt", "Flag"]
    b = ['NetPayable', 'SettledAmount', 'TDS', 'UTRNo', 'transferDate']

    c = ["statDesc", "BankAMount", "Bank", "BankDate"]

    q1 = "SELECT HospitalID, BillNo,BillDate, CompanyType, CompanyName,PatientName, MemberID, claimID, " \
         "BalanceAmt, Flag from settlementDuesList where HospitalID=%s and Flag='N'"
    params = [hospital_id]

    # q1 = "SELECT HospitalID, BillNo,BillDate, CompanyType, CompanyName,PatientName, MemberID, claimID, " \
    #      "BalanceAmt, Flag from settlementDuesList where claimID='RC-HS21-12373571'"
    # params = []

    q2 = "SELECT NetPayable, SettledAmount, TDS, UTRNo, Transactiondate from stgSettlement " \
         "where ClaimNo=%s or ClaimNo=%s or ClaimNo=%s limit 1"

    q2_1 = "SELECT NetPayable, SettledAmount, TDS, UTRNo, Transactiondate from stgSettlement " \
         "where MemberID=%s or MemberID=%s or MemberID=%s limit 1"



    q4 = "SELECT Name_ECS_No, amount, banknm, date from settlementutrupdate where UTR_No=%s limit 1"

    with mysql.connector.connect(**p_conn_data) as con:
        cur = con.cursor()
        cur.execute(q1, params)
        r1 = cur.fetchall()
    for row in r1:
        try:
            total += 1
            tmp1 = {}
            for k, v in zip(a, row):
                tmp1[k] = v
            claimid, memberid, r2 = tmp1['claimID'].strip(), tmp1['MemberID'].strip(), None
            # strings = claimid, re.sub(r"^[^0-9]+", '', claimid), re.sub(r"^0+", '', claimid)
            # strings1 = memberid, re.sub(r"^[^0-9]+", '', memberid), re.sub(r"^0+", '', memberid)
            with mysql.connector.connect(**p_conn_data) as con:
                cur = con.cursor()
                cur.execute(q2, [claimid, re.sub(r"^[^0-9]+", '', claimid), re.sub(r"[-/]0$", '', claimid)])
                r2 = cur.fetchone()
                if r2 is None:
                    cur.execute(q2_1, [memberid, re.sub(r"^[^0-9]+", '', memberid), re.sub(r"[-/]0$", '', memberid)])
                    r2 = cur.fetchone()
                if r2:
                    for k, v in zip(b, r2):
                        tmp1[k] = v
                    tmp1["Flag"] = 'P'
                else:
                    #compare pname from both table(partila match)  and  diff between settlementDuesList.balance amt  stgsett.netpayable is 10%
                    # add flag R in both tables
                    # stgsettment.pname  -> settlementDuesList.PatientName
                    # if partial and full match
                    # if  diff between settlementDuesList.balance amt  stgsett.netpayable is 10% pick closest
                    r3 = None
                    word_list = re.split(r" +", re.sub(r"[^\w ]", "", tmp1['PatientName']).strip())
                    q3 = "SELECT NetPayable, SettledAmount, TDS, UTRNo, Transactiondate from stgSettlement where "
                    q3 += " PatientName=%s or" * len(word_list)
                    q3 = q3.strip('or')
                    with mysql.connector.connect(**p_conn_data) as con:
                        cur = con.cursor()
                        cur.execute(q3, word_list)
                        r3 = cur.fetchall()
                    bal_amt = float(tmp1['BalanceAmt'])
                    tmp_dict = {}
                    for row_ in r3:
                        tmp = {}
                        for k, v in zip(b, row_):
                            tmp[k] = v
                        net_pay = float(tmp['NetPayable'])
                        percent = min(bal_amt, net_pay) / max(net_pay, bal_amt)
                        tmp_dict[percent] = tmp
                    if len(tmp_dict) > 0 and max(tmp_dict.keys()) > 0.9:
                        for k, v in tmp_dict[max(tmp_dict.keys())].items():
                            tmp1[k] = v
                        tmp1["Flag"] = 'R'
            # Pick orange col data from where stgSettlement--> utrno = settlementutrupdate.utrno
            if 'UTRNo' in tmp1:
                with mysql.connector.connect(**get_db_conf(hosp=hospital_id)) as con:
                    cur = con.cursor()
                    cur.execute(q4, [tmp1['UTRNo']])
                    if r4 := cur.fetchone():
                        for k, v in zip(c, r4):
                            tmp1[k] = v
            #function to insert record in settlementCommon
            if tmp1['Flag'] != 'N':
                insert_in_table(tmp1, "settlementCommon")
                q5 = "update settlementDuesList set Flag=%s where claimID=%s"
                with mysql.connector.connect(**p_conn_data) as con:
                    cur = con.cursor()
                    cur.execute(q5, [tmp1['Flag'], tmp1['claimID']])
                    con.commit()
                found += 1
        except:
            log_exceptions(row=row)
    print("processed records: ", total, " found: ", found)

def comparebybank_lib(hospital_id, **kwargs):
    r1 = []
    total, found = 0, 0
    a = ["SrNo", "UTRNo", "statDesc", "BankAMount", "Bank", "BankDate"]
    b = ["ClaimID", "PatientName", "MemberID", "NetPayable", "SettledAmount", "tDS", "transferDate"]
    q = "select srno, UTR_No, Name_ECS_No, amount, banknm, date from settlementutrupdate"
    q2 = "SELECT ClaimNo, PatientName, MemberID, NetPayable, SettledAmount, TDS, Transactiondate from stgSettlement " \
         "where UTRNo=%s or UTRNo=%s or UTRNo=%s limit 1"
    with mysql.connector.connect(**get_db_conf(hosp=hospital_id)) as con:
        cur = con.cursor()
        cur.execute(q)
        r1 = cur.fetchall()
    for row in r1:
        try:
            total += 1
            tmp = {}
            for k, v in zip(a, row):
                tmp[k] = v
            tmp['HospitalID'] = hospital_id
            utrno = tmp['UTRNo']
            # strings1 = utrno, re.sub(r"^[^0-9]+", '', utrno), re.sub(r"^0+", '', utrno)
            with mysql.connector.connect(**p_conn_data) as con:
                cur1 = con.cursor()
                cur1.execute(q2, [utrno, re.sub(r"^[^0-9]+", '', utrno), re.sub(r"[-/]0$", '', utrno)])
                if r2 := cur1.fetchone():
                    for k, v in zip(b, r2):
                        tmp[k] = v
            # function to insert record in settlementByBank
            if r2:
                insert_in_table(tmp, "settlementByBank")
                found += 1
        except:
            log_exceptions(row=row)
    print("processed records: ", total, " found: ", found)

def insert_in_table(tmp_dict, table):
    # a = {'col1': 'val1', 'col2': 'val2'}
    # tmp = [i for i in a.keys()]
    # q = "insert into table (" + ', '.join(tmp) + ") values (" + ('%s, ' * (len(tmp) - 1)) + "%s)"
    # params = [a[i] for i in tmp]
    tmp = [i for i in tmp_dict.keys()]
    q = f"insert into {table} (" + ', '.join(tmp) + ") values (" + ('%s, ' * (len(tmp) - 1)) + "%s)"
    params = [tmp_dict[i] for i in tmp]
    with mysql.connector.connect(**p_conn_data) as con:
        cur = con.cursor()
        try:
            cur.execute(q, params)
        except:
            log_exceptions(q=q, params=params)
        finally:
            con.commit()

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
        q = "SELECT PatientID_TreatmentID, Type_Ref, Type, status, HospitalID, cdate, person_name,smsTrigger, pushTrigger, `lock`, error, errorDescription, insurerID, fStatus, fLock FROM hospitalTLog where smsTrigger='0' and `lock`='0' and error='0';"
        cur.execute(q)
        result = cur.fetchall()
    if len(result) > 0:
        for j, i in enumerate(result):
            datadict = dict()
            for key, value in zip(field_list, i):
                datadict[key] = value
            records[j] = datadict
        for key, value in records.items():
            with mysql.connector.connect(**logs_conn_data) as con:
                cur = con.cursor()
                q = "update hospitalTLog set `lock`= 1 where Type_Ref=%s"
                cur.execute(q, (value['Type_Ref'],))
                ####for test purpose
                con.commit()
                ####
                q = "select descr from form_status where scode=%s limit 1"
                cur.execute(q, (value['status'],))
                result = cur.fetchone()
                if result is not None:
                    value['status'] = result[0]
                else:
                    continue
            with open('logs/status.log', 'a') as fp:
                print(str(datetime.now()), 'found record', value['Type_Ref'], value['HospitalID'], value['Type'], value['status'], sep=',', file=fp)
            trigger(value['Type_Ref'], value['HospitalID'], value['Type'], value['status'])
            with open('logs/status.log', 'a') as fp:
                print(str(datetime.now()), 'processed record', value['Type_Ref'], value['HospitalID'], value['Type'], value['status'], sep=',', file=fp)
            with mysql.connector.connect(**logs_conn_data) as con:
                cur = con.cursor()
                q = "update hospitalTLog set `lock`=0 where Type_Ref=%s"
                cur.execute(q, (value['Type_Ref'],))
                ####for test purpose
                con.commit()
                ####


def run_sms_scheduler():
    sched = BackgroundScheduler(daemon=False)
    sched.add_job(sms_scheduler, 'interval', seconds=30, max_instances=1)
    sched.start()


def fun():
    with open('1.txt', 'a') as fp:
        print(str(random.randint(1000, 10000)), file=fp)


if __name__ == '__main__':
    sms_scheduler()
