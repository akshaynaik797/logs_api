from flask import Flask, request, jsonify, url_for
from flask_cors import CORS
import mysql.connector
import os
from common import conf_conn_data, logs_conn_data, run_sms_scheduler
from alerts_ import get_db_conf
app = Flask(__name__)

cors = CORS(app)

####for test purpose
run_sms_scheduler()
####

app.config['CORS_HEADERS'] = 'Content-Type'
app.config['referrer_url'] = None


@app.route("/")
def index():
    return url_for('index', _external=True)


@app.route("/get_api_link", methods=["POST"])
def get_api_link():
    data = request.form.to_dict()
    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        query = "SELECT apiLink FROM apisConfig where hospitalID=%s and processName=%s limit 1;"
        cur.execute(query, (data['hospitalID'], data['processName']))
        result = cur.fetchone()
        if result is not None:
            return result[0]
    return jsonify(None)

@app.route("/update_downtime", methods=["POST"])
def update_downtime():
    fields = ('id', 'start_time', 'fail_time', 'serial_no')
    data = request.form.to_dict()
    if 'start_time' not in data and 'fail_time' not in data:
        return jsonify('pass start or fail time')
    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        q = 'SELECT * FROM update_downtime order by id desc limit 1;'
        cur.execute(q)
        record = cur.fetchone()
        tempdict = {}
        if record is not None:
            for i, j in zip(fields, record):
                tempdict[i] = j
            if tempdict['fail_time'] is not None and tempdict['start_time'] is None:
                if 'start_time' in data:
                    q = "update update_downtime set start_time=%s where id=%s"
                    cur.execute(q, (data['start_time'], tempdict['id']))
            if tempdict['start_time'] is not None:
                if 'fail_time' in data:
                    q = "insert into update_downtime (fail_time) values (%s)"
                    cur.execute(q, (data['fail_time'],))
        con.commit()
    return jsonify('done')

@app.route("/get_hospital_db_info", methods=["POST"])
def get_hospital_db_info():
    field_list, records = ('srno', 'environment', 'hospitalID', 'host', 'dbName', 'port', 'userName', 'password',
                           'cDate'), dict()
    data = request.form.to_dict()
    with mysql.connector.connect(**conf_conn_data) as con:
        cur = con.cursor()
        if 'hospitalID' in data:
            q = 'SELECT * FROM dbConfiguration where hospitalID=%s;'
            cur.execute(q, (data['hospitalID'],))
        else:
            q = 'SELECT * FROM dbConfiguration;'
            cur.execute(q)
        result = cur.fetchall()
    for i in result:
        datadict = dict()
        for key, value in zip(field_list, i):
            datadict[key] = value
        records[datadict['hospitalID']] = datadict
    return jsonify(records)


@app.route('/modify_hospitaltlog', methods=["POST"])
def modify_hospitaltlog():
    field_list, datadict = ('PatientID_TreatmentID', 'Type_Ref', 'Type', 'status', 'HospitalID', 'cdate', 'person_name',
                            'smsTrigger', 'pushTrigger', 'lock', 'error',
                            'errorDescription', 'insurerID', 'fStatus', 'fLock', 'transactionID'), dict()
    for i in field_list:
        datadict[i] = ' '
    data = request.form.to_dict()
    if 'Type_Ref' not in data:
        return jsonify('insert refno')
    else:
        for i in datadict:
            if i in data:
                datadict[i] = data[i]
        datadict['smsTrigger'], datadict['lock'], datadict['error'] = '0', '0', '0'
        record_data = []
        for i in field_list:
            record_data.append(datadict[i])
        #logic for diif hospitals
        with mysql.connector.connect(**logs_conn_data) as con:
            cur = con.cursor()
            q = "INSERT INTO hospitalTLog (`PatientID_TreatmentID`,`Type_Ref`,`Type`,`status`,`HospitalID`,`cdate`,`person_name`,`smsTrigger`,`pushTrigger`,`lock`,`error`,`errorDescription`, insurerID, fStatus, fLock, transactionID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            cur.execute(q, record_data)
            con.commit()
    return jsonify('success')


@app.route('/update_hospitaltlog', methods=["POST"])
def update_hospitaltlog():
    data = request.form.to_dict()
    if 'Type_Ref' not in data or 'Type' not in data or 'status' not in data:
        return jsonify('insert refno, Type, status')
    if 'fStatus' in data and 'fLock' in data:
        q = "update hospitalTLog set fStatus=%s ,fLock = %s where Type_Ref=%s and Type=%s and status=%s"
        record = (data['fStatus'], data['fLock'], data['Type_Ref'], data['Type'], data['status'],)
    elif 'fStatus' in data:
        q = "update hospitalTLog set fStatus=%s where Type_Ref=%s and Type=%s and status=%s"
        record = (data['fStatus'], data['Type_Ref'], data['Type'], data['status'],)
    elif 'fLock' in data:
        q = "update hospitalTLog set fLock=%s where Type_Ref=%s and Type=%s and status=%s"
        record = (data['fLock'], data['Type_Ref'], data['Type'], data['status'],)
    else:
        return jsonify('insert flock or fstatus')
    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, record)
        con.commit()
    return jsonify('success')

@app.route('/get_hospitaltlog', methods=["POST"])
def get_hospitaltlog():
    data = request.form.to_dict()
    field_list, datadict, records = ('srno', 'transactionID', 'PatientID_TreatmentID', 'Type_Ref', 'Type',
                                     'status', 'HospitalID', 'cdate',
                                     'person_name', 'smsTrigger', 'pushTrigger', 'insurerID', 'fStatus', 'fLock',
                                     'lock', 'error', 'errorDescription'), dict(), []
    preauth_field_list = ("preauthNo", "MemberId", "p_sname", "admission_date", "dischargedate", "flag","CurrentStatus", "cdate", "up_date", "hospital_name", "p_policy")
    q = "select `srno`, `transactionID`,`PatientID_TreatmentID`,`Type_Ref`,`Type`,`status`,`HospitalID`,`cdate`,`person_name`,`smsTrigger`,`pushTrigger`,`insurerID`,`fStatus`,`fLock`,`lock`,`error`,`errorDescription` from hospitalTLog where transactionID is not null and transactionID != '' and str_to_date(cdate,'%d/%m/%Y')>=str_to_date('12/02/2021','%d/%m/%Y') and srno is not null "
    params = []
    #add preauth params p_sname CurrentStatus
    if 'fromdate' in data and 'todate' in data:
        q = q + ' and cdate > %s and cdate < %s'
        params = params + [data['fromdate'], data['todate']]
    if 'hospitalid' in data:
        q = q + ' and HospitalID=%s'
        params = params + [data['hospitalid']]
    if 'status' in data:
        q = q + ' and status=%s'
        params = params + [data['status']]
    if 'refNo' in data:
        q = q + ' and Type_Ref=%s'
        params = params + [data['refNo']]
    if 'insurerID' in data:
        q = q + ' and insurerID=%s'
        params = params + [data['insurerID']]
    q = q + ' order by cdate desc'
    params = tuple(params)
    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, params)
        r = cur.fetchall()
        for i in r:
            datadict = dict()
            for j, k in zip(field_list, i):
                datadict[j] = k
            q = "select descr from form_status where scode=%s limit 1"
            cur.execute(q, (datadict['status'],))
            result = cur.fetchone()
            if result is not None:
                datadict['description'] = result[0]
            cur.execute("select name from insurer_tpa_master where TPAInsurerID=%s limit 1", (datadict['insurerID'],))
            r1 = cur.fetchone()
            if r1 is not None:
                datadict['insurer_tpa'] = r1[0]
            q = "select preauthNo, MemberId, p_sname, admission_date, dischargedate, flag, " \
                "CurrentStatus, cdate, up_date, hospital_name, p_policy from preauth where srno is not null "
            params = []
            if 'p_sname' in data:
                q = q + ' and p_sname like %s'
                params = params + ['%' + data['p_sname'] + '%']
            if 'CurrentStatus' in data:
                q = q + ' and CurrentStatus=%s'
                params = params + [data['CurrentStatus']]
            q = q + ' and refno=%s limit 1'
            params = params + [datadict['Type_Ref']]
            params = tuple(params)
            dbconf = get_db_conf(hosp=datadict['HospitalID'])
            with mysql.connector.connect(**dbconf) as con:
                cur1 = con.cursor()
                cur1.execute(q, params)
                result = cur1.fetchone()
                if result is not None:
                    for key, value in zip(preauth_field_list, result):
                        datadict[key] = value
                    records.append(datadict)
    return jsonify(records)



@app.route('/modify_apisLog', methods=["POST"])
def modify_apisLog():
    field_list, datadict = ('hospitalID','referenceNo','method','title','purpose','status',
                            'request','response','error','runtime','ipAddress'), dict()
    for i in field_list:
        datadict[i] = ' '
    data = request.form.to_dict()
    if 'referenceNo' not in data:
        return jsonify('insert refno')
    else:
        for i in datadict:
            if i in data:
                datadict[i] = data[i]
        record_data = []
        for i in field_list:
            record_data.append(datadict[i])
        #logic for diif hospitals
        with mysql.connector.connect(**logs_conn_data) as con:
            cur = con.cursor()
            q = "INSERT INTO apisLog (`hospitalID`,`referenceNo`,`method`,`title`,`purpose`,`status`,`request`,`response`,`error`,`runtime`,`ipAddress`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            cur.execute(q, record_data)
            con.commit()
    return jsonify('success')




@app.route('/get_apisLog', methods=["POST"])
def get_apisLog():
    data = request.form.to_dict()
    field_list, datadict, records = ('srno', 'dateTime','hospitalID','referenceNo','method','title','purpose','status',
                            'request','response','error','runtime','ipAddress'), dict(), []
    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        q = "select `srno`,`dateTime`,`hospitalID`,`referenceNo`,`method`,`title`,`purpose`,`status`,`request`,`response`,`error`,`runtime`,`ipAddress` from apisLog where dateTime between %s and %s;"
        cur.execute(q, (data['from'], data['to']))
        r = cur.fetchall()
        for i in r:
            datadict = dict()
            for j, k in zip(field_list, i):
                datadict[j] = k
            records.append(datadict)
    return jsonify(records)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=9980)
