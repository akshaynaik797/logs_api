from flask import Flask, request, jsonify, url_for
from flask_cors import CORS
import mysql.connector
from common import conf_conn_data, logs_conn_data, run_sms_scheduler

app = Flask(__name__)

cors = CORS(app)
run_sms_scheduler()
app.config['CORS_HEADERS'] = 'Content-Type'
app.config['referrer_url'] = None


@app.route("/")
def index():
    return url_for('index', _external=True)


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
                            'errorDescription', 'insurerID', 'fStatus', 'fLock'), dict()
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
            q = "INSERT INTO hospitalTLog (`PatientID_TreatmentID`,`Type_Ref`,`Type`,`status`,`HospitalID`,`cdate`,`person_name`,`smsTrigger`,`pushTrigger`,`lock`,`error`,`errorDescription`, insurerID, fStatus, fLock) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            cur.execute(q, record_data)
            con.commit()
    return jsonify('success')


@app.route('/get_hospitaltlog', methods=["POST"])
def get_hospitaltlog():
    field_list, datadict, records = ('PatientID_TreatmentID', 'Type_Ref', 'Type', 'status', 'HospitalID', 'cdate',
                                     'person_name', 'smsTrigger', 'insurerID', 'fStatus', 'fLock'
                                     , 'pushTrigger', 'lock', 'error', 'errorDescription'), dict(), []
    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        q = "select `PatientID_TreatmentID`,`Type_Ref`,`Type`,`status`,`HospitalID`,`cdate`,`person_name`," \
            "`smsTrigger`,`pushTrigger`,`lock`,`error`,`errorDescription` , 'insurerID', 'fStatus', 'fLock' " \
            "from hospitalTLog where smsTrigger='0' and `lock`='0';"
        cur.execute(q)
        r = cur.fetchall()
        for i in r:
            for j, k in zip(field_list, i):
                datadict[j] = k
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
