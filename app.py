import requests
from flask import Flask, request, jsonify, url_for, send_from_directory
from flask_cors import CORS
import mysql.connector
import os

from werkzeug.utils import secure_filename

from common import conf_conn_data, logs_conn_data, run_sms_scheduler, p_conn_data, insert_in_settlementdueslist, \
    comparesettlementdata_lib
from alerts_ import get_db_conf
from excel_api import allowed_file, main

app = Flask(__name__)

cors = CORS(app)

####for test purpose
# run_sms_scheduler()
####

dir_name = 'excel_files'
if not os.path.exists(dir_name):
    os.mkdir(dir_name)

app.config['CORS_HEADERS'] = 'Content-Type'
app.config['referrer_url'] = None

@app.route("/", methods=["POST", "GET"])
def index():
    return "this is logs api"

@app.route("/api/downloadfile")
def get_file():
    """Download a file."""
    if request.args['filename'] != None:
        filepath = request.args['filename']
        print("path=", filepath)
        # log_api_data('filepath', filepath)
        # filepath1=r"C:\Users\91798\Desktop\trial_shikha-master2\hdfc\attachments_pdf_denial\PreAuthDenialLe_RC-HS19-10809032_1_202_20200129142830250_19897.pdf"
        filepath = filepath.replace("\\", "/")
        mylist = filepath.split('/')
        filename = mylist[-1]
        index = 0
        dirname = ''
        for x in mylist:
            index = index + 1
            if index != len(mylist):
                dirname = dirname + x + '/'
        # return send_from_directory(r"C:\Users\91798\Desktop\download\templates", filename='ASHISHKUMAR_IT.pdf', as_attachment=True)
        return send_from_directory(dirname, filename=filename, as_attachment=True, mimetype='application/pdf')

@app.route("/uploadduelist", methods=["POST"])
def uploadduelist():
    data = request.form.to_dict()
    if 'file' not in request.files:
        return jsonify("upload file")
    file = request.files['file']
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        dst = os.path.join(dir_name, filename)
        file.save(dst)
        if insert_in_settlementdueslist(dst, data['HospitalID']):
            return jsonify("file uploaded")
    return jsonify("upload excel file")


@app.route("/comparesettlementdata", methods=["POST"])
def comparesettlementdata():
    data = request.form.to_dict()
    hospital_id = data['HospitalID']
    comparesettlementdata_lib(hospital_id)


@app.route("/comparebybank", methods=["POST"])
def comparebybank():
    data = request.form.to_dict()
    hospital_id = data['HospitalID']
    comparesettlementdata_lib(hospital_id)


@app.route("/getupdationdetaillogcopy", methods=["POST"])
def get_updation_detail_log_copy():
    data_list = []
    date_format = '%d/%m/%Y %H:%i:%s'
    link_text = request.url_root + 'api/downloadfile?filename='
    fields = ("runno","insurerid","process","downloadtime","starttime","endtime","emailsubject","date","fieldreadflag","failedfields","apicalledflag","apiparameter","apiresult","sms","error","row_no","emailid","completed","file_path","mail_id","hos_id","preauthid","amount","status","lettertime","policyno","memberid","comment","time_difference","diagno","insname","doa","dod","corp","polhol","jobid","time_difference2","weightage", "refno")
    data = request.form.to_dict()
    q = "select * from updation_detail_log_copy where " \
        "STR_TO_DATE(date, %s) between " \
        "STR_TO_DATE(%s, %s) and STR_TO_DATE(%s, %s) "
    params = [date_format, data['fromtime'], date_format, data['totime'], date_format]
    if 'hospital' in data:
        q = q + ' and hos_id=%s '
        params.append(data['hospital'])
    if 'refno' in data:
        q = q + ' and refno=%s '
        params.append(data['refno'])
    if 'flag' in data:
        q = q + " and completed=%s "
        params.append(data['flag'])
    params = tuple(params)
    with mysql.connector.connect(**p_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, params)
        r = cur.fetchall()
        for row in r:
            temp = {}
            for k, v in zip(fields, row):
                temp[k] = v
            temp['file_path'] = link_text + temp['file_path']
            data_list.append(temp)
    return jsonify(data_list)

@app.route("/getsettlementmails", methods=["POST"])
def get_settlement_mails():
    link_text = request.url_root + 'api/downloadfile?filename='
    data = request.form.to_dict()
    data_list, fields, params = [], ('sno', 'subject', 'date', 'attach_path'), []
    q = "SELECT sno, subject, date, attach_path FROM settlement_mails where id is not null"
    if 'hospital' in data:
        q = q + ' and hospital=%s'
        params.append(data['hospital'])
    if 'flag' in data:
        q = q + ' and completed=%s'
        params.append(data['flag'])
    if 'insurer' in data:
        q = q + ' and attach_path like %s'
        params.append('%' + data['insurer'] + '%')


    with mysql.connector.connect(**p_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, params)
        r = cur.fetchall()
        for row in r:
            temp = {}
            for k, v in zip(fields, row):
                temp[k] = v
            temp['attach_path'] = link_text + temp['attach_path']
            data_list.append(temp)
    return jsonify(data_list)

@app.route("/setsettlementmails", methods=["POST"])
def set_settlement_mails():
    data = request.form.to_dict()
    with mysql.connector.connect(**p_conn_data) as con:
        cur = con.cursor()
        q = "update settlement_mails set completed=%s where sno=%s"
        cur.execute(q, (data['flag'], data['flag']))
        con.commit()
    return jsonify('done')

@app.route("/getstgsettlementmails", methods=["POST"])
def get_stg_settlement_mails():
    link_text = request.url_root + 'api/downloadfile?filename='
    data = request.form.to_dict()
    fields = ("srno", "InsurerID", "ALNO", "ClaimNo", "UTRNo", "NetPayable", "Transactiondate", "attach_path")
    data_list = []

    # if 'hospital' in data:
    #     q = "SELECT stgSettlement.srno, stgSettlement.InsurerID, stgSettlement.ALNO, stgSettlement.ClaimNo, " \
    #         "stgSettlement.UTRNo, stgSettlement.NetPayable, stgSettlement.Transactiondate, settlement_mails.attach_path" \
    #         "  FROM stgSettlement  INNER JOIN settlement_mails  ON stgSettlement.sett_table_sno = settlement_mails.sno" \
    #         "  where InsurerID = '' or ALNO = '' or ClaimNo = '' or UTRNo = '' or NetPayable = '' or Transactiondate = '' " \
    #         "and settlement_mails.hospital=%s;"
    #     params = [data['hospital']]
    #
    # if 'parametername' in data:
    #     q = "SELECT stgSettlement.srno, stgSettlement.InsurerID, stgSettlement.ALNO, stgSettlement.ClaimNo, " \
    #         "stgSettlement.UTRNo, stgSettlement.NetPayable, stgSettlement.Transactiondate, settlement_mails.attach_path" \
    #         "  FROM stgSettlement  INNER JOIN settlement_mails  ON stgSettlement.sett_table_sno = settlement_mails.sno" \
    #         f"  where {data['parametername']}='';"
    #     params = [data['hospital']]

    if 'TPAID' in data:
        if data['TPAID'] == 'star':
            data['TPAID'] = 'big'
        if data['TPAID'] == 'newindia':
            q = "SELECT stgSettlement.srno, stgSettlement.InsurerID, stgSettlement.ALNO, stgSettlement.ClaimNo, " \
                "stgSettlement.UTRNo, stgSettlement.NetPayable, stgSettlement.Transactiondate, settlement_mails.attach_path" \
                " FROM stgSettlement  INNER JOIN settlement_mails  ON stgSettlement.sett_table_sno = settlement_mails.sno" \
                f" where InsurerID=%s "
            params = [data['TPAID']]
        else:
            q = "SELECT stgSettlement.srno, stgSettlement.InsurerID, stgSettlement.ALNO, stgSettlement.ClaimNo, " \
                "stgSettlement.UTRNo, stgSettlement.NetPayable, stgSettlement.Transactiondate, settlement_mails.attach_path" \
                " FROM stgSettlement  INNER JOIN settlement_mails  ON stgSettlement.sett_table_sno = settlement_mails.sno" \
                f" where TPAID=%s "
            params = [data['TPAID']]
        if 'hospital' in data:
            q = q + " and settlement_mails.hospital=%s "
            params.append(data['hospital'])
        if 'from' in data and 'to' in data:
            date_format = '%d/%m/%Y %H:%i:%s'
            q = q + " and STR_TO_DATE(stgSettlement.Transactiondate, %s) between STR_TO_DATE(%s, %s) and STR_TO_DATE(%s, %s)"
            params.extend([date_format, data['from'], date_format, data['to'], date_format])
    with mysql.connector.connect(**p_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, params)
        r = cur.fetchall()
        for row in r:
            temp = {}
            for k, v in zip(fields, row):
                temp[k] = v
            temp['attach_path'] = link_text + temp['attach_path']
            data_list.append(temp)
    return jsonify(data_list)

@app.route("/loadexcel", methods=["POST"])
def load_excel():
    data = request.form.to_dict()
    if 'file' not in request.files:
        return jsonify("upload file")
    file = request.files['file']
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        dst = os.path.join(dir_name, filename)
        file.save(dst)
        return jsonify(main(dst, data['TPAID']))
    return jsonify("upload excel file")

@app.route("/setstgsettlementmails", methods=["POST"])
def set_stg_settlement_mails():
    data = request.form.to_dict()
    fields = ("InsurerID", "ALNO", "ClaimNo", "UTRNo", "NetPayable", "Transactiondate")

    if 'srno' in data:
        q = "update stgSettlement set "
        params = []
        for i in fields:
            if i in data:
                q = q + i + "=" + '%s,'
                params.append(data[i])
        q = q.strip(',') + " where srno=%s"
        params.append(data['srno'])
        with mysql.connector.connect(**p_conn_data) as con:
            cur = con.cursor()
            cur.execute(q, params)
            con.commit()
        return jsonify({"msg": "done"})

@app.route("/gethospitalid", methods=["POST"])
def gethospitalid():
    data = request.form.to_dict()
    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        q = "select hospitalID from hospitallist where hospitalName=%s limit 1"
        cur.execute(q, (data['hospitalName'],))
        result = cur.fetchone()
        if result is not None:
            return {"hospitalID": result[0]}
    return {"error": "not found"}

@app.route("/getsentmaillogs", methods=["POST"])
def get_sentmaillogs():
    #flag, push_content
    # query add flag = null
    date_format = '%d/%m/%Y %H:%i:%s'
    fields = ("sno","transactionID","refNo","cdate","doc_count","push_content","push_success","flag","comment")
    data_dict = []
    data = request.form.to_dict()

    q = "SELECT * FROM sentmaillogs where flag is null "
    params = []
    if 'flag' in data:
        q = q + " and flag=%s"
        params.append(data['flag'])

    if 'pushcontent' in data:
        if data['pushcontent'] == '_blank':
            q = q + " and  push_content is null or push_content = ''"
        elif data['pushcontent'] == '_notblank':
            q = q + " and push_content != ''"
        else:
            q = q + ' and push_content like %s'
            params.append('%' + data['pushcontent'] + '%')

    if 'fromtime' in data and 'totime' in data:
        q = q + " and STR_TO_DATE(cdate, %s) between STR_TO_DATE(%s, %s) and STR_TO_DATE(%s, %s)"
        params = params + [date_format, data['fromtime'], date_format, data['totime'], date_format]

    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, params)
        result = cur.fetchall()
        for row in result:
            temp = {}
            for k, v in zip(fields, row):
                temp[k] = v
            data_dict.append(temp)
    return jsonify(data_dict)

@app.route("/setsentmaillogs", methods=["POST"])
def set_sentmaillogs():
    #flag comment sno
    data = request.form.to_dict()
    q = "update sentmaillogs "
    params = []
    if 'flag' in data and 'comment' in data:
        q = q + ' set flag=%s, comment=%s'
        params.extend([data['flag'], data['comment']])
    elif 'flag' in data:
        q = q + ' set flag=%s'
        params.append(data['flag'])
    elif 'comment' in data:
        q = q + ' set comment=%s'
        params.append(data['comment'])
    q1 = " where sno=%s"
    params.append(data['sno'])
    q = q + q1
    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        cur.execute(q, params)
        con.commit()
    return {"msg": "done"}

@app.route("/get_api_link", methods=["POST"])
def get_api_link():
    data = request.form.to_dict()
    with mysql.connector.connect(**logs_conn_data) as con:
        cur = con.cursor()
        query = "SELECT apiLink FROM apisConfig where hospitalID=%s and processName=%s limit 1;"
        cur.execute(query, (data['hospitalID'], data['processName']))
        result = cur.fetchone()
        if result is not None:
            return {'link': result[0]}
    return jsonify({'error': 'record not found'})

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

@app.route('/getpatientdetailsbyname', methods=["POST"])
def getpatientdetailsbyname():
    data = request.form.to_dict()
    field_list, datadict, records = ('srno', 'transactionID', 'PatientID_TreatmentID', 'Type_Ref', 'Type',
                                     'status', 'HospitalID', 'cdate',
                                     'person_name', 'smsTrigger', 'pushTrigger', 'insurerID', 'fStatus', 'fLock',
                                     'lock', 'error', 'errorDescription'), dict(), []
    if 'p_sname' in data and 'hospitalid' in data:
        url = 'http://3.7.8.68:9982/api/get_from_name1'
        #no_limit = X
        if 'no_limit' in data:
            myobj = {'hospital_id': data['hospitalid'], 'name': data['p_sname'], 'no_limit': data['no_limit']}
        else:
            myobj = {'hospital_id': data['hospitalid'], 'name': data['p_sname']}
        x = requests.post(url, data=myobj)
        temp = x.json()
        for record in temp:
            datadict = {}
            for k, v in record.items():
                datadict[k] = v
            datadict['transactionID'] = ""
            q = "select transactionID from hospitalTLog where Type_Ref=%s union select transactionID from hospitalTLogDel where Type_Ref=%s"
            with mysql.connector.connect(**logs_conn_data) as con:
                cur = con.cursor()
                cur.execute(q, (datadict['refno'], datadict['refno']))
                r = cur.fetchall()
                if len(r) > 0:
                    datadict['transactionID'] = r[-1][0]
                cur.execute("select name from insurer_tpa_master where TPAInsurerID=%s limit 1",
                            (datadict['insurerID'],))
                r1 = cur.fetchone()
                if r1 is not None:
                    datadict['insurer_tpa'] = r1[0]
            for i in field_list:
                if i not in datadict:
                    datadict[i] = ""
            records.append(datadict)
    return jsonify(records)

@app.route('/get_hospitaltlog', methods=["POST"])
def get_hospitaltlog():
    data = request.form.to_dict()
    field_list, datadict, records = ('srno', 'transactionID', 'PatientID_TreatmentID', 'Type_Ref', 'Type',
                                     'status', 'HospitalID', 'cdate',
                                     'person_name', 'smsTrigger', 'pushTrigger', 'insurerID', 'fStatus', 'fLock',
                                     'lock', 'error', 'errorDescription'), dict(), []
    preauth_field_list = ("preauthNo", "MemberId", "p_sname", "admission_date", "dischargedate", "flag","CurrentStatus", "cdate", "up_date", "hospital_name", "p_policy")

    q = "select `srno`, `transactionID`,`PatientID_TreatmentID`,`Type_Ref`,`Type`,`status`,`HospitalID`,`cdate`,`person_name`,`smsTrigger`,`pushTrigger`,`insurerID`,`fStatus`,`fLock`,`lock`,`error`,`errorDescription` from hospitalTLog where transactionID is not null and transactionID != '' and srno is not null "
    params = []
    #add preauth params p_sname CurrentStatus
    if 'fromdate' in data and 'todate' in data:
        fmt = '%d/%m/%Y %H:%i:%s'
        q = q + " and str_to_date(cdate, %s) BETWEEN str_to_date(%s, %s) AND str_to_date(%s, %s)"
        params = params + [fmt, data['fromdate'], fmt, data['todate'], fmt]
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
    q = q + ' order by cdate desc limit 100'
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
                else:
                    for key in preauth_field_list:
                        datadict[key] = ""
            records.append(datadict)
    return jsonify(records)

@app.route('/modify_apisLog', methods=["POST"])
def modify_apisLog():
    field_list, datadict = ('dateTime', 'hospitalID','referenceNo','method','title','purpose','status',
                            'request','response','error','runtime','ipAddress', 'transactionID'), dict()
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
            q = "INSERT INTO apisLog (`dateTime`,`hospitalID`,`referenceNo`,`method`,`title`,`purpose`,`status`,`request`,`response`,`error`,`runtime`,`ipAddress`,transactionID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
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
