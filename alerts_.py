import re
import mysql.connector
from datetime import datetime
from alertconfig import dbconfig
from make_log import log_exceptions, log_data, log_custom_data
from sms_and_push import send_push, send_sms
from db_conf import hosp_conn_data

info_dict = dict()

def get_db_conf(**kwargs):
    fields = ('host', 'database', 'port', 'user', 'password')
    if 'env' not in kwargs:
        kwargs['env'] = 'live'
    conn_data = {'host': "database-iclaim.caq5osti8c47.ap-south-1.rds.amazonaws.com",
                 'user': "admin",
                 'password': "Welcome1!",
                 'database': 'portals'}
    with mysql.connector.connect(**conn_data) as con:
        cur = con.cursor()
        q = 'SELECT host, dbName, port, userName, password FROM dbConfiguration where hospitalID=%s and environment=%s limit 1;'
        cur.execute(q, (kwargs['hosp'], kwargs['env']))
        result = cur.fetchone()
        if result is not None:
            conf_data = dict()
            for key, value in zip(fields, result):
                conf_data[key] = value
            return conf_data


def check_table(hospital_id, table_name, current_dbconf):
    with mysql.connector.connect(**current_dbconf) as con:
        cur = con.cursor()
        q = 'SHOW TABLES LIKE %s;'
        cur.execute(q, (table_name,))
        result = cur.fetchone()
        if result is not None:
            return current_dbconf
        else:
            dbconf = get_db_conf(hosp=hospital_id)
            if dbconf is not None:
                return get_db_conf(hosp=hospital_id)
            return current_dbconf

def trigger(refno, hospital_id, t_ype, status):
    try:
        master = dict()
        q = f"SELECT `Type` FROM send_sms_config where Type != 'Admin' and HospitalID=%s and statuslist LIKE '%{status}%'"
        q1 = f"SELECT `Type` FROM send_sms_config where Type='Admin' and statuslist LIKE '%{status}%'"
        conn_data = check_table(hospital_id, 'send_sms_config', hosp_conn_data)
        with mysql.connector.connect(**conn_data) as con:
            cur = con.cursor()
            cur.execute(q, (hospital_id,))
            result = cur.fetchall()
            cur.execute(q1)
            result1 = cur.fetchall()
        user_types = [i[0] for i in result] + [i[0] for i in result1]
        sms_texts = []
        print('user types', refno, hospital_id, t_ype, status)
        for i in user_types:
            q = "SELECT sms FROM alerts where UserType=%s and Type=%s and Status=%s limit 1"
            conn_data = check_table(hospital_id, 'alerts', hosp_conn_data)
            with mysql.connector.connect(**conn_data) as con:
                cur = con.cursor()
                cur.execute(q, (i, t_ype, status))
                result = cur.fetchone()
                if result is not None:
                    master[i] = {"smstext_raw": result[0]}
        print('raw sms', refno, hospital_id, t_ype, status)
        for user_type, data in master.items():
            word_list = re.findall(r"(?<=<<)\w+(?=>>)", data['smstext_raw'])
            master[user_type]['wordlist'] = word_list
        for i in master:
            master[i]['worddict'] = {}
            for k in master[i]['wordlist']:
                q = "select tableMap, tableColumn from variablesMap where variableName=%s"
                conn_data = check_table(hospital_id, 'variablesMap', hosp_conn_data)
                with mysql.connector.connect(**conn_data) as con:
                    cur = con.cursor()
                    cur.execute(q, (k, ))
                    result = cur.fetchone()
                    if result is not None:
                        master[i]['worddict'][k] = {'table': result[0], 'column': result[1]}
        print('word list', refno, hospital_id, t_ype, status)
        for i in master:
            for j in master[i]['worddict']:
                word, table, column = j, master[i]['worddict'][j]['table'], master[i]['worddict'][j]['column']
                if table not in ['preauth_document', 'query_document']:
                    q = "select %s from %s where refno='%s' limit 1" % (column, table, refno)
                    conn_data = check_table(hospital_id, table, hosp_conn_data)
                    with mysql.connector.connect(**conn_data) as con:
                        cur = con.cursor()
                        try:
                            cur.execute(q)
                        except:
                            z = 1
                        result = cur.fetchone()
                        if result is not None:
                            master[i]['worddict'][j]['value'] = result[0]
                else:
                    q = "select srno from status_track where Type_Ref=%s and Type=%s and status=%s limit 1"
                    conn_data = check_table(hospital_id, 'status_track', hosp_conn_data)
                    with mysql.connector.connect(**conn_data) as con:
                        cur = con.cursor()
                        cur.execute(q, (refno, t_ype, status))
                        statustrackid = cur.fetchone()
                        if statustrackid is not None:
                            statustrackid = statustrackid[0]
                            q = "select %s from %s where statustrackid='%s' limit 1" % (column, table, statustrackid)
                            conn_data = check_table(hospital_id, table, hosp_conn_data)
                            with mysql.connector.connect(**conn_data) as con:
                                cur = con.cursor()
                                cur.execute(q)
                                result = cur.fetchone()
                                if result is not None:
                                    master[i]['worddict'][j]['value'] = result[0]
        for i in master:
            if 'insurerTPA' in master[i]['worddict']:
                temp = master[i]['worddict']['insurerTPA']['value']
                q = "select name from insurer_tpa_master where TPAInsurerID=%s"
                with mysql.connector.connect(**conn_data) as con:
                    cur = con.cursor()
                    cur.execute(q, (temp, ))
                    result = cur.fetchone()
                    if result is not None:
                        master[i]['worddict']['insurerTPA']['value'] = result[0]
        print('word value', refno, hospital_id, t_ype, status)
        for i in master:
            raw_sms, worddict = master[i]['smstext_raw'], master[i]['worddict']
            for j in worddict:
                raw_sms = raw_sms.replace(f"<<{j}>>", worddict[j]['value'])
            master[i]['sms'] = raw_sms
        print('sms body', refno, hospital_id, t_ype, status)
        from common import update_data_sms
        for i in master:
            if i == 'Patient':
                no_list = fetch_p_contact_no(refno, hosp_conn_data, hospital_id)
                for mob_no in no_list:
                    data_dict = {}
                    print("sending sms")
                    response = send_sms(mob_no, master[i]['sms'])
                    data_dict['mobileno'] = mob_no
                    data_dict['type'] = t_ype
                    data_dict['notification_text'] = master[i]['sms']
                    data_dict['sms'] = "X"
                    data_dict['push'] = ""
                    data_dict['timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    data_dict['messageid'] = ""
                    data_dict['error'] = response  # if response == '200', then flag = 2
                    data_dict['device_token'] = ""
                    data_dict['ref_no'] = refno
                    write_to_alert_log(data_dict)
                    if response == 200:
                        update_data_sms(smsTrigger='1', error='0', Type_Ref=refno)
                    else:
                        update_data_sms(smsTrigger='0', error='1', errorDescription=response, Type_Ref=refno)
            elif i == 'Admin':
                p = fetch_admin_contacts(hosp_conn_data, hospital_id)
                for mob_no, hosp_id in p:
                    if hospital_id == hosp_id or hosp_id == 'Admin':
                        data_dict = {}
                        print("sending sms")
                        response = send_sms(mob_no, master[i]['sms'])
                        data_dict['mobileno'] = mob_no
                        data_dict['type'] = t_ype
                        data_dict['notification_text'] = master[i]['sms']
                        data_dict['sms'] = "X"
                        data_dict['push'] = ""
                        data_dict['timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                        data_dict['messageid'] = ""
                        data_dict['error'] = response  # if response == '200', then flag = 2
                        data_dict['device_token'] = ""
                        data_dict['ref_no'] = refno
                        write_to_alert_log(data_dict)
                        if response == 200:
                            update_data_sms(smsTrigger='1', error='0', Type_Ref=refno)
                        else:
                            update_data_sms(smsTrigger='0', error='1', errorDescription=response, Type_Ref=refno)
            elif i == 'Hospital':
                no_list = fetch_hosp_contacts(hospital_id, hosp_conn_data)
                for mob_no in no_list:
                    data_dict = {}
                    response =send_sms(mob_no, master[i]['sms'])
                    data_dict['mobileno'] = mob_no
                    data_dict['type'] = t_ype
                    data_dict['notification_text'] = master[i]['sms']
                    data_dict['sms'] = "X"
                    data_dict['push'] = ""
                    data_dict['timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    data_dict['messageid'] = ""
                    data_dict['error'] = response  # if response == '200', then flag = 2
                    data_dict['device_token'] = ""
                    data_dict['ref_no'] = refno
                    write_to_alert_log(data_dict)
                    if response == 200:
                        update_data_sms(smsTrigger='1', error='0', Type_Ref=refno)
                    else:
                        update_data_sms(smsTrigger='0', error='1', errorDescription=response, Type_Ref=refno)
        return True
    except Exception as e:
        log_exceptions(refno=refno, hospital_id=hospital_id, t_ype=t_ype, status=status)


def triggerAlert(refno, hospital_id):
    try:
        a = fetch_sms_and_push_notification_records(refno, hospital_id)
        # print(info_dict)
        for i in a:
            if i[0] == 'Patient':  # loop for all the values of a[i][0]
                # if UserType from iclaimtest.alerts is Patient
                b = fetch_p_contact_no(refno)
                e = fetch_token_list(b, "patient")
                send_sms_and_notifications(e, [i], refno, hospital_id)
                log_data(i=[i], b=b, e=e, refno=refno, hid=hospital_id)
            elif i[0] == 'Hospital':
                # if UserType from iclaimtest.alerts is not Patient
                c = fetch_hosp_contacts(hospital_id)
                d = fetch_token_list(c, "hospital")
                send_sms_and_notifications(d, [i], refno, hospital_id)
                log_data(i=[i], c=c, d=d, refno=refno, hid=hospital_id)
            elif i[0] == 'Admin':
                p = fetch_admin_contacts()
                no_list = [i[0] for i in p]
                receiver_list = [i[1] for i in p]
                q = fetch_token_list(no_list, "admin")
                new_list = []
                for i, j in zip(q, receiver_list):
                    i = list(i)
                    i[0] = i[0] + '_' + j
                    new_list.append(i)
                send_sms_and_notifications(new_list, [i], refno, hospital_id)

        return True
    except Exception as e:
        return "fail in triggeralert " + str(e)


def fetch_sms_and_push_notification_records(refno, hospital_id):
    try:
        global info_dict
        mydb = mysql.connector.connect(
            host=dbconfig["Config"]['MYSQL_HOST'],
            user=dbconfig["Config"]['MYSQL_USER'],
            password=dbconfig["Config"]['MYSQL_PASSWORD'],
            database=dbconfig["Config"]['MYSQL_DB']
        )
        mycursor, result3, url = mydb.cursor(buffered=True), [], 'https://vnusoftware.com/iclaimmax/'
        # select currentstatus from preauth where where PatientID_TreatmentID='%s' and HospitalID='%s' -> Preauth - Information_Awaiting ->type and status
        q1 = """select currentstatus, PatientID_TreatmentID from preauth where refno='%s' and HospitalID='%s' limit 1;""" % (
        refno, hospital_id)
        mycursor.execute(q1)
        result = mycursor.fetchone()
        log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, result=result)

        if result is not None:
            result, status, patientid_treatmentid = [result[0].split('-')[0].strip()], result[0].split('-')[1].strip(), \
                                                    result[1]  # use strip value to make status
            info_dict['patientid_treatmentid'], info_dict['status'] = patientid_treatmentid, status
            q2 = """SELECT Type FROM send_sms_config where statuslist LIKE '%s';""" % ("%" + status + "%")
            mycursor.execute(q2)
            result2 = mycursor.fetchall()
            log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, result2=result2)
            if result2 is not None:
                for i in result2:
                    # uncomment below line
                    user_type, t_ype = i[0], result[0]
                    # remove below line
                    # status, user_type = "Approved", "Patient"
                    q3 = """SELECT UserType,Link_SMS, SMS, PushNotification FROM alerts where UserType='%s' and Type='%s' and Status='%s' limit 1""" % (
                    user_type, t_ype, status)
                    mycursor.execute(q3)
                    temp = mycursor.fetchone()
                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, temp=temp)
                    if temp is not None:
                        col_dict = {}
                        word_list = re.compile(r"(?<=<<)\w+(?=>>)").findall(temp[2]) + re.compile(
                            r"(?<=<<)\w+(?=>>)").findall(temp[3])
                        for j in word_list:
                            q4 = """select tableMap, tableColumn from variablesMap where variableName='%s' limit 1""" % j
                            mycursor.execute(q4)
                            result4 = mycursor.fetchone()
                            mycursor.reset()
                            log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, result4=result4)
                            value = ''
                            if result4 is not None:
                                if result4[0] == 'preauth':
                                    q5 = """select %s from preauth where refno = '%s';""" % (result4[1], refno)
                                    mycursor.execute(q5)
                                    result5 = mycursor.fetchone()
                                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id,
                                                    result5=result5)
                                    if result5 is not None:
                                        value = result5[0]
                                if result4[1] == 'insname' and value != '':
                                    q5 = """select %s from insurer_tpa_master where TPAInsurerID = '%s';""" % (
                                    'name', value)
                                    mycursor.execute(q5)
                                    result5 = mycursor.fetchone()
                                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id,
                                                    result5=result5)
                                    if result5 is not None:
                                        value = result5[0]
                                if result4[0] == 'claim':
                                    q5 = """select %s from claim where claimNo = '%s';""" % (result4[1], refno)
                                    mycursor.execute(q5)
                                    result5 = mycursor.fetchone()
                                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id,
                                                    result5=result5)
                                    if result5 is not None:
                                        value = result5[0]
                                if result4[0] == 'preauth_document':
                                    q5 = """SELECT flag, file_path, Doctype FROM (`preauth_document`) WHERE `status` ='%s' AND `PatientID_TreatmentID`='%s'""" % (
                                    status, patientid_treatmentid)
                                    mycursor.execute(q5)
                                    result5 = mycursor.fetchone()
                                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id,
                                                    result5=result5)
                                    if result5 is not None:
                                        flag, filepath, doctype, route = result5[0], result5[1], result5[2], ''
                                        if flag == 'IP':
                                            route = 'assets/pdf/newpre_auth/initialassessment/'
                                        elif flag == 'E':
                                            route = 'assets/upload/preauth/Enhancement/'
                                        elif flag == 'OT':
                                            route = 'assets/pdf/otnote/'
                                        elif doctype == 'Preauth':
                                            route = 'assets/pdf/newpre_auth/'
                                        else:
                                            route = 'assets/upload/preauth/'
                                        value = url + route + filepath
                                        # value = """<a href="%s">file link</a>""" % value

                                if result4[0] == 'claim_document':
                                    q5 = """SELECT flag, file_path, Doctype FROM (`claim_document`) WHERE `status` ='%s' AND `PatientID_TreatmentID`='%s'""" % (
                                    status, patientid_treatmentid)
                                    mycursor.execute(q5)
                                    result5 = mycursor.fetchone()
                                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id,
                                                    result5=result5)
                                    if result5 is not None:
                                        flag, filepath, doctype, route = result5[0], result5[1], result5[2], ''
                                        if flag == 'FB' or doctype == 'Hospital main bill':
                                            route = 'assets/pdf/finalbill/'
                                        if flag == 'DS' or doctype == 'Hospital Discharge summary':
                                            route = 'assets/pdf/dischargeSummary/'
                                        if flag == 'OT' or doctype == 'Operation Theatre Notes':
                                            route = 'assets/pdf/otnote/'
                                        if flag == 'PR' or doctype == 'Original Pre-authorization request':
                                            route = 'assets/pdf/newpre_auth/'
                                        else:
                                            route = 'assets/upload/claim/'
                                        value = url + route + filepath
                                        # value = """<a href="%s">file link</a>""" % value

                                col_dict[j] = {'table': result4[0], 'column': result4[1], 'value': value}
                        temp = list(temp)
                        try:
                            for key, value in col_dict.items():
                                info_dict[key] = value['value']
                            for k in col_dict:
                                try:
                                    if col_dict[k]['value'] is not None:
                                        temp[2] = temp[2].replace('<<' + k + '>>', col_dict[k]['value'])
                                        temp[3] = temp[3].replace('<<' + k + '>>', col_dict[k]['value'])

                                except:
                                    log_exceptions()
                        except:
                            log_exceptions()
                            pass
                        result3.append(temp)

            else:
                log_data(status=status, info="no such records in send_sms_config")
        else:
            # log_data(params=(patientid_treatmentid, hospital_id, status), info="no such records in status_track")
            pass
        mycursor.close()
        log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, result3=result3)
        return result3
    except Exception as e:
        log_exceptions()
        return e


def fetch_p_contact_no(refno, hosp_conn_data, hospital_id):
    try:
        q = """SELECT p_contact FROM preauth where refno='%s';""" % (refno)
        conn_data = check_table(hospital_id, 'preauth', hosp_conn_data)
        with mysql.connector.connect(**conn_data) as con:
            cur = con.cursor()
            cur.execute(q)
            no_list = cur.fetchall()
            clean = []
            for i in no_list:
                clean.append(i[0])
            return clean
    except:
        log_exceptions()
        return []


def fetch_hosp_contacts(hospital_id, hosp_conn_data):
    try:
        q = """SELECT he_mobile FROM hospital_employee where he_hospital_id = '%s';""" % (hospital_id)
        conn_data = check_table(hospital_id, 'hospital_employee', hosp_conn_data)
        with mysql.connector.connect(**conn_data) as con:
            cur = con.cursor()
            cur.execute(q)
            no_list = cur.fetchall()
            clean = []
            for i in no_list:
                clean.append(i[0])
            return clean
    except:
        log_exceptions()
        return []


def fetch_admin_contacts(hosp_conn_data, hospital_id):
    try:
        q = """SELECT type_values, receiver FROM admin_alerts where status = 1 and sendUpdate = 1 and type = 'mobile'"""
        conn_data = check_table(hospital_id, 'admin_alerts', hosp_conn_data)
        with mysql.connector.connect(**conn_data) as con:
            cur = con.cursor()
            cur.execute(q)
            no_list = cur.fetchall()
            clean = []
            for i in no_list:
                clean.append(i)
            return clean
    except:
        log_exceptions()
        return []


def fetch_token_list(mobile_no_list, type):
    try:
        mydb = mysql.connector.connect(
            host=dbconfig["Config"]['MYSQL_HOST'],
            user=dbconfig["Config"]['MYSQL_USER'],
            password=dbconfig["Config"]['MYSQL_PASSWORD'],
            database=dbconfig["Config"]['MYSQL_DB']
        )
        token_list = []
        mycursor = mydb.cursor()
        for i in mobile_no_list:
            q1 = """SELECT tokenNo FROM device_Token where mobileNo='%s'""" % (i)
            mycursor.execute(q1)
            temp = []
            for j in mycursor:
                temp.append(j[0])
            token_list.append((i, type, temp))
        mycursor.close()
        return token_list
    except:
        log_exceptions()
        return []


def send_sms_and_notifications(mob_token_list, result, ref_no, hospital_id):
    from common import update_data_sms
    for i in result:
        body = i[2]
        flag = 0
        # mob_token_list = list(set(mob_token_list))
        for j in mob_token_list:
            mob_no, t_ype, token_list, data_dict = j[0], j[1], j[2], dict()
            if t_ype != 'admin':
                response = send_sms(mob_no, body)
                data_dict['mobileno'] = mob_no
                data_dict['type'] = t_ype
                data_dict['notification_text'] = body
                data_dict['sms'] = "X"
                data_dict['push'] = ""
                data_dict['timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                data_dict['messageid'] = ""
                data_dict['error'] = response  # if response == '200', then flag = 2
                data_dict['device_token'] = ""
                data_dict['ref_no'] = ref_no
                write_to_alert_log(data_dict)
                if response == 200:
                    update_data_sms(smsTrigger='1', error='0', Type_Ref=ref_no)
                else:
                    update_data_sms(smsTrigger='0', error='1', errorDescription=response, Type_Ref=ref_no)
            else:
                mobile, receiver = mob_no.split('_')
                if receiver == hospital_id or receiver == 'Admin':
                    response = send_sms(mob_no, body)
                    data_dict['mobileno'] = mob_no
                    data_dict['type'] = t_ype
                    data_dict['notification_text'] = body
                    data_dict['sms'] = "X"
                    data_dict['push'] = ""
                    data_dict['timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    data_dict['messageid'] = ""
                    data_dict['error'] = response  # if response == '200', then flag = 2
                    data_dict['device_token'] = ""
                    data_dict['ref_no'] = ref_no
                    write_to_alert_log(data_dict)
                    if response == 200:
                        update_data_sms(smsTrigger='1', error='0', Type_Ref=ref_no)
                    else:
                        update_data_sms(smsTrigger='0', error='1', errorDescription=response, Type_Ref=ref_no)


def write_to_alert_log(data_dict):
    try:
        try:
            mobileno, t_ype, notification_text, sms = data_dict['mobileno'], data_dict['type'], data_dict[
                'notification_text'], data_dict['sms']
            push, timestamp, messageid, error = data_dict['push'], data_dict['timestamp'], data_dict['messageid'], data_dict['error']
            device_token = data_dict['device_token']
            ref_no = data_dict['ref_no']
        except:
            mobileno, t_ype, notification_text = "", "", ""
            sms, push, timestamp, messageid, error, device_token, ref_no = "", "", "", "", "", "", ""
        conn_data = {'host': "database-iclaim.caq5osti8c47.ap-south-1.rds.amazonaws.com",
                     'user': "admin",
                     'password': "Welcome1!",
                     'database': 'portals'}
        with mysql.connector.connect(**conn_data) as con:
            cur = con.cursor()
            q1 = """insert into alerts_log (mobileno, type, notification_text, sms, push, timestamp, messageid, error, device_token, ref_no) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')""" % (
            mobileno, t_ype, notification_text, sms, push, timestamp, messageid, error, device_token, ref_no)
            cur.execute(q1)
            con.commit()
    except:
        log_exceptions()

if __name__ == "__main__":
    refno, hospital_id, t_ype, status = 'MSS-1006467', '8', 'Claim', 'Approved'
    trigger(refno, hospital_id, t_ype, status)