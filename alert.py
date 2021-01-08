import re
import mysql.connector
from datetime import datetime
from alertconfig import dbconfig
from make_log import log_exceptions, log_data, log_custom_data
from sms_and_push import send_push, send_sms


def triggerAlert1(patientid_treatmentid,hospital_id,status):
    a = fetch_sms_and_push_notification_records(patientid_treatmentid, hospital_id, status)
    if a[0][0] == 'Patient':
        #if UserType from iclaimtest.alerts is Patient
        b = fetch_p_contact_no(patientid_treatmentid)
        e = fetch_token_list(b, "patient")
        g = send_sms_and_notifications(e, a)
        return True
    elif a[0][0] != 'Patient':
        # if UserType from iclaimtest.alerts is not Patient
        c = fetch_hosp_contacts(hospital_id)
        d = fetch_token_list(c, "hospital")
        f = send_sms_and_notifications(d, a)
        return True




def triggerAlert(refno,hospital_id):
    try:
        a = fetch_sms_and_push_notification_records(refno, hospital_id)
        for i in a:
            if i[0] == 'Patient': # loop for all the values of a[i][0] 
                #if UserType from iclaimtest.alerts is Patient
                b = fetch_p_contact_no(refno)
                e = fetch_token_list(b, "patient")
                #pool = Pool(processes=1)              # Start a worker processes.
                #result = pool.apply_async(send_sms_and_notifications, [e, [i], refno])
                send_sms_and_notifications(e, [i], refno)
                log_data(i=[i], b=b, e=e, refno=refno, hid=hospital_id)
            elif i[0] != 'Patient':
                # if UserType from iclaimtest.alerts is not Patient
                c = fetch_hosp_contacts(hospital_id)
                d = fetch_token_list(c, "hospital")
                #pool = Pool(processes=1)              # Start a worker processes.
                #result = pool.apply_async(send_sms_and_notifications, [d, [i], refno])
                send_sms_and_notifications(d, [i], refno)
                log_data(i=[i], c=c, d=d, refno=refno, hid=hospital_id)
        return True
    except Exception as e:
        return "fail in triggeralert "+str(e)

def fetch_sms_and_push_notification_records(refno, hospital_id):
    try:
        mydb = mysql.connector.connect(
          host=dbconfig["Config"]['MYSQL_HOST'],
          user=dbconfig["Config"]['MYSQL_USER'],
          password=dbconfig["Config"]['MYSQL_PASSWORD'],
          database=dbconfig["Config"]['MYSQL_DB']
        )
        mycursor, result3, url = mydb.cursor(buffered=True), [], 'https://vnusoftware.com/iclaimmax/'
        # select currentstatus from preauth where where PatientID_TreatmentID='%s' and HospitalID='%s' -> Preauth - Information_Awaiting ->type and status
        q1 = """select currentstatus, PatientID_TreatmentID from preauth where refno='%s' and HospitalID='%s' limit 1;""" % (refno, hospital_id)
        mycursor.execute(q1)
        result = mycursor.fetchone()
        log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, result=result)

        if result is not None:
            result, status, patientid_treatmentid = [result[0].split('-')[0].strip()], result[0].split('-')[1].strip(), result[1] #use strip value to make status
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
                    q3 = """SELECT UserType,Link_SMS, SMS, PushNotification FROM alerts where UserType='%s' and Type='%s' and Status='%s' limit 1""" % (user_type, t_ype, status)
                    mycursor.execute(q3)
                    temp = mycursor.fetchone()
                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, temp=temp)
                    if temp is not None:
                        col_dict = {}
                        word_list = re.compile(r"(?<=<<)\w+(?=>>)").findall(temp[2]) + re.compile(r"(?<=<<)\w+(?=>>)").findall(temp[3])
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
                                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, result5=result5)
                                    if result5 is not None:
                                        value = result5[0]
                                if result4[1] == 'insname' and value != '':
                                    q5 = """select %s from insurer_tpa_master where TPAInsurerID = '%s';""" % ('name', value)
                                    mycursor.execute(q5)
                                    result5 = mycursor.fetchone()
                                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, result5=result5)
                                    if result5 is not None:
                                        value = result5[0]
                                if result4[0] == 'claim':
                                    q5 = """select %s from claim where claimNo = '%s';""" % (result4[1], refno)
                                    mycursor.execute(q5)
                                    result5 = mycursor.fetchone()
                                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, result5=result5)
                                    if result5 is not None:
                                        value = result5[0]                               
                                if result4[0] == 'preauth_document':
                                    q5 = """SELECT flag, file_path, Doctype FROM (`preauth_document`) WHERE `status` ='%s' AND `PatientID_TreatmentID`='%s'""" % (status, patientid_treatmentid)
                                    mycursor.execute(q5)
                                    result5 = mycursor.fetchone()
                                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, result5=result5)
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
                                        #value = """<a href="%s">file link</a>""" % value


                                if result4[0] == 'claim_document':
                                    q5 = """SELECT flag, file_path, Doctype FROM (`claim_document`) WHERE `status` ='%s' AND `PatientID_TreatmentID`='%s'""" % (status, patientid_treatmentid)
                                    mycursor.execute(q5)
                                    result5 = mycursor.fetchone()
                                    log_custom_data(filename='result', refno=refno, hospital_id=hospital_id, result5=result5)
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
                                        #value = """<a href="%s">file link</a>""" % value

                                col_dict[j] = {'table':result4[0], 'column':result4[1], 'value':value}
                        temp = list(temp)
                        try:
                            for k in col_dict:
                                try:
                                    if col_dict[k]['value'] is not None:
                                        temp[2] = temp[2].replace('<<'+k+'>>', col_dict[k]['value'])
                                        temp[3] = temp[3].replace('<<'+k+'>>', col_dict[k]['value'])

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


def fetch_p_contact_no(refno):
    try:
        mydb = mysql.connector.connect(
            host=dbconfig["Config"]['MYSQL_HOST'],
            user=dbconfig["Config"]['MYSQL_USER'],
            password=dbconfig["Config"]['MYSQL_PASSWORD'],
            database=dbconfig["Config"]['MYSQL_DB']
        )
        mycursor = mydb.cursor()
        q1 = """SELECT p_contact FROM preauth where refno='%s';""" % (refno)
        mycursor.execute(q1)
        no_list = mycursor.fetchall()
        if no_list is None:
            mycursor.close()
            return []
        clean = []
        for i in no_list:
            clean.append(i[0])
        mycursor.close()
        return clean
    except:
        log_exceptions()
        return []


def fetch_hosp_contacts(hospital_id):
    try:
        mydb = mysql.connector.connect(
            host=dbconfig["Config"]['MYSQL_HOST'],
            user=dbconfig["Config"]['MYSQL_USER'],
            password=dbconfig["Config"]['MYSQL_PASSWORD'],
            database=dbconfig["Config"]['MYSQL_DB']
        )
        mycursor = mydb.cursor()
        q1 = """SELECT he_mobile FROM hospital_employee where he_hospital_id = '%s';""" % (hospital_id)
        mycursor.execute(q1)
        no_list = mycursor.fetchall()
        if no_list is None:
            return []
        clean = []
        for i in no_list:
            clean.append(i[0])
        mycursor.close()
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


def send_sms_and_notifications(mob_token_list, result, ref_no):
    for i in result:
        body = i[2]
        flag = 0
        #mob_token_list = list(set(mob_token_list))
        for j in mob_token_list:
            mob_no, t_ype, token_list = j[0], j[1], j[2]
            if t_ype == "hospital" or t_ype == "admin": # add admin also
                for k in token_list:
                    data_dict = {}
                    response = send_push(k, 'Status update', body)

                    data_dict['mobileno'] = mob_no
                    data_dict['type'] = t_ype
                    data_dict['notification_text'] = body
                    data_dict['sms'] = ""
                    data_dict['push'] = "X"
                    data_dict['timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    data_dict['messageid'] = response[2]
                    data_dict['error'] = response[1]
                    data_dict['device_token'] = k
                    data_dict['ref_no'] = ref_no
                    write_to_alert_log(data_dict)
            else:
                for k in token_list:
                    data_dict = {}
                    response = send_push(k, 'Status update', body)
                    flag = 0
                    if response[0] == False:
                        flag = 1

                    data_dict['mobileno'] = mob_no
                    data_dict['type'] = t_ype
                    data_dict['notification_text'] = body
                    data_dict['sms'] = ""
                    data_dict['push'] = "X"
                    data_dict['timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    data_dict['messageid'] = response[2]
                    data_dict['error'] = response[1]
                    data_dict['device_token'] = k
                    data_dict['ref_no'] = ref_no

                    write_to_alert_log(data_dict)

                    if flag == 1 or flag == 2:
                        if mob_no == '0' or mob_no == '':
                            response = 'invalid mobno'
                            flag = 2
                        else:
                            response = send_sms(mob_no, body) #check whether mob no exists or not, change flag value to 2 and check at last flag MUST equal to 1
                        if response == 200:
                            flag = 2
                        data_dict['mobileno'] = mob_no
                        data_dict['type'] = t_ype
                        data_dict['notification_text'] = body
                        data_dict['sms'] = "X"
                        data_dict['push'] = "Failed"
                        data_dict['timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                        data_dict['messageid'] = ""
                        data_dict['error'] = response # if response == '200', then flag = 2
                        data_dict['device_token'] = ""
                        data_dict['ref_no'] = ref_no

                        write_to_alert_log(data_dict)
                if (flag == 1 or len(token_list) == 0) and flag != 2:
                    data_dict = {}
                    if mob_no == '0' or mob_no == '':
                        response = 'invalid mobno'
                    else:
                        response = send_sms(mob_no, body) #check whether mob no exists or not, change flag value to 2 and check at last flag MUST equal to 1
                    data_dict['mobileno'] = mob_no
                    data_dict['type'] = t_ype
                    data_dict['notification_text'] = body
                    data_dict['sms'] = "X"
                    data_dict['push'] = "Failed"
                    data_dict['timestamp'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                    data_dict['messageid'] = ""
                    data_dict['error'] = response
                    data_dict['device_token'] = ""
                    data_dict['ref_no'] = ref_no

                    write_to_alert_log(data_dict)



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
        mydb = mysql.connector.connect(
            host=dbconfig["Config"]['MYSQL_HOST'],
            user=dbconfig["Config"]['MYSQL_USER'],
            password=dbconfig["Config"]['MYSQL_PASSWORD'],
            database=dbconfig["Config"]['MYSQL_DB']
        )
        mycursor = mydb.cursor()
        q1 = """insert into alerts_log (mobileno, type, notification_text, sms, push, timestamp, messageid, error, device_token, ref_no) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')"""% (mobileno, t_ype, notification_text, sms, push, timestamp, messageid, error, device_token, ref_no)
        mycursor.execute(q1)
        mydb.commit()
    except:
        log_exceptions()
        pass

if __name__ == "__main__":
    refno, hos_id = 'MSS-1004649', '8'
    triggerAlert(refno,hos_id)
