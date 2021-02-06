import sys
import json
#import http.client
from make_log import log_exceptions, log_custom_data
import requests
import re

def send_push(to, title, body):
    message_id, file_url = "", ""
    #if to is blank return false
    # send push will not execute if token list is blank
    if to is None or to == "":
        return False, "", message_id

    try:
        serverToken = 'AAAAKXlyywA:APA91bFI-NyB5RpfiSyk5Sd6guWtL5GeHNTBhLFREFFxukrqOqxYckIgIc1MIcXS_7m1qEHiL357oUp7e1bbpRHsREbSmRBp8KVWpMVc0voO91q7OLgbRaPlHN5yOf6IQaWdE0rtu-Q1'
        deviceToken = to

        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'key=' + serverToken,
        }

        temp = re.compile(r"https://vnusoftware.com/iclaimmax/assets.*").search(body)
        if temp is not None:
            file_url = temp.group()
            body = body.replace(file_url, '')


        body = {
            'notification': {'title': title,
                             'body': body,
                             'click_action': file_url,
                             'icon': "https://vnusoftware.com/assets/img/logo.jpg"
                             },
            'to': deviceToken,
            'priority': 'high',

        }
        response = requests.post("https://fcm.googleapis.com/fcm/send", headers=headers, data=json.dumps(body))
        log_custom_data(filename="file_urls", url=file_url, body=body)
        if response.status_code == 200:
            temp = response.json()
            if temp["failure"] == 1:
                return False, response.status_code, message_id
            message_id = temp['results'][0]['message_id']
            return True, response.status_code, message_id
        return False, response.status_code, message_id
        # print(response.status_code)
        # print(response.json())
        # 200
        # {'multicast_id': 7768132433662919541, 'success': 1, 'failure': 0, 'canonical_ids': 0,
        #  'results': [{'message_id': '0:1601379275358812%cb94bd62cb94bd62'}]}
    except:
        log_exceptions()
        return False, "", message_id


def send_sms(mobile_no, body):
    try:
        headers = {
            'authkey': '167826ARvnR1lKl5cee8065',
            'content-type': "application/json"
        }
        API_ENDPOINT = "https://api.msg91.com/api/v2/sendsms"
        data = {
            "sender": "MAXPPG",
            "route": "4",
            "country": "91",
            "sms": [
                {
                    "message": body,
                    "to": [
                        mobile_no
                    ]
                }
            ]
        }
        r = requests.post(url=API_ENDPOINT, data=json.dumps(data), headers=headers)
        log_custom_data(filename="sms_body", no=mobile_no, body=body)
        return r.status_code
    except:
        log_exceptions()
    pass
if __name__ == "__main__":
    send_sms('7709698773', 'hi')
