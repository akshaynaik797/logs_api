import requests

def grouping_ins_hosp():
    url = 'http://3.7.8.68:9980/get_hospitaltlog'
    myobj = {}
    x = requests.post(url, data = myobj)
    data = x.json()
    data_dict = dict()
    data_set = set()
    for i in data:
        data_set.add((i['HospitalID'], i['insurerID']))
    for i, j in data_set:
        data_dict[i+','+j] = []
        for record in data:
            if record['HospitalID'] == i and record['insurerID'] == j:
                data_dict[i + ',' + j].append(record)
    return data_dict

if __name__ == '__main__':
    a = grouping_ins_hosp()