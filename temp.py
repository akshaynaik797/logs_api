import requests

url = 'http://3.7.8.68:9982/api/get_from_name1'
myobj = {'hospital_id': '8900080427990', 'insid': 'I14', 'name': 'n'}
x = requests.post(url, data = myobj)
a = x.json()
pass