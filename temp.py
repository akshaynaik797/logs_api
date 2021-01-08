import requests
data = {}
request2 = requests.post("http://localhost:9980/get_hospital_db_info", data=data)
a = request2.json()
pass