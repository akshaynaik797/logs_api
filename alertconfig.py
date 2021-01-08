import mysql.connector
from os.path import dirname,abspath
import json

dbconfig = {}
with open ( dirname ( abspath ( __file__ ) ) + "/databaseconfig.json" ) as json_data_file :
    data = json.load ( json_data_file )
    for keys in data.keys ( ) :
        values = data[keys]
        dbconfig[keys] = values

"""
mydb = mysql.connector(
  host=dbConfig["Config"]['MYSQL_HOST'],
  user=dbConfig["Config"]['MYSQL_USER'],
  password=dbConfig["Config"]['MYSQL_PASSWORD'],
  database=dbConfig["Config"]['MYSQL_DB']
)
"""
