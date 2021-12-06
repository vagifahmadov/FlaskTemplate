from setting.config import *
# apps.config['MONGO_URI'] = 'mongodb://security:Aze1234@ds137508.mlab.com:37508/security?retryWrites=false'
apps.config['MONGO_URI'] = 'mongodb://localhost:27017/test_base'

con_mssql = pyodbc.connect('DRIVER={SQL Server};SERVER=192.168.75.246;DATABASE=NEW_QMS;UID=AppUserTest;PWD=As!@an#$SSSa123')
con_mysql = pymysql.connect(host="localhost", user="root", password="", database="test")

mongo = PyMongo(apps)
mysql = con_mysql.cursor()
mssql = con_mssql.cursor()
