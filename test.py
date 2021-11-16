import pyodbc
import datetime
import gzip

server = 'localhost'
username = 'SA'
password = 'Qwer1234'
location = '/media/d/backup'

TODAY = datetime.date.today()
SKIP = ['master', 'tempdb', 'model', 'msdb']

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';'
                            'DATABASE=master;UID='+username+';PWD='+ password)
cnxn.autocommit = True
cursor = cnxn.cursor()

cursor.execute("select name from sys.databases")
databases = cursor.fetchall()
cursor.close()

for database in databases:
    database_name = database[0]
    if database_name not in SKIP:
        cursor = cnxn.cursor()
        backup = "BACKUP DATABASE [" + database_name + "] TO DISK = N'" + location + "/" + database_name + '-' + str(TODAY) + ".bak'"
        # cursor.execute(backup)
        cursor.execute(f"{backup} | gzip -c --best -out {location}")
        while cursor.nextset():
            pass
        cursor.close()

cnxn.close()