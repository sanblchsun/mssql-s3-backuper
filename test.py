import pyodbc
import datetime
import gzip
import os

server = 'localhost'
username = 'SA'
password = 'Qwer1234'
location = '/media/d/backup'
DB_FILENAME = "/media/d/backup/backup_db.sql.gz.enc"
BACKUP_KEY_PUB_FILE = "backup_key.pem.pub"

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
        path = location + "/" + database_name + '-' + str(TODAY) + ".bak"
        # backup = "BACKUP DATABASE [" + database_name + "] TO DISK = N'" + path
        backup = f"BACKUP DATABASE [{database_name}] TO DISK = N'{path}' WITH FORMAT, STATS=1"
        cursor.execute(backup)
        str_cmd = f"openssl smime -encrypt -aes256 -binary -outform DEM -in {path} -out {DB_FILENAME} {BACKUP_KEY_PUB_FILE}"

        dump_db_operation_status = os.WEXITSTATUS(os.system(str_cmd))
        print(str_cmd)
        if dump_db_operation_status != 0:
            exit(f"\U00002757 Dump database command exits with status "
                 f"{dump_db_operation_status}.")
        print("\U0001F510 DB dumped, archieved and encoded")

        # dump_db_operation_status = os.WEXITSTATUS(os.system(cursor.execute(backup)))
        while cursor.nextset():
            pass
        cursor.close()

cnxn.close()