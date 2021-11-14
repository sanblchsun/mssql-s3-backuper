import pyodbc
import os
import time
import smtplib
import datetime
from email.mime.text import MIMEText
from email.utils import formatdate
from email.mime.multipart import MIMEMultipart
import subprocess


def now_time():
    hour = datetime.datetime.now().hour
    minute = datetime.datetime.now().minute
    seconds = datetime.datetime.now().second
    now = f'{hour:02}:{minute:02}:{seconds:02}'
    return now


def delta_hour_min_sec(start,finish):
    delta = finish - start
    total_sec = delta.total_seconds()
    total_hours = int(total_sec // 3600)
    total_min = int((total_sec % 3600) // 60)
    final_sec = int((total_sec % 3600) % 60)
    return f'Потребовалось {total_hours} час. {total_min:02} мин. {final_sec:02} сек.'


def send_email(subject, body_text, to_emails, cc_emails):
    # extract server and from_addr from config
    host = ''
    from_addr = ''

    # create the message
    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)

    if body_text:
        msg.attach(MIMEText(body_text))

    msg["To"] = ', '.join(to_emails)
    msg["cc"] = ', '.join(cc_emails)

    emails = to_emails + cc_emails
    server = smtplib.SMTP(host)
    server.sendmail(from_addr, emails, msg.as_string())
    server.quit()

# сохраним время старта
start = datetime.datetime.now()
backuped_base_name = input("Введите имя бэкапируемой базы: ")
restored_base_name = input("Введите имя восстанавливаемой базы: ")
# добавим адрес пользователя, для которого будет создана база
email_adrr = input("Введите адрес электронной почты: ")
# версия платформы.
ver_1c = input('Введите версию платформы в формате "8.Х.ХХ.ХХХХ": ')
# задаем путь к папке
path_backup = '\\\\server\\backup_path'
# имя файла бэкапа
file_backup = f'{backuped_base_name}.bak'
# полный путь к бэкапу
full_path = f'{path_backup}\\{file_backup}'


# проверяем наличие старого бэкпа и если он есть удаляем
if os.access(f'{full_path}', os.F_OK):
    os.remove(f'{full_path}')
    del_file_backup = f'Старый файл бэкапа удален - {now_time()}'
    print(del_file_backup)

# указываем драйвер
driver = 'DRIVER={ODBC Driver 17 for SQL Server}'
# SQL сервер
server = 'SERVER=SQL_server_name'
# указываем порт
port = 'PORT=1433'
# указываем имя бэкапируемой базы
db = f'DATABASE={backuped_base_name}'
# учетная запись, у которой есть права на backup
user = 'UID=user_backup'
# пароль для учетной записи
pw = 'PWD=password'
# соберем строку подключения к серверу
conn_str = ';'.join([driver, server, port, db, user, pw])
# подключаемся к базе
base_conn = pyodbc.connect(conn_str)
connect_to_base = f'Подключились к базе {backuped_base_name} для создания бэкапа - {now_time()}'
print(connect_to_base)

cursor = base_conn.cursor()
base_conn.autocommit = True

# создаем бэкап
start_create_backup = f'Начали создание бэкапа - {now_time()}'
print(start_create_backup)
# вводим переменную для "красивого" отображения процентов
stats = -2
# выполняем команду для создания бэкапа
cursor.execute(f"BACKUP DATABASE [{backuped_base_name}] TO DISK = N'{full_path}' WITH COMPRESSION, COPY_ONLY, STATS=1")
# получаем ответ от сервера SQL и оповещаем о статусе выполнения
while cursor.nextset():
    stats += 1
    if stats > 0:
        print(f'Выполненно {stats}% - {now_time()}')
    pass
finish_create_backup = f'Создание бэкапа завершено - {now_time()}'
print(finish_create_backup)

# выполняем запрос на получение логических имен файлов
cursor.execute('select name from sys.database_files')
# получаем список кортежей с логическими именами файлов(первый кортеж файл базы, второй - лога)
logical_name_files = cursor.fetchall()
# задаем список для имен
logical_name_files_list = []
# перебираем список кортежей и добавляем логические имена файлов
for logical_name in logical_name_files:
    logical_name_files_list.append(logical_name[0])
# задаем переменные для логических имен файлов
logical_backuped_base_name = logical_name_files_list[0]
logical_backuped_log_name = logical_name_files_list[1]

# выполняем запрос
cursor.execute('SELECT name FROM sys.databases')
# получаем в список кортежей, где на первой позиции, в каждом из кортежей имя базы
databases = cursor.fetchall()
# список для баз
databases_list = []
# перебираем список картежей и добавляем имена баз в список databases_list
for base in databases:
    databases_list.append(base[0])

if restored_base_name in databases_list:
    # выводим оповещения об устанавливке SINGLE_USER
    single_user = f'Устанавливаем SINGLE_USER - {now_time()}'
    print(single_user)
    # выполняем запрос на установку SINGLE_USER
    cursor.execute(f'ALTER DATABASE [{restored_base_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE')
    # выводим оповещение об удалении базы
    print(f'Удаляем базу {restored_base_name} - {now_time()}')
    # удаляем базу
    cursor.execute(f'use master DROP DATABASE [{restored_base_name}]')
    time.sleep(20)
else:
    # если база не обнаружена оповещаем об этом
    print(f'База с именем {restored_base_name} не обнаружена приступаем к восстановлению - {now_time()}')

# оповещаем о начале восстановления базы
restore_database = f'Начали восстановление базы {restored_base_name} в - {now_time()}'
print(restore_database)
# устанавливаем режим автосохранения транзакций
base_conn.autocommit = True
# вводим переменную для "красивого" отображения процентов
stats = -2
# выполняем запрос
cursor.execute(f"RESTORE DATABASE [{restored_base_name}] FROM  DISK = N'{full_path}' WITH  FILE = 1, MOVE N'{logical_backuped_base_name}' TO N'D:\\MSSQL\\DATA\\{restored_base_name}.mdf', MOVE N'{logical_backuped_log_name}' TO N'D:\\MSSQL\\DATA\\{restored_base_name}.ldf', NOUNLOAD, STATS=1")
# получаем ответ от сервера SQL и оповещаем о статусе выполнения
while cursor.nextset():
    stats += 1
    if stats > 0:
        print(f'Выполненно {stats}% - {now_time()}')
    pass
base_conn.close()

# задаем имя сервера
server = '1c_server_name'
# порт RAS
ras_port = '1545'
# создаем базу в кластере 1С
create_db_cmd = f'"C:\\Program Files (x86)\\1cv8\\{ver_1c}\\bin\\1cv8.exe" CREATEINFOBASE Srvr="{server}";Ref="{restored_base_name}";DBMS=MSSQLServer;DBSrvr="SQL_server_name";DB={restored_base_name};DBUID="";DBPwd="";CrSQLDB="n";SchJobDn="Y"'
process = subprocess.Popen(create_db_cmd)


finish = datetime.datetime.now()
time_delta = delta_hour_min_sec(start=start, finish=finish)
full_time = f'Потребовалось {time_delta}'
print(full_time)


if __name__ == "__main__":
    emails = ["Admins@mailserver.ru", f"{email_adrr}"]
    cc_emails = []# здесь можно добавить кого-нибудь в копию
    bcc_emails = [] # здесь скрытая копия

    subject = f"База {restored_base_name} восстановлена из {backuped_base_name}"
    body_text = f'''База {restored_base_name} восстановлена из {backuped_base_name} и добвалена в кластер 1С.
    Отчет:
        1. {connect_to_base}.
        2. {start_create_backup}.
        3. {finish_create_backup}.
        4. {restore_database}.
        5. {full_time}'''
    send_email(subject, body_text, emails, cc_emails)