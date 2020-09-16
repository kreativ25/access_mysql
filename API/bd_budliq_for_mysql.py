import pyodbc as pd
import datetime as dt
import pymysql as pm
import mysql_connnection as mcon

# текущая и вчерашняя даты
tekDate = dt.datetime.date(dt.datetime.now())
date_1 = dt.datetime.date(dt.datetime.now() - dt.timedelta(days=1))

# Блок подключения к БД MySQL
connection = pm.connect(host=mcon.host,
                        user=mcon.user,
                        password=mcon.password,
                        db=mcon.db,
                        charset=mcon.charset,
                        cursorclass=mcon.cursorclass)

# блок подключения к БД Бюджета
driver = mcon.driver

con = pd.connect(driver)
cursor = con.cursor()

# запрос с БД Бюджета - максимальная дата
bd_max_date = cursor.execute("select max(DATE_BL) as D from BUDLIQ_DATA").fetchone()
bd_max_date.D = dt.datetime.date(bd_max_date.D)


# запрос к MySQL - максимальная дата
cur = connection.cursor()
cur.execute("select max(DATE_BL) as D_M, max(TS) as TS  from db_budliq")
m_max_date = cur.fetchone()
connection.commit()

if bd_max_date.D > m_max_date['D_M'] and dt.datetime.date(dt.datetime.now()) != dt.datetime.date(m_max_date['TS']):

    term = 1
    status = 'факт'
    data_access = cursor.execute("SELECT * FROM BUDLIQ_DATA WHERE DATE_BL > ? and TERM = ? and STATUS = ?", (m_max_date['D_M'], term, status)).fetchall()

    for x in data_access:
        DATE_BL = x.DATE_BL
        DATE_BL = dt.datetime.date(DATE_BL)

        DATE_SAVED = x.DATE_SAVED
        DATE_SAVED = dt.datetime.date(DATE_SAVED)

        TIME_SAVED = x.TIME_SAVED
        TIME_SAVED = dt.datetime.time(TIME_SAVED)

        INDEX_BL = x.INDEX_BL
        VALUE_BL = x.VALUE_BL
        TS = dt.datetime.now()

        cur = connection.cursor()
        cur.execute("INSERT INTO db_budliq (DATE_BL, DATE_SAVED, TIME_SAVED, INDEX_BL, VALUE_BL, TS) VALUES (%s, %s, %s, %s, %s, %s)", (DATE_BL, DATE_SAVED, TIME_SAVED, INDEX_BL, VALUE_BL, TS))
        connection.commit()

        print(DATE_BL)
print('данные записаны')
