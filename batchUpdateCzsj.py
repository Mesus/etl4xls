# -*- coding: utf-8 -*-
import psycopg2

# 数据库参数
db_uid = 'farming'
db_username = 'postgres'
db_password = 'bXlZcWw2'
db_host = '10.0.100.128'

conn = psycopg2.connect(database=db_uid, user=db_username, password=db_password, host=db_host, port="5432")

sSql = "select t1.id,t1.insert_time from mrcsb.b2mcczb t1"
cur = conn.cursor()
cur.execute(sSql)
for row in cur.fetchall():
    sCZSJ = row[1][0:4]+'-'+row[1][4:6]+'-'+row[1][6:8]
    # print sCZSJ
    uSql = "update mrcsb.b2mcczb set czsj='%s' where id='%s'"%(sCZSJ,row[0])
    # print uSql
    cur.execute(uSql)
conn.commit()
conn.close()