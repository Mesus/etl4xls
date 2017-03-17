#coding:utf8
import psycopg2
db_uid = 'postgres'
db_username = 'postgres'
db_password = 'bXlZcWw2'
db_host = '10.0.100.128'
conn = psycopg2.connect(database=db_uid, user=db_username, password=db_password, host=db_host, port="5432")
def generate_sql(sheet_28):
    for row in range(sheet_28.nrows):
        row = ','.join(map(str, sheet_28.row_values(row)))
        row_list = row.split(",")
        #品名代码
        sPMDM = row_list[0][:4]
        try:
            int(sPMDM)
        except:
            continue
        if len(sPMDM) == 0:
            continue
        #品名
        p = query_pm(sPMDM)
        sPM = p[0]
        #品类
        sPL = p[1]
        #明日出菜量
        try:
            dot = row_list[3].strip().find('.')
            fMRCC = float(row_list[3][0:dot + 4])
        except:
            fMRCC = 0.0
        #后日出菜量
        try:
            dot = row_list[4].strip().find('.')
            fHRCC = float(row_list[4][0:dot + 4])
        except:
            fHRCC = 0.0
        #第三日出菜量
        try:
            dot = row_list[5].strip().find('.')
            third_day = float(row_list[5][0:dot + 4])
        except:
            third_day = 0.0
        #第四日出菜量
        try:
            dot = row_list[6].strip().find('.')
            fourth_day = float(row_list[6][0:dot + 4])
        except:
            fourth_day = 0.0
        #第五日出菜量
        try:
            dot = row_list[7].strip().find('.')
            fifth_day = float(row_list[7][0:dot + 4])
        except:
            fifth_day = 0.0
        #下月出菜量
        try:
            dot = row_list[8].strip().find('.')
            next_month = float(row_list[8][0:dot + 4])
        except:
            next_month = 0.0

        iLen = fMRCC+fHRCC+third_day+fourth_day+fifth_day
        if iLen>0:
            tid = uuid.uuid1()
            sql = "INSERT INTO mrcsb.mryjcc(id,parent_id,insert_time,farm,pmdm,sheet,mrccl,dat_ccl,third_day,fourth_day,fifth_day,next_month) VALUES('%s','%s','%s','%s','%s','%s',%f,%f,%f,%f,%f,%f);" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, '预计出菜表',fMRCC,fHRCC,third_day,fourth_day,fifth_day,next_month )
def query_pm(pmdm):
    sql = "SELECT pm.pm_name,pl.pl_name FROM public.pm,public.pl WHERE pm.pl_id=pl.id AND pm.id='%s'"%(pmdm)
    cur = conn.cursor()
    cur.execute(sql)
    r = cur.fetchone()
    pm = r[0]
    pl = r[1]
    return pm,pl
if __name__=='__main__':
    query_pm('1111')