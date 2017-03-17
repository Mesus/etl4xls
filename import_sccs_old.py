# -*- coding: utf-8 -*-
import xlrd
import psycopg2
# import string
from ftplib import FTP
import sys,os, shutil, time, uuid
# from decimal import Decimal
import logging
import logging.config

reload(sys)
sys.setdefaultencoding("utf-8")
# 本地临时目录
# tmpdir = '/root/etl/etl/tmp'
tmpdir = '/home/vicent/PycharmProjects/ETL/tmp'
# ftp参数
ftp_ip = '10.0.100.134'
ftp_username = 'hanzhiwei'
ftp_password = '123456'
ftp_path = '/'
# 数据库参数
db_uid = 'farming'
db_username = 'postgres'
db_password = 'bXlZcWw2'
db_host = '10.0.100.128'
conn = psycopg2.connect(database=db_uid, user=db_username, password=db_password, host=db_host, port="5432")
# 过滤通过文件列表
list = []
# Sql statement
sql_l = []
# logging.config.fileConfig("/root/etl/etl/src/logger.conf")
logging.config.fileConfig("logger.conf")
logger = logging.getLogger("root")


# 过滤文件
def ls_filter(line):
    info_arr = line.split()
    # cur_mon = time.strftime('%b', time.localtime(time.time()))  # 当前系统月份
    # if info_arr[5] == cur_mon:  # 只接受当前月份的文件
    sFN = info_arr[8]  # 文件名称
    sPrefix = sFN[-3:]  # 扩展名
    if "xls" in sPrefix:  # 只接受xls文件
        if "每日采收表" in sFN:  # 上报类型
            sFileDate = sFN[-29:22]  # 截取文件名中的日期
            # print sFileDate
            if is_valid_date(sFileDate):  # 校验文件名-日期是否合法
                # logger.info(sFN)
                if q_filename(sFN) == 0:
                    list.append(sFN)
def q_filename(fn):
    # print "\'"
    sql = "select count(*) from mrcsb.originaldata where filename~*\'"+fn+"\'"
    # print sql
    cur = conn.cursor()
    cur.execute(sql)
    r = cur.fetchone()
    return r[0]

# 日期合法性校验
def is_valid_date(str):
    try:
        time.strptime(str, "%Y%m%d%H")
        return True
    except:
        return False


def ftp_down():
    logger.info('同步率100%，正在进行连接,曲线正常，LCL浓度正常')
    ftp = FTP()
    timeout = 30
    port = 22
    ftp.connect(ftp_ip, port, timeout)  # 连接FTP服务器
    ftp.login(ftp_username, ftp_password)  # 登录
    print ftp.getwelcome()  # 获得欢迎信息
    ftp.cwd(ftp_path)  # 设置FTP路径
    # list = ftp.nlst()       # 获得目录列表
    fs = ftp.retrlines('LIST', ls_filter)  # 快速获取文件列表
    for filename in list:  # 下载过滤成功的文件
        logger.info("Download: " + filename)
        # print ftp.getwelcome()#显示ftp服务器欢迎信息
        # ftp.cwd('xxx/xxx/') #选择操作目录
        bufsize = 1024
        local = os.path.join(tmpdir, filename)
        file_handler = open(local, 'wb').write  # 以写模式在本地打开文件
        ftp.retrbinary('RETR %s' % filename, file_handler, bufsize)  # 接收服务器上文件并写入本地文件
        ftp.set_debuglevel(0)
        # file_handler.close()
    ftp.quit()
    print "ftp down OK"


# 遍历本地临时目录
def traverse():
    for parent, dirnames, filenames in os.walk(tmpdir):
        for filename in filenames:
            tMetaData = make_originaldata(filename)
            sFullFile = os.path.join(parent, filename)
            logger.info('Local temp file:' + sFullFile)
            readFile(sFullFile, tMetaData)


# 文件信息解析为SQL
def make_originaldata(fn):
    # File date
    insert_time = fn[-29:22]
    # File farm
    iTag = fn.find(u'农场')
    # Substring farm
    farm = fn[0:iTag + iTag * 2]
    print fn
    tid = uuid.uuid1()
    sql = "INSERT INTO mrcsb.originaldata(id,insert_time,farm,filename) VALUES('%s','%s','%s','%s');" % (
    tid, insert_time, farm, fn)
    logger.info('第一锁定器解除')
    logger.info("File info sql:" + sql)
    sql_l.append(sql)
    return (tid, insert_time, farm, fn)


def readFile(filename, metadata):
    data = xlrd.open_workbook(filename, 'rb')  # Open file
    # iSheetNum = len(data.sheets())
    # i=1
    # for sheet in data.sheets():
    # print 'Sheet:',sheet.name
    # if(i < iSheetNum):
    # for row in range(sheet.nrows):
    # print(','.join(map(str, sheet.row_values(row))))
    # i+=1
    sheet_21 = data.sheet_by_name(u'表2.1毛菜采摘表')
    for row in range(sheet_21.nrows):
        # print(','.join(map(str, sheet_21.row_values(row))))
        row = ','.join(map(str, sheet_21.row_values(row)))
        row_list = row.split(",")
        sPMDM = row_list[1][:4]
        if len(sPMDM) == 0:
            continue
        sDKH = row_list[5]  # 地块号
        sCZSJ = row_list[6]  # 采摘时间
        sCZR = row_list[7]  # 采摘人
        sCZSL = row_list[8]  # 采摘数量
        sCCJGSL = row_list[9]  # 秸秆/残菜数量
        try:
            # sVAL_1 = float(sVAL_1)
            dot = sCZSL.strip().find('.')
            fCZSL = float(sCZSL[0:dot + 4])
        except:
            fCZSL = 0.0
        try:
            # sVAL_2 = float(sVAL_2)
            dot = sCCJGSL.strip().find('.')
            fCCJGSL = float(sCCJGSL[0:dot + 4])
        except:
            fCCJGSL = 0.0
        iLen = fCZSL+fCCJGSL
        if iLen > 0:
            # print(row)
            # sPMDM = row_list[1][:4]
            # print sPMDM
            if sPMDM.find("品") == -1:
                if qforpmdm(sPMDM) == True:
                    tid = uuid.uuid1()
                    sql = "INSERT INTO mrcsb.b2mcczb(id,parent_id,insert_time,farm,sheet,pmdm,dkh,czsj,czr,mcczl,jgccsl) VALUES('%s','%s','%s','%s','%s','%s','%s','%s','%s',%f,%f);" % (
                    tid, metadata[0], metadata[1], metadata[2], metadata[3], sPMDM, sDKH.strip(), sCZSJ.strip(),
                    sCZR.strip(), fCZSL, fCCJGSL)
                    sql_l.append(sql)
                    logger.info(sql)
                    # print sql
            else:
                logger.info('第二锁定器解除')
    sheet_22 = data.sheet_by_name(u'表2.2精品菜加工表')
    for row in range(sheet_22.nrows):
        row = ','.join(map(str, sheet_22.row_values(row)))
        row_list = row.split(",")
        sPMDM = row_list[0][:4]
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[3]  # 精品菜1数量
        sVAL_2 = row_list[4]  # 精品菜2数量
        sVAL_3 = row_list[5]  # 精品菜3数量
        sVAL_4 = row_list[6]  # 精品菜加工总数量：kg
        try:
            # sVAL_1 = float(sVAL_1)
            dot = sVAL_1.strip().find('.')
            fVAL_1 = float(sVAL_1[0:dot + 4])
        except:
            fVAL_1 = 0.0
        try:
            # sVAL_2 = float(sVAL_2)
            dot = sVAL_2.strip().find('.')
            fVAL_2 = float(sVAL_2[0:dot + 4])
        except:
            fVAL_2 = 0.0
        try:
            if len(sVAL_3) > 0:
                dot = sVAL_3.strip().find('.')
                fVAL_3 = float(sVAL_3[0:dot + 4])
            else:
                fVAL_3 = 0.0
        except:
            fVAL_3 = 0.0
        try:
            # fVAL_4 = float(sVAL_4)
            dot = sVAL_4.strip().find('.')
            fVAL_4 = float(sVAL_4[0:dot + 4])
        except:
            fVAL_4 = 0.0
        iLen = fVAL_1+fVAL_2+fVAL_3+fVAL_4
        #print iLen
        if iLen > 0:
            #print(row)
            # sPMDM = row_list[0][:4]
            # print sPMDM
            # if sPMDM.find("品") == -1:
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.b2mrcsb(uid,parent_id,insert_time,farm,pmdm,sheet,jpcjg_1,jpcjg_2,jpcjg_3,jpcjgsl) VALUES('%s','%s','%s','%s','%s',%d,%f,%f,%f,%f);" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, 2, fVAL_1, fVAL_2, fVAL_3, fVAL_4)
                sql_l.append(sql)
                logger.info(sql)
                    # print ''.join(sql)
            # else:
            #     logger.info('第三锁定器解锁10%')
            #     print '第三锁定器解锁10%'
    sheet_23 = data.sheet_by_name(u'表2.3商品菜加工表')
    for row in range(sheet_23.nrows):
        row = ','.join(map(str, sheet_23.row_values(row)))
        row_list = row.split(",")
        sPMDM = row_list[0][:4]
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[3]  # 商品菜加工数量：kg
        sVAL_2 = row_list[4]  # 残菜数量：kg
        try:
            if len(sVAL_1) > 0:
                dot = sVAL_1.strip().find('.')
                fVAL_1 = float(sVAL_1[0:dot + 4])
            else:
                fVAL_1 = 0.0
        except:
            fVAL_1 = 0.0
        try:
            if len(sVAL_2) > 0:
                dot = sVAL_2.strip().find('.')
                fVAL_2 = float(sVAL_2[0:dot + 4])
            else:
                fVAL_2 = 0.0
        except:
            fVAL_2 = 0.0
        # print "商品菜加工数量%d"%fVAL_1
        iLen = fVAL_1+fVAL_2
        #print iLen
        if iLen > 0:
            # print(row)
            # sPMDM = row_list[0][:4]
            # print sPMDM
            # if sPMDM.find("品") == -1:
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.b2mrcsb(uid,parent_id,insert_time,farm,pmdm,sheet,spcjgsl,spcjg_ccsl) VALUES('%s','%s','%s','%s','%s',%d,%f,%f);" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, 3, fVAL_1, fVAL_2)
                sql_l.append(sql)
                logger.info(sql)
                # print ''.join(sql)
            # else:
            #     logger.info('第三锁定器解锁20%')
            #     print '第三锁定器解锁20%'
    sheet_24 = data.sheet_by_name(u'表2.4精品菜总库存表')
    for row in range(sheet_24.nrows):
        row = ','.join(map(str, sheet_24.row_values(row)))
        row_list = row.split(",")
        sPMDM = row_list[0][:4]
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[3]  # 上日库存：kg
        sVAL_2 = row_list[4]  # 当日本地精品菜入库：kg
        sVAL_3 = row_list[5]  # 当日外埠精品菜调拨入库：kg
        sVAL_4 = row_list[6]  # 当日本地精品菜出库：kg
        sVAL_5 = row_list[7]  # 当日精品菜总库存量：kg
        try:
            dot = sVAL_1.strip().find('.')
            fVAL_1 = float(sVAL_1[0:dot + 4])
        except:
            fVAL_1 = 0.0
        try:
            dot = sVAL_2.strip().find('.')
            fVAL_2 = float(sVAL_2[0:dot + 4])
        except:
            fVAL_2 = 0.0
        try:
            if len(sVAL_3) > 0:
                dot = sVAL_3.strip().find('.')
                fVAL_3 = float(sVAL_3[0:dot + 4])
            else:
                fVAL_3 = 0.0
        except:
            fVAL_3 = 0.0
        try:
            dot = sVAL_4.strip().find('.')
            fVAL_4 = float(sVAL_4[0:dot + 4])
        except:
            fVAL_4 = 0.0
        try:
            dot = sVAL_5.strip().find('.')
            fVAL_5 = float(sVAL_5[0:dot + 4])
        except:
            fVAL_5 = 0.0
        iLen = fVAL_1+fVAL_2+fVAL_3+fVAL_4+fVAL_5
        # print iLen
        if iLen > 0:
            # print(row)
            # sPMDM = row_list[0][:4]
            # print sPMDM
            # if sPMDM.find("品") == -1:
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.b2mrcsb(uid,parent_id,insert_time,farm,pmdm,sheet,jpczkc_1,jpczkc_2,jpczkc_3,jpczkc_4,jpczkc) VALUES('%s','%s','%s','%s','%s',%d,%f,%f,%f,%f,%f);" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, 4, fVAL_1, fVAL_2, fVAL_3, fVAL_4, fVAL_5)
                sql_l.append(sql)
                logger.info(sql)
                    # print ''.join(sql)
            # else:
            #     logger.info('第三锁定器解锁30%')
            #     print '第三锁定器解锁30%'
    sheet_241 = data.sheet_by_name(u'表2.4.1精品菜调拨入库表')
    for row in range(sheet_241.nrows):
        row = ','.join(map(str, sheet_241.row_values(row)))
        row_list = row.split(",")
        sPMDM = row_list[1][:4]
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[4]  # 当日外埠调拨精品菜入库：kg
        sVAL_2 = row_list[5]  # 当日外埠精品菜调拨来源
        try:
            if len(sVAL_1) > 0:
                dot = sVAL_1.strip().find('.')
                fVAL_1 = float(sVAL_1[0:dot + 4])
            else:
                fVAL_1 = 0.0
        except:
            fVAL_1 = 0.0
        # print fVAL_1
        if fVAL_1 > 0:
            # print(row)
            # sPMDM = row_list[1][:4]
            # if sPMDM.find("品") == -1:
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.b2mrcsb(uid,parent_id,insert_time,farm,pmdm,sheet,jpcdbrk_1,jpcdbrk_2) VALUES('%s','%s','%s','%s','%s',%d,%f,'%s');" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, 5, fVAL_1, sVAL_2)
                sql_l.append(sql)
                logger.info(sql)
                    # print ''.join(sql)
            # if sPMDM.find("品") == 1:
            #     logger.info('第三锁定器解锁40%')
            #     print '第三锁定器解锁40%'
    sheet_25 = data.sheet_by_name(u'表2.5商品菜总库存表')
    for row in range(sheet_25.nrows):
        row = ','.join(map(str, sheet_25.row_values(row)))
        row_list = row.split(",")
        sPMDM = row_list[0][:4]
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[3]  # 上日商品菜库存：kg
        sVAL_2 = row_list[4]  # 当日本地商品菜入库：kg
        sVAL_3 = row_list[5]  # 当日外埠调拨商品菜入库：kg
        sVAL_4 = row_list[6]  # 当日本地商品菜出库：kg
        sVAL_5 = row_list[7]  # 当日商品菜总库存量：kg
        try:
            dot = sVAL_1.strip().find('.')
            fVAL_1 = float(sVAL_1[0:dot + 4])
        except:
            fVAL_1 = 0.0
        try:
            dot = sVAL_2.strip().find('.')
            fVAL_2 = float(sVAL_2[0:dot + 4])
        except:
            fVAL_2 = 0.0
        try:
            if len(sVAL_3) > 0:
                dot = sVAL_3.strip().find('.')
                fVAL_3 = float(sVAL_3[0:dot + 4])
            else:
                fVAL_3 = 0.0
        except:
            fVAL_3 = 0.0
        try:
            dot = sVAL_4.strip().find('.')
            fVAL_4 = float(sVAL_4[0:dot + 4])
        except:
            fVAL_4 = 0.0
        try:
            dot = sVAL_5.strip().find('.')
            fVAL_5 = float(sVAL_5[0:dot + 4])
        except:
            fVAL_5 = 0.0
        iLen = fVAL_1+fVAL_2+fVAL_3+fVAL_4+fVAL_5
        # print iLen
        if iLen > 0:
            # print(row)
            # sPMDM = row_list[0][:4]
            # print sPMDM
            # if sPMDM.find("品") == -1:
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.b2mrcsb(uid,parent_id,insert_time,farm,pmdm,sheet,spczkc_1,spczkc_2,spczkc_3,spczkc_4,spczk) VALUES('%s','%s','%s','%s','%s',%d,%f,%f,%f,%f,%f);" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, 6, fVAL_1, fVAL_2, fVAL_3, fVAL_4, fVAL_5)
                sql_l.append(sql)
                logger.info(sql)
    sheet_251 = data.sheet_by_name(u'表2.5.1商品菜调拨入库表')
    for row in range(sheet_251.nrows):
        row = ','.join(map(str, sheet_251.row_values(row)))
        row_list = row.split(",")
        sPMDM = row_list[1][:4]
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[4]  # 当日外埠调拨商品菜入库：kg
        sVAL_2 = row_list[5]  # 当日外埠商品菜调拨来源
        try:
            if len(sVAL_1) > 0:
                dot = sVAL_1.strip().find('.')
                fVAL_1 = float(sVAL_1[0:dot + 4])
            else:
                fVAL_1 = 0.0
        except:
            fVAL_1 = 0.0
        # print fVAL_1
        if fVAL_1 > 0:
            # print(row)
            # if sPMDM == '1001':
                # print '表2.5.1商品菜调拨入库表'
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.b2mrcsb(uid,parent_id,insert_time,farm,pmdm,sheet,spcdbrk_1,spcdbrk_2) VALUES('%s','%s','%s','%s','%s',%d,%f,'%s');" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, 7, fVAL_1, sVAL_2)
                sql_l.append(sql)
                logger.info(sql)
    sheet_26 = data.sheet_by_name(u'表2.6精品菜出库表')
    for row in range(sheet_26.nrows):
        row = ','.join(map(str, sheet_26.row_values(row)))
        # print row
        row_list = row.split(",")
        sPMDM = row_list[0][:4]
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[3]  # 精品采摘菜出库：kg
        sVAL_2 = row_list[4]  # 精品团购菜出库：kg
        sVAL_3 = row_list[5]  # 调拨精品菜出库：kg
        sVAL_4 = row_list[6]  # 精品菜出库总量：kg
        try:
            # sVAL_1 = float(sVAL_1)
            dot = sVAL_1.strip().find('.')
            fVAL_1 = float(sVAL_1[0:dot + 4])
        except:
            fVAL_1 = 0.0
        try:
            # sVAL_2 = float(sVAL_2)
            dot = sVAL_2.strip().find('.')
            fVAL_2 = float(sVAL_2[0:dot + 4])
        except:
            fVAL_2 = 0.0
        try:
            if len(sVAL_3) > 0:
                dot = sVAL_3.strip().find('.')
                fVAL_3 = float(sVAL_3[0:dot + 4])
            else:
                fVAL_3 = 0.0
        except:
            fVAL_3 = 0.0
        try:
            # fVAL_4 = float(sVAL_4)
            dot = sVAL_4.strip().find('.')
            fVAL_4 = float(sVAL_4[0:dot + 4])
        except:
            fVAL_4 = 0.0
        iLen = fVAL_1+fVAL_2+fVAL_3+fVAL_4
        # print "::%f,%f,%f,%f"%(fVAL_1,fVAL_2,fVAL_3,fVAL_4)
        if iLen > 0:
            # print(row)
            # print sPMDM
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.b2mrcsb(uid,parent_id,insert_time,farm,pmdm,sheet,jpcck_1,jpcck_2,jpcck_3,jpccksl) VALUES('%s','%s','%s','%s','%s',%d,%f,%f,%f,%f);" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, 8, fVAL_1, fVAL_2, fVAL_3, fVAL_4)
                sql_l.append(sql)
                logger.info(sql)
                # print sql
    sheet_261 = data.sheet_by_name(u'表2.6.1精品菜团购出库表')
    for row in range(sheet_261.nrows):
        row = ','.join(map(str, sheet_261.row_values(row)))
        row_list = row.split(",")
        sPMDM = row_list[1][:4]
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[4]  # VIP菜数量：kg
        sVAL_2 = row_list[5]  # VIP菜用户单位
        sVAL_3 = row_list[6]  # 餐饮菜数量：kg
        sVAL_4 = row_list[7]  # 餐饮菜用户单位
        sVAL_5 = row_list[8]  # 礼品菜数量：kg
        sVAL_6 = row_list[9]  # 礼品菜用户单位
        try:
            dot = sVAL_1.strip().find('.')
            fVAL_1 = float(sVAL_1[0:dot + 4])
        except:
            fVAL_1 = 0.0
        try:
            dot = sVAL_3.strip().find('.')
            fVAL_3 = float(sVAL_3[0:dot + 4])
        except:
            fVAL_3 = 0.0
        try:
            dot = sVAL_5.strip().find('.')
            fVAL_5 = float(sVAL_5[0:dot + 4])
        except:
            fVAL_5 = 0.0
        iLen = fVAL_1+fVAL_3+fVAL_5
        # print iLen
        if iLen > 0:
            # print(row)
            # print sPMDM
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.b2mrcsb(uid,parent_id,insert_time,farm,pmdm,sheet,jpctgck_1, jpctgck_2, jpctgck_3, jpctgck_4, jpctgck_5, jpctgck_6) VALUES('%s','%s','%s','%s','%s',%d,%f,'%s',%f,'%s',%f,'%s');" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, 9, fVAL_1, sVAL_2, fVAL_3, sVAL_4, fVAL_5,sVAL_6)
                sql_l.append(sql)
                logger.info(sql)
    sheet_262 = data.sheet_by_name(u'表2.6.2精品菜调拨出库表')
    for row in range(sheet_262.nrows):
        row = ','.join(map(str, sheet_262.row_values(row)))
        row_list = row.split(",")
        sPMDM = row_list[1][:4]
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[4]  # 调拨数量：kg
        sVAL_2 = row_list[5]  # 调拨方向
        try:
            if len(sVAL_1) > 0:
                dot = sVAL_1.strip().find('.')
                fVAL_1 = float(sVAL_1[0:dot + 4])
            else:
                fVAL_1 = 0.0
        except:
            fVAL_1 = 0.0
        # print fVAL_1
        if fVAL_1 > 0:
            # print(row)
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.b2mrcsb(uid,parent_id,insert_time,farm,pmdm,sheet,jpcdbck_1, jpcdbck_2) VALUES('%s','%s','%s','%s','%s',%d,%f,'%s');" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, 10, fVAL_1, sVAL_2)
                sql_l.append(sql)
                logger.info(sql)
    sheet_27 = data.sheet_by_name(u'表2.7商品菜出库表')
    for row in range(sheet_27.nrows):
        row = ','.join(map(str, sheet_27.row_values(row)))
        # print row
        row_list = row.split(",")
        sPMDM = row_list[0][:4]
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[3]  # 当日商品菜出库：kg
        sVAL_2 = row_list[4]  # 当日调拨商品菜出库：kg
        sVAL_3 = row_list[5]  # 当日商品菜出库总量：kg
        try:
            # sVAL_1 = float(sVAL_1)
            dot = sVAL_1.strip().find('.')
            fVAL_1 = float(sVAL_1[0:dot + 4])
        except:
            fVAL_1 = 0.0
        try:
            # sVAL_2 = float(sVAL_2)
            dot = sVAL_2.strip().find('.')
            fVAL_2 = float(sVAL_2[0:dot + 4])
        except:
            fVAL_2 = 0.0
        try:
            if len(sVAL_3) > 0:
                dot = sVAL_3.strip().find('.')
                fVAL_3 = float(sVAL_3[0:dot + 4])
            else:
                fVAL_3 = 0.0
        except:
            fVAL_3 = 0.0
        iLen = fVAL_1+fVAL_2+fVAL_3
        if iLen > 0:
            # print(row)
            # print sPMDM
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.b2mrcsb(uid,parent_id,insert_time,farm,pmdm,sheet,spcck_1, spcck_2, spccksl) VALUES('%s','%s','%s','%s','%s',%d,%f,%f,%f);" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, 11, fVAL_1, fVAL_2, fVAL_3)
                sql_l.append(sql)
                logger.info(sql)
    sheet_271 = data.sheet_by_name(u'表2.7.1商品菜调拨出库表')
    for row in range(sheet_271.nrows):
        row = ','.join(map(str, sheet_271.row_values(row)))
        row_list = row.split(",")
        sPMDM = row_list[1][:4]
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[4]  # 调拨数量：kg
        sVAL_2 = row_list[5]  # 调拨方向
        try:
            if len(sVAL_1) > 0:
                dot = sVAL_1.strip().find('.')
                fVAL_1 = float(sVAL_1[0:dot + 4])
            else:
                fVAL_1 = 0.0
        except:
            fVAL_1 = 0.0
        # print fVAL_1
        if fVAL_1 > 0:
            # print(row)
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.b2mrcsb(uid,parent_id,insert_time,farm,pmdm,sheet,spcdbck_1, spcdbck_2) VALUES('%s','%s','%s','%s','%s',%d,%f,'%s');" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, 12, fVAL_1, sVAL_2)
                sql_l.append(sql)
                logger.info(sql)
    sheet_28 = data.sheet_by_name(u'表2.8预计出菜表')
    for row in range(sheet_28.nrows):
        row = ','.join(map(str, sheet_28.row_values(row)))
        row_list = row.split(",")
        sPMDM = row_list[0][:4]
        try:
            int(sPMDM)
        except:
            continue
        if len(sPMDM) == 0:
            continue
        sVAL_1 = row_list[3]  # 明日出菜量：kg
        # print '明日出菜量'
        sVAL_2 = row_list[4]  # 下周一
        sVAL_3 = row_list[5]  # 下周二
        sVAL_4 = row_list[6]  # 下周三
        sVAL_5 = row_list[7]  # 下周四
        sVAL_6 = row_list[8]  # 下周五
        sVAL_7 = row_list[9]  # 下周六
        sVAL_8 = row_list[10]  # 下周日
        sVAL_9 = row_list[11]  # 合计
        sVAL_10 = row_list[12]  # 下一月出菜量：kg
        sVAL_11 = row_list[13]  # 下两月出菜量：kg
        sVAL_12 = row_list[14]  # 下三月出菜量：kg
        try:
            dot = sVAL_1.strip().find('.')
            fVAL_1 = float(sVAL_1[0:dot + 4])
        except:
            fVAL_1 = 0.0
        try:
            dot = sVAL_2.strip().find('.')
            fVAL_2 = float(sVAL_2[0:dot + 4])
        except:
            fVAL_2 = 0.0
        try:
            dot = sVAL_3.strip().find('.')
            fVAL_3 = float(sVAL_3[0:dot + 4])
        except:
            fVAL_3 = 0.0
        try:
            dot = sVAL_4.strip().find('.')
            fVAL_4 = float(sVAL_4[0:dot + 4])
        except:
            fVAL_4 = 0.0
        try:
            dot = sVAL_5.strip().find('.')
            fVAL_5 = float(sVAL_5[0:dot + 4])
        except:
            fVAL_5 = 0.0
        try:
            dot = sVAL_6.strip().find('.')
            fVAL_6 = float(sVAL_6[0:dot + 4])
        except:
            fVAL_6 = 0.0
        try:
            dot = sVAL_7.strip().find('.')
            fVAL_7 = float(sVAL_7[0:dot + 4])
        except:
            fVAL_7 = 0.0
        try:
            dot = sVAL_8.strip().find('.')
            fVAL_8 = float(sVAL_8[0:dot + 4])
        except:
            fVAL_8 = 0.0
        try:
            dot = sVAL_9.strip().find('.')
            fVAL_9 = float(sVAL_9[0:dot + 4])
        except:
            fVAL_9 = 0.0
        try:
            dot = sVAL_10.strip().find('.')
            fVAL_10 = float(sVAL_10[0:dot + 4])
        except:
            fVAL_10 = 0.0
        try:
            dot = sVAL_11.strip().find('.')
            fVAL_11 = float(sVAL_11[0:dot + 4])
        except:
            fVAL_11 = 0.0
        try:
            dot = sVAL_12.strip().find('.')
            fVAL_12 = float(sVAL_12[0:dot + 4])
        except:
            fVAL_12 = 0.0
        iLen = fVAL_1+fVAL_2+fVAL_3+fVAL_4+fVAL_5+fVAL_6+fVAL_7+fVAL_8+fVAL_9+fVAL_10+fVAL_11+fVAL_12
        # print iLen
        if iLen > 0:
            # print(row)
            # print sPMDM
            if qforpmdm(sPMDM) == True:
                tid = uuid.uuid1()
                sql = "INSERT INTO mrcsb.mryjcc(id,parent_id,insert_time,farm,pmdm,sheet,mrccl, xzcc_1, xzcc_2, xzcc_3, xzcc_4, xzcc_5,xzcc_6,xzcc_7,xzcc_hj,next_month,next_two_month,next_three_month) VALUES('%s','%s','%s','%s','%s','%s',%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f);" % (
                    tid, metadata[0], metadata[1], metadata[2], sPMDM, '预计出菜表', fVAL_1, fVAL_2, fVAL_3, fVAL_4, fVAL_5,fVAL_6,fVAL_7,fVAL_8,fVAL_9,fVAL_10,fVAL_11,fVAL_12)
                sql_l.append(sql)
                logger.info(sql)

def qforpmdm(pmdm):
    sql = "select count(*) from pm where id='%s'" % (pmdm)
    # print sql
    cur = conn.cursor()
    cur.execute(sql)
    r = cur.fetchone()
    # print r[0]
    if r[0] == 0:
        return False
    else:
        return True


def execute():
    cur = conn.cursor()
    for sql in sql_l:
        print sql
        cur.execute(sql)
    conn.commit()
    conn.close()


# 主流程
# 0、本地临时目录是否存在，存在则删除重建
# if os.path.exists(tmpdir):
#     shutil.rmtree(tmpdir)
# os.mkdir(tmpdir)
# 1、下载远程文件
# ftp_down()
# 2、遍历本地临时文件并读取
traverse()
execute()
shutil.rmtree(tmpdir)
logger.info('任务完成，正在回收')
conn.close()
# readFile('/home/vicent/Documents/上海农场2016011324每日采收表.xls')
