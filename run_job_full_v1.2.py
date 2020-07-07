#!/usr/bin/python
#coding:utf-8
"""
v0.7 支持分区表
v0.8 支持子分区
v0.9 支持把配置文件插入到目标端数据库sync_log中
v1.0 增加源端与目标端数据count校验功能
v1.1 支持源端schema与目标端database名字不一致
"""

import os
import sys
import getopt
import subprocess
import datetime
import cx_Oracle
import threading
import MySQLdb
from concurrent.futures import ThreadPoolExecutor
from concurrent import futures

def printf (format,*args):
    sys.stdout.write (format % args)

def printException (exception):
    error, = exception.args
    printf ("Error code = %s\n",error.code);
    printf ("Error message = %s\n",error.message);

def update_count_rows(mysql_connect,sql):
    #conn = MySQLdb.connect(host = '172.16.50.124',port = 3320,user = 'zjtest',passwd = 'zjtest',db = 'scott',charset='gbk')
    config=eval(mysql_connect)
    conn = MySQLdb.connect(**config)
    try:
      cur = conn.cursor()
      cur.execute(sql)
      conn.commit()
      cur.close()
      conn.close()
    except MySQLdb.Error, e:
      print "MySQL Error %d:%s" % (e.args[0], e.args[1])
      conn.close()
      sys.exit(1)

#传入sync_log中的table_name、schema_name
def get_source_count(oracle_connect,mysql_connect,source_schema,target_schema,table_name):

    if table_name.find('.') == -1 :
        tb_name = table_name
        count_sql = 'select count(*) from \"%s\".\"%s\"' % (source_schema, table_name)
    else:
        full_tb_name = table_name.split('.')
        tb_name = full_tb_name[0]
        part_name = full_tb_name[1]
        count_sql = 'select count(*) from \"%s\".\"%s\" partition(%s)' % (source_schema,tb_name,part_name)

    #print count_sql
    try:
        oracleConn = cx_Oracle.connect(oracle_connect)
    except cx_Oracle.DatabaseError, exception:
        print 'Failed to connect to %s\n' % oracle_connect
        printException (exception)
        sys.exit(1)

    oracleCursor = oracleConn.cursor()

    try:
        oracleCursor.execute(count_sql)
    except cx_Oracle.DatabaseError, exception:
        print 'Failed execute select count(*) from %s.%s partition(%s), on %s \n' % (source_schema,tb_name,part_name,oracle_connect)
        printException (exception)
        sys.exit(1)

    rows=oracleCursor.fetchall()
    source_rows = rows[0][0]
    #在oracle连接释放之前完整sql语句拼接
    update_source_sql = "update sync_log.sync_log set source_count=%d where table_name='%s' and schema_name=upper('%s') " % (source_rows,table_name,target_schema)

    oracleCursor.close()
    oracleConn.close()
    #print update_source_sql
    #在mysql中更新源端行数
    try:
        update_count_rows(mysql_connect,update_source_sql)
    except MySQLdb.Error, e:
        print "Failed execute update sync_log.sync_log set source_count=%d where table_name='%s' and schema_name=upper('%s') .on %s\n" % (source_rows,table_name,target_schema,mysql_connect)
        print "MySQL Error %d:%s" % (e.args[0], e.args[1])
        sys.exit(1)

#传入sync_log中的table_name、schema_name
def get_target_count(mysql_connect,target_schema,table_name):

    if table_name.find('.') == -1 :
        tb_name = table_name
        count_sql = 'select count(*) from `%s`.`%s`' % (target_schema, tb_name)
    else:
        full_tb_name = table_name.split('.')
        tb_name = full_tb_name[0]
        part_name = full_tb_name[1]
        count_sql = 'select count(*) from `%s`.`%s` partition(%s)' % (target_schema,tb_name,part_name)

    config=eval(mysql_connect)

    try:
        conn = MySQLdb.connect(**config)
        cur = conn.cursor()
        cur.execute(count_sql)
        rows = cur.fetchall()
        target_rows = rows[0][0]
    except MySQLdb.Error, e:
        print "MySQL Error %d:%s" % (e.args[0], e.args[1])
        conn.close()
        sys.exit(1)

    update_target_sql = "update sync_log.sync_log set target_count=%d where table_name='%s' and schema_name=upper('%s') " % (target_rows,table_name,target_schema)
    #print  update_target_sql
    cur.close()
    conn.close()

    try:
        #更新行数
        update_count_rows(mysql_connect,update_target_sql)
    except MySQLdb.Error, e:
        print("Failed execute update sync_log.sync_log set target_count=%d where table_name='%s' and schema_name=upper('%s') .\n") % (target_rows,table_name,target_schema)
        print "MySQL Error %d:%s" % (e.args[0], e.args[1])
        sys.exit(1)


#获取表的字典信息
def get_table_job_list(mysql_connect,owner,sql):
    tb_job_dict={}
    config=eval(mysql_connect)
    conn = MySQLdb.connect(**config)
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        tb_job_dict[row[0]]=row[1]
    conn.close()
    return tb_job_dict

def runjob(oracle_config,mysql_connect,source_schema,target_schema,table_name,jobname):
    config=eval(mysql_connect)
    conn = MySQLdb.connect(**config)
    conn.autocommit(0)
    cur = conn.cursor()

    begin_sync = "update sync_log.sync_log set syning=1,sync_complete=0,complete_status=2 where schema_name=upper('%s') and table_name='%s'" % (target_schema,table_name)
    success_sync = "update sync_log.sync_log set syning=0,sync_complete=1,complete_status=1 where schema_name=upper('%s') and table_name='%s'" % (target_schema,table_name)
    failed_sync = "update sync_log.sync_log set syning=0,sync_complete=1,complete_status=0 where schema_name=upper('%s') and table_name='%s'" % (target_schema,table_name)

    log_dir_first = 'log/'
    log_name = log_dir_first + jobname + '.log'
    log_path = log_dir_first + os.path.dirname(jobname)
    if not os.path.exists(log_path):
        try:
            os.makedirs(log_path)
        except OSError as e:
            raise

    #begin
    cur.execute(begin_sync)
    with open(log_name,'w') as f:
        p = subprocess.Popen(["./bin/smartMove.py", jobname], stdout=f, stderr=f)
    p.wait()

    if p.returncode == 0:
        try:
            cur.execute(success_sync)
            conn.commit()
        except MySQLdb.Error, e:
            print "MySQL Error %d:%s" % (e.args[0], e.args[1])
            conn.close()
            sys.exit(1)
        cur.close()
        conn.close()

        pool = ThreadPoolExecutor(max_workers=100)
        job1 = pool.submit(get_source_count,oracle_config,mysql_connect,source_schema,target_schema,table_name)
        job2 = pool.submit(get_target_count,mysql_connect,target_schema,table_name)
        if job1.done() :
            print "*******************target: %s.%s count success." % (source_schema,table_name)
        else :
            print "*******************target: %s.%s count failed." % (source_schema,table_name)
            print job1.result()
        if job2.done() :
            print "*******************source: %s.%s count success." % (target_schema,table_name)
        else :
            print "*******************source: %s.%s count failed." % (target_schema,table_name)
            print job2.result()
        return jobname + " success"
    else:
        try:
            cur.execute(failed_sync)
            conn.commit()
        except MySQLdb.Error, e:
            print "MySQL Error %d:%s" % (e.args[0], e.args[1])
            conn.close()
            sys.exit(1)
        cur.close()
        conn.close()
        return jobname + " failed"


def usage():
    print("Usage:%s [--help|--suser|--spwd|--sip|--sid|--tuser|--tpwd|--tip|--tport|--tdb|--threadnum|--limitnum|--only_validate_data] args...." % sys.argv[0])

if __name__ == "__main__":
    suser = ''
    spwd = ''
    sip = ''
    sid = ''
    tuser = ''
    tpwd = ''
    tip = ''
    tport = ''
    tdb = ''
    threadNum = ''
    limitNum = ''
    only_validate_data = '0'
    try:
        opts, args = getopt.getopt(sys.argv[1:], [], ["help", "suser=", "spwd=", "sip=", "sid=", "tuser=", "tpwd=", "tip=", "tport=", "tdb=", "threadnum=", "limitnum=", "only_validate_data="])
        for opt, arg in opts:
            if opt == ("--help"):
                usage()
                sys.exit(1)
            elif opt == ("--suser"):
                suser = str(arg)
            elif opt == ("--spwd"):
                spwd = str(arg)
            elif opt == ("--sip"):
                sip = str(arg)
            elif opt == ("--sid"):
                sid = str(arg)
            elif opt == ("--tuser"):
                tuser = str(arg)
            elif opt == ("--tpwd"):
                tpwd = str(arg)
            elif opt == ("--tip"):
                tip = str(arg)
            elif opt == ("--tport"):
                tport = str(arg)
            elif opt == ("--tdb"):
                tdb = str(arg)
            elif opt == ("--threadnum"):
                threadNum = str(arg)
            elif opt == ("--limitnum"):
                limitNum = str(arg)
            elif opt == ("--only_validate_data"):
                only_validate_data = str(arg)
    except getopt.GetoptError as err:
        print("getopt error!")
        print str(err)
        usage()
        sys.exit(1)

    if suser == '':
        print("No declared [--suser] arguments ,e.g. --suser=scott ")
        usage()
        sys.exit(1)
    if spwd == '':
        print("No declared [--spwd] arguments ,e.g. --spwd=tiger ")
        usage()
        sys.exit(1)
    if sip == '':
        print("No declared [--sip] arguments ,e.g. --sip=172.16.90.231 ")
        usage()
        sys.exit(1)
    if sid == '':
        print("No declared [--sid] arguments ,e.g. --sid=orcl ")
        usage()
        sys.exit(1)
    if tuser == '':
        print("No declared [--tuser] arguments ,e.g. --tuser=dbscale ")
        usage()
        sys.exit(1)
    if tpwd == '':
        print("No declared [--tpwd] arguments ,e.g. --tpwd=abc123 ")
        usage()
        sys.exit(1)
    if tip == '':
        print("No declared [--tip] arguments ,e.g. --tip=172.16.90.220 ")
        usage()
        sys.exit(1)
    if tport == '':
        print("No declared [--tport] arguments ,e.g. --tport=23306 ")
        usage()
        sys.exit(1)
    if tdb == '':
        print("No declared [--tdb] arguments ,e.g. --tdb=test ")
        usage()
        sys.exit(1)
    if threadNum == '':
        print("No declared [--threadnum] arguments ,default threadnum=5 ")
        threadNum = 5
    if limitNum == '':
        print("No declared [--limitnum] arguments ,default limitnum=10 ")
        limitNum = 10
    if only_validate_data == '':
        print("No declared [--only_validate_data] arguments ,default only_validate_data=0 ")
        only_validate_data = '0'

    oracle_config='%s/%s@%s:1521/%s' % (suser,spwd,sip,sid)
    mysql_config = "{'user': '%s','passwd': '%s','host': '%s','port': %s ,'db': '%s'}" % (tuser,tpwd,tip,tport,tdb)

    if only_validate_data == '1':
        #获取需要校验数据的表信息,只更新已经同步的任务的行数
        vd_jobs = []
        tb_validate_sql = "select table_name,job_name from sync_log.sync_log where schema_name=upper('%s') and complete_status=1 and syning=0 and sync_complete=1 and (source_count is null or target_count is null) limit %d" % (tdb,int(limitNum))
        validate_table_dict = get_table_job_list(mysql_config,tdb,tb_validate_sql)
        for table_name in validate_table_dict:
            vd_pool = ThreadPoolExecutor(max_workers=100)
            job11 = vd_pool.submit(get_source_count,oracle_config,mysql_config,suser,tdb,table_name)
            job21 = vd_pool.submit(get_target_count,mysql_config,tdb,table_name)
            if job11.done() :
                print "*******************target: %s.%s count success." % (suser,table_name)
            else :
                print "*******************target: %s.%s count failed." % (suser,table_name)
                print job11.result()
            if job21.done() :
                print "*******************source: %s.%s count success." % (tdb,table_name)
            else :
                print "*******************source: %s.%s count failed." % (tdb,table_name)
                print job21.result()
    if only_validate_data == '0':
        jobs = []
        tb_sync_sql = "select table_name,job_name from sync_log.sync_log where schema_name=upper('%s') and (complete_status!=1 or complete_status is null) and (syning!=1 or syning is null) limit %d" % (tdb,int(limitNum))
        full_table_dict = get_table_job_list(mysql_config,tdb,tb_sync_sql)
        thread_pool = ThreadPoolExecutor(max_workers=int(threadNum))
        for table_name in full_table_dict:
            job = thread_pool.submit(runjob,oracle_config,mysql_config,suser,tdb,table_name,full_table_dict[table_name])
            jobs.append(job)
        for job in futures.as_completed(jobs):
            print job.result()

