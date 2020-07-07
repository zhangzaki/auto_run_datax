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
import cx_Oracle
import threading
import subprocess
import MySQLdb
os.environ["NLS_LANG"] = ".AL32UTF8"

#需要修改的
owner='SCOTT'

#分区表sql模板
part_table_sql="""with t1 as
 (select segment_name, partition_name,owner
    from (select bytes / 1024 / 1024 part_size, segment_name, partition_name,owner
            from dba_segments
           where segment_type = 'TABLE PARTITION' and owner=upper('%s') and segment_name not like 'BIN%%'
           order by 1 desc, 2)
   where part_size > %d
     and part_size <= %d)
select table_name||'.'||t1.partition_name,no_where ||'\\"'||t1.owner||'\\".\\"'||TABLE_NAME|| '\\" partition(' || t1.partition_name || ') t'
  from (select 'select /*+ parallel(t,%d) */ ' || max(listagg)||' from  '  no_where,table_name
          from (select distinct table_name,
                       to_char(wm_concat(to_char('\\"'||column_name||'\\"')) over (partition by table_name order by COLUMN_ID)) LISTAGG
                  from (select distinct table_name,
                                        column_id,
                                        column_name as column_name
                          from dba_tab_columns
                         where table_name in (select distinct segment_name from t1) and owner=upper('%s')
                         order by 2 desc)) group by table_name ) t2,t1
                 where t1.segment_name=t2.table_name """
#子分区表sql模板
subpart_table_sql="""with t1 as
 (select segment_name, partition_name,owner
    from (select bytes / 1024 / 1024 part_size, segment_name, partition_name,owner
            from dba_segments
           where segment_type = 'TABLE SUBPARTITION' and owner=upper('%s') and segment_name not like 'BIN%%'
           order by 1 desc, 2)
   where part_size > %d
     and part_size <= %d)
select table_name||'.'||t1.partition_name,no_where ||'\\"'||t1.owner||'\\".\\"'||TABLE_NAME|| '\\" subpartition(' || t1.partition_name || ') t'
  from (select 'select /*+ parallel(t,%d) */ ' || max(listagg)||' from  '  no_where,table_name
          from (select distinct table_name,
                       to_char(wm_concat(to_char('\\"'||column_name||'\\"')) over (partition by table_name order by COLUMN_ID)) LISTAGG
                  from (select distinct table_name,
                                        column_id,
                                        column_name as column_name
                          from dba_tab_columns
                         where table_name in (select distinct segment_name from t1) and owner=upper('%s')
                         order by 2 desc)) group by table_name) t2,t1
                 where t1.segment_name=t2.table_name"""
#普通表sql模板
normal_table_sql="""
with t1 as
(select segment_name,owner
    from (select bytes / 1024 / 1024 table_size, segment_name,owner
    from dba_segments
        where segment_type = 'TABLE' and owner=upper('%s') and segment_name not like 'BIN%%'
    order by 1 desc, 2)
   where table_size > %d
     and table_size <= %d)
    select table_name,no_where||'\\"'||t1.owner||'\\".\\"'||TABLE_NAME || '\\" t'
    from (select 'select /*+ parallel(t,%d) */ ' || max(listagg)||' from '  no_where,table_name
    from (select distinct table_name as table_name,
                          to_char(wm_concat(to_char('\\"'||column_name||'\\"')) over (partition by table_name order by COLUMN_ID)) LISTAGG
    from (select distinct table_name,
                          column_id,
                          column_name as column_name
    from dba_tab_columns
        where table_name in (select distinct segment_name from t1) and owner=upper('%s')
    order by 2 desc)) group by table_name) t2,t1
    where t1.segment_name=t2.table_name
"""

normal_table_sql = normal_table_sql % (owner,0,10240000,1,owner)

part_table_no_parallel = part_table_sql % (owner,0,1024,1,owner)
part_table_two_parallel = part_table_sql % (owner,1024,10240000,2,owner)

subpart_table_no_parallel = subpart_table_sql % (owner,0,1024,1,owner)
subpart_table_two_parallel = subpart_table_sql % (owner,1024,10240000,2,owner)


#job模板
jobCnf = """
{
    "job": {
        "content": [
            {
                "reader": {
                    "name": "oraclereader",
                    "parameter": {
                        "connection": [
                            {
                            "jdbcUrl": ["ReadUrl"],
                            "querySql": ["qSQL"]
                            }
                        ],
                        "mandatoryEncoding":"gbk",
                        "password": "ReadPassword",
                        "username": "ReadUsername"
                    }
                },
                "writer": {
                    "name": "mysqlwriter",
                    "parameter": {
                        "column": [ColumnInfo],
                        "connection": [
                            {
                                "jdbcUrl": "WriteUrl",
                                "table": ["tableName"]
                            }
                        ],
                        "password": "WritePassword",
                        "preSql": [],
                        "session": [ "set foreign_key_checks = 0" ],
                        "username": "WriteUsername",
                        "writeMode": "insert"
                    }
                }
            }
        ],
        "setting": {
            "speed": {
                "channel": "RealChannel"
            }
        }
    }
}
"""

def get_sql_dict(oracle_connect,sql):
    sqldict={}
    oracleConn = cx_Oracle.connect(oracle_connect)
    oracleCursor = oracleConn.cursor()
    oracleCursor.execute(sql)
    rows=oracleCursor.fetchall()
    for row in rows:
        sqldict[row[0]]=row[1]
    return sqldict

def get_table_list(oracle_connect,sql):
    tablelist=[]
    oracleConn = cx_Oracle.connect(oracle_connect)
    oracleCursor = oracleConn.cursor()
    oracleCursor.execute(sql)
    rows=oracleCursor.fetchall()
    for row in rows:
        tablelist.append(row[0])
    return tablelist

def get_table_column_dict(oracle_config):
    column_dict={}
    oracleConn = cx_Oracle.connect(oracle_config)
    oracleCursor = oracleConn.cursor()
    exec_sql="""
        select table_name, max(listagg) from (
select table_name, to_char(wm_concat(to_char('"`' || COLUMN_NAME || '`"')) over (partition by table_name ORDER BY COLUMN_ID)) AS listagg
FROM dba_tab_columns
WHERE OWNER=upper('%s')) group by table_name    """ % owner
    oracleCursor.execute(exec_sql)
    rows=oracleCursor.fetchall()
    for row in rows:
        column_dict[row[0]]=row[1]
    return column_dict

#insert job info
def insert_job(mysql_connect,table_name,owner,job_name):
  #conn = MySQLdb.connect(host = '172.16.50.124',port = 3320,user = 'zjtest',passwd = 'zjtest',db = 'scott',charset='gbk')
  #字符串转成字典
  config=eval(mysql_connect)
  conn = MySQLdb.connect(**config)
  try:
    cur = conn.cursor()
    cur.execute("delete from sync_log.sync_log where table_name='%s' and schema_name='%s'" % (table_name,owner.upper()))
    cur.execute("insert into sync_log.sync_log(table_name,schema_name,job_name) values('%s','%s','%s')" % (table_name,owner.upper(),job_name))
    conn.commit()
    conn.close()
  except MySQLdb.Error, e:
    print "WARNING MySQL Error %d:%s" % (e.args[0], e.args[1])
    sys.exit(1)

def write_part_table_job(oracle_connect,mysql_connect,sql_statment,owner,jobconfig):
    table_column_dict=get_table_column_dict(oracle_connect)
    parallel_sql_dict=get_sql_dict(oracle_connect,sql_statment)
    table_part_nosplit_tbname=get_table_list(oracle_connect,sql_statment)
    for table_part_name in table_part_nosplit_tbname:

        query_sql= parallel_sql_dict[table_part_name]
        table_name=table_part_name.split('.')
        column_info=table_column_dict[table_name[0]]
        job=jobconfig.replace("qSQL",query_sql).replace("tableName",table_name[0]).replace("ColumnInfo",column_info).replace("WriteUrl",write_url).replace("ReadUrl",read_url).replace("ReadUsername",read_username).replace("ReadPassword",read_password).replace("WriteUsername",write_username).replace("WritePassword",write_password).replace("RealChannel",real_channel)

        first_path = '.'
        second_path = '/%s_part_table/' % owner
        dir_name = first_path + second_path + table_name[0]
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        cnfName =dir_name+'/'+ table_part_name + ".job"

        with open(cnfName, 'w') as f:
            f.write(job)
        insert_job(mysql_connect,table_part_name,owner,cnfName)
        print table_part_name,cnfName

def write_normal_table_job(oracle_connect,mysql_connect,sql_statment,owner,jobconfig):
    table_column_dict=get_table_column_dict(oracle_connect)
    parallel_sql_dict=get_sql_dict(oracle_connect,sql_statment)
    table_part_nosplit_tbname=get_table_list(oracle_connect,sql_statment)
    for normal_table_name in table_part_nosplit_tbname:
        query_sql= parallel_sql_dict[normal_table_name]
        column_info=table_column_dict[normal_table_name]
        job=jobconfig.replace("qSQL",str(query_sql)).replace("tableName",normal_table_name).replace("ColumnInfo",str(column_info)).replace("WriteUrl",write_url).replace("ReadUrl",read_url).replace("ReadUsername",read_username).replace("ReadPassword",read_password).replace("WriteUsername",write_username).replace("WritePassword",write_password).replace("RealChannel",real_channel)

        first_path = '.'
        second_path = '/%s_normal_table' % owner
        dir_name = first_path + second_path
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        cnfName =dir_name+'/'+ normal_table_name + ".job"
        with open(cnfName, 'w') as f:
            f.write(job)
        insert_job(mysql_connect,normal_table_name,owner,cnfName)
        print normal_table_name,cnfName

def usage():
    print("Usage:%s [--help|--suer|--spwd|--sip|--sid|--tuser|--tpwd|--tip|--tport|--tdb] args...." % sys.argv[0])

if __name__ == "__main__":
    suer = ''
    spwd = ''
    sip = ''
    sid = ''
    tuser = ''
    tpwd = ''
    tip = ''
    tport = ''
    tdb = ''
    system_name = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], [], ["help", "suser=", "spwd=", "sip=", "sid=", "tuser=", "tpwd=", "tip=", "tport=", "tdb="])
        for opt, arg in opts:
            if opt == ("--help"):
                usage()
                sys.exit(1)
            elif opt == ("--suser"):
                suer = str(arg)
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


    except getopt.GetoptError:
        print("getopt error!")
        usage()
        sys.exit(1)

    if suer == '':
        print("No declared [--suer] arguments ,e.g. --suer=scott ")
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

    oracle_config='%s/%s@%s:1521/%s' % (suer,spwd,sip,sid)
    read_url='jdbc:oracle:thin:@//%s:1521/%s' % (sip,sid)
    read_username=suer
    read_password=spwd
    write_url='jdbc:mysql://%s:%s/%s' % (tip,tport,tdb)
    write_username=tuser
    write_password=tpwd
    real_channel='10'

    mysql_config = "{'user': '%s','passwd': '%s','host': '%s','port': %s ,'db': '%s'}" % (tuser,tpwd,tip,tport,tdb)

    dir_name = '.'
    rm_cmd = "rm -rf %s*" % (tdb.upper())
    os.popen(rm_cmd)
    # write normal table job config
    write_normal_table_job(oracle_config,mysql_config,normal_table_sql,tdb,jobCnf)
    # write part table job config
    write_part_table_job(oracle_config,mysql_config,part_table_no_parallel,tdb,jobCnf)
    write_part_table_job(oracle_config,mysql_config,part_table_two_parallel,tdb,jobCnf)
    # write subpart table job config
    write_part_table_job(oracle_config,mysql_config,subpart_table_no_parallel,tdb,jobCnf)
    write_part_table_job(oracle_config,mysql_config,subpart_table_two_parallel,tdb,jobCnf)

    #tar_cmd = "tar zcf %s.tar.gz %s_part_table %s_normal_table" % (owner,owner,owner)
    #os.popen(tar_cmd)
    #print("Please copy %s.tar.gz file to the directory where dataX is installed") % owner