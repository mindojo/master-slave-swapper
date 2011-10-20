# -*- coding: utf-8 -*-
import re
from functools import partial
from os import path
from fabric.api import run, cd, env, get, put, settings
from fabric.operations import sudo, local


def nice_run(cmd, nice_level=20):
    run("nice -n %s " % nice_level + cmd)

env.hosts = [""]
env.key_filename = path.expanduser("~/.ssh/id_rsa")

import mysql_commands

MYSQL_USER = '****'
MYSQL_PASSWD = '****'

mysql_commands.MYSQL_USER = MYSQL_USER
mysql_commands.MYSQL_PASSWD = MYSQL_PASSWD

MYSQL_REPLICATION_USER = 'replication'
MYSQL_REPLICATION_PASS = '*****'
MYSQL_REPLICATION_PORT = 3307

FORWARD_HOST = 'FROM_HOST'
BACKWARD_HOST = 'TO_HOST'


from mysql_commands import MysqlCmd, ask_ok

def __nginx_ctrl(ctrl_cmd):
    run("/etc/init.d/nginx %s" % ctrl_cmd)

def __apache_ctrl(ctrl_cmd):
    run("/etc/init.d/apache2 %s" % ctrl_cmd)

def __proxy_swap(from_s, to_s):
    """
    Remote proxy controll for apache
    
    :param from_s:
    :param to_s:
    """
    apache_conf = '/etc/apache2/sites-available/default'
    get(apache_conf, '/tmp/apache-conf')

    with open('/tmp/apache-conf') as f:
        buff = f.read()

    buff = buff.replace(from_s, to_s)

    with open('/tmp/apache-conf', 'w') as f:
        f.write(buff)

    put('/tmp/apache-conf', apache_conf)

def __enable_proxy():
    __proxy_swap("ProxyPass /proxy", "ProxyPass /")

def __disable_proxy():
    __proxy_swap("ProxyPass /", "ProxyPass /proxy")

def __flush_binlogs():
    mysqlCmd = MysqlCmd()
    cmd = mysqlCmd('FLUSH LOGS', True)
    run(cmd)

def __read_binlogs():
    mysqlCmd = MysqlCmd()
    cmd = mysqlCmd('SHOW MASTER STATUS\G', True)

    output = run(cmd)

    binlogfile = re.findall(r'File: \S+', output)[0].split(':')[-1].strip()
    binlogposition = re.findall(r'Position: \S+', output)[0].split(':')[-1].strip()

    return binlogfile, binlogposition

def __promote_to_slave(host, port, user, passwd, log_name, log_pos):

    mysqlCmd = MysqlCmd()

    cmd = mysqlCmd('STOP SLAVE', True)
    run(cmd)
    params = dict(host=host,
                  port=port,
                  user=user,
                  passwd=passwd,
                  log_name=log_name,
                  log_pos=log_pos)
    cmd = """CHANGE MASTER TO 
                    MASTER_HOST='%(host)s',
                    MASTER_PORT=%(port)s,  
                    MASTER_USER='%(user)s', 
                    MASTER_PASSWORD='%(passwd)s', 
                    MASTER_LOG_FILE='%(log_name)s',
                    MASTER_LOG_POS=%(log_pos)s""" % params;

    cmd = mysqlCmd(cmd, True)
    run(cmd)

    cmd = mysqlCmd('SHOW SLAVE STATUS \G', True)
    run(cmd)

    cmd = mysqlCmd('START SLAVE', True)
    run(cmd)

    cmd = mysqlCmd('SHOW SLAVE STATUS \G', True)
    run(cmd)

    print "I'm a new slave !"

def __promote_to_master(log_name, log_pos):

    mysqlCmd = MysqlCmd()

    # initialize the lock for sync it returns when logs reach certain position.
    # we should get this position from the master after lock !
    cmd = "SELECT MASTER_POS_WAIT('%s', %s)" % (log_name, int(log_pos))
    #cmd = mysqlCmd(cmd, True)
    #run(cmd)

    cmd = mysqlCmd("SHOW SLAVE STATUS\G", True)
    run(cmd)

    #initialize the show status and ask prompt
    is_ok = ask_ok('Is this output ok ?')

    if not is_ok:
        raise Exception('Show status was not ok !')


    cmd = mysqlCmd('STOP SLAVE', True)
    run(cmd)

    cmd = mysqlCmd('RESET MASTER', True)
    run(cmd)

    print "I'm a new master !"


def forward():
    # set proxy redirect to / in apache config
    with settings(host_string=BACKWARD_HOST):
        __enable_proxy()
        __apache_ctrl('stop')
        __flush_binlogs()
        binlogfile, binlogposition = __read_binlogs()

    with settings(host_string=FORWARD_HOST):
        __promote_to_master(log_name=binlogfile, log_pos=binlogposition)
        binlogfile, binlogposition = __read_binlogs()

    with settings(host_string=BACKWARD_HOST):
        __promote_to_slave('127.0.0.1', MYSQL_REPLICATION_PORT,
                           MYSQL_REPLICATION_USER, MYSQL_REPLICATION_PASS,
                           log_name=binlogfile, log_pos=binlogposition)
        __apache_ctrl('start')

#    with settings(host_string=FORWARD_HOST):
#        __nginx_ctrl('start')


def backward():

    with settings(host_string=FORWARD_HOST):
#        __nginx_ctrl('stop')
        __flush_binlogs()
        binlogfile, binlogposition = __read_binlogs()

    with settings(host_string=BACKWARD_HOST):
        __promote_to_master(log_name=binlogfile, log_pos=binlogposition)
        binlogfile, binlogposition = __read_binlogs()

    with settings(host_string=FORWARD_HOST):
        __promote_to_slave('127.0.0.1', MYSQL_REPLICATION_PORT,
                           MYSQL_REPLICATION_USER, MYSQL_REPLICATION_PASS,
                           log_name=binlogfile, log_pos=binlogposition)

    with settings(host_string=BACKWARD_HOST):
        __apache_ctrl('stop')
        __disable_proxy()
        __apache_ctrl('start')

