# -*- coding: utf-8 -*-
import re
from functools import partial
from os import path
from fabric.api import run, cd, env, get, put
from fabric.operations import sudo, local

from mysql_commands import MysqlCmd, ask_ok

def nice_run(cmd, nice_level=20):
    run("nice -n %s " % nice_level + cmd)

env.hosts = ["root@server.com.com:22"]
env.key_filename = path.expanduser("~/.ssh/id_rsa")


MYSQL_USER = 'root'
MYSQL_PASSWD = '****'

MYSQL_REPLICATION_USER = 'replication'
MYSQL_REPLICATION_PASS = '****'

def __nginx_ctrl(run):
    local("/etc/init.d/nginx %s" % run)

def __apache_ctrl(run):
    run("/etc/init.d/apache2 %s" % run)

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

def __flush_binlogs(remote=False):
    if remote:
        run = run
    else:
        run = partial(local, capture=True)

    mysql_cmd = 'FLUSH LOGS'
    cmd = '''mysql -u %(user)s -p%(passwd)s -e "%(cmd)s"'''
    params = dict(user=MYSQL_USER,
                  passwd=MYSQL_PASSWD,
                  cmd=mysql_cmd)

    run(cmd % params)

def __read_binlogs(remote=True):
    if remote:
        run = run
    else:
        run = partial(local, capture=True)

    mysql_cmd = 'SHOW MASTER STATUS\G'
    cmd = '''mysql -u %(user)s -p%(passwd)s -e "%(cmd)s"'''

    params = dict(user=MYSQL_USER,
                  passwd=MYSQL_PASSWD,
                  cmd=mysql_cmd)

    output = run(cmd % params)

    binlogfile = re.findall(r'File: \S+', output)[0].split(':')[-1].strip()
    binlogposition = re.findall(r'Position: \S+', output)[0].split(':')[-1].strip()

    return binlogfile, binlogposition

def __promote_to_slave(host, port, user, passwd, log_name, log_pos, remote=True):
    if remote:
        run = run
    else:
        run = partial(local, capture=True)

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
                    MASTER_PORT='%(port)s',  
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

def __promote_to_master(log_name, log_pos, remote=True):
    if remote:
        run = run
    else:
        run = partial(local, capture=True)

    mysqlCmd = MysqlCmd()

    # initialize the lock for sync it returns when logs reach certain position.
    # we should get this position from the master after lock !
    cmd = "SELECT MASTER_POS_WAIT('%s', %s)" % (log_name, int(log_pos))
    cmd = mysqlCmd(cmd, True)
    local(cmd)

    cmd = mysqlCmd("SHOW SLAVE STATUS\G", True)
    local(cmd)

    #initialize the show status and ask prompt
    is_ok = ask_ok('Is this output ok ?')

    if not is_ok:
        raise Exception('Show status was not ok !')


    cmd = mysqlCmd('STOP SLAVE', True)
    local(cmd)

    cmd = mysqlCmd('RESET MASTER', True)
    local(cmd)

    print "I'm a new master !"


def __promote_remote_to_slave(host, port, user, passwd, log_name, log_pos):
    __promote_to_slave(host, port, user, passwd, log_name, log_pos, remote=True)

def __promote_local_to_slave(host, port, user, passwd, log_name, log_pos):
    __promote_to_slave(host, port, user, passwd, log_name, log_pos, remote=False)

def __promote_local_to_master(log_name, log_pos):
    __promote_to_master(log_name, log_pos, remote=False)

def __promote_remote_to_master(log_name, log_pos):
    __promote_to_master(log_name, log_pos, remote=True)


def forward():
    # set proxy redirect to / in apache config
    __enable_proxy()

    __apache_ctrl('stop')
    __flush_binlogs(remote=True)

    binlogfile, binlogposition = __read_binlogs(remote=True)
    __promote_local_to_master(log_name=binlogfile, log_pos=binlogposition)

    binlogfile, binlogposition = __read_binlogs(remote=False)
    __promote_remote_to_slave('127.0.0.1', 3307, MYSQL_REPLICATION_USER,
                              MYSQL_PASSWD, log_name=binlogfile,
                              log_pos=binlogposition)
    __nginx_ctrl('stop')

def backward():

    __nginx_ctrl('stop')
    __disable_proxy()
    __flush_binlogs(remote=False)
    binlogfile, binlogposition = __read_binlogs(remote=False)

    __promote_remote_to_master(log_name=binlogfile, log_pos=binlogposition)
    binlogfile, binlogposition = __read_binlogs(remote=True)

    __promote_local_to_slave('127.0.0.1', 3307, MYSQL_REPLICATION_USER,
                              MYSQL_PASSWD, log_name=binlogfile,
                              log_pos=binlogposition)

    __apache_ctrl('start')
