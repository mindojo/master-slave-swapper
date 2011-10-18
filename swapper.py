# -*- coding: utf-8 -*-
"""
    Mysql Slave
    ~~~~~~~~~~~~~~


    Slave scripting
    
    General information about switching master <-> slave
    
    Make sure that machines can talk to each other !
    
    mysql -u<username> -p -h otherhost
    
    Grant replication access for some user::
        
        grant replication slave on *.* to myslaveusr@'<host>' identified by 'MYSLVPWD';
    
    
    :created_on: Oct 4, 2011
    :author: marcink
"""

import re
import sys
from logbook import Logger
log = Logger('CommandLogger')

from subprocess import Popen, PIPE

DEBUG = True
MYSQL_USER = 'root'
MYSQL_PASSWD = 'qwe123qwe'

def ask_ok(prompt, retries=4, complaint='Yes or no, please!'):
    while True:
        ok = raw_input(prompt+' \n[y/n]')
        if ok in ('y', 'ye', 'yes'):
            return True
        if ok in ('n', 'no', 'nop', 'nope'):
            return False
        retries = retries - 1
        if retries < 0:
            raise IOError
        print complaint
        
class Command(object):

    def __init__(self, cwd=None):
        self.cwd = cwd

    def execute(self, cmd, *args):
        """Runs command on the system with given ``args``.
        """

        command = cmd + ' ' + ' '.join(args)
        log.debug('Executing %s' % command)
        if DEBUG:
            print command
        p = Popen(command, shell=True, stdout=PIPE, stderr=PIPE, cwd=self.cwd)
        stdout, stderr = p.communicate()
        if DEBUG:
            print stdout, stderr
        if stderr:
            raise Exception(stderr)
        return stdout, stderr

    def get_value(self):
        """
        get's value parsed from executed command
        """
        raise NotImplementedError

    def check(self, alarm_level):
        fstr = self.get_output()
        val = self.get_value(fstr)
        log.debug('checking %s => %s/%s' % (self, val, alarm_level))
        if self.is_alarm(val, alarm_level):
            return fstr, 'WARNING: current level of %s [%s] is not in bounds %s' % \
                (self, val, alarm_level)
        return fstr, None

    def get_formated(self):
        return '%s\n\n%s' % (self, self.get_output())


class MysqlCmd(Command):

    def __call__(self, mysql_cmd):
        cmd = '''mysql -u %(user)s -p%(passwd)s -e "%(cmd)s"'''
        params = dict(user=MYSQL_USER,
                      passwd=MYSQL_PASSWD,
                      cmd=mysql_cmd) 
        stdout, stderr = self.execute(cmd % params)
        return stdout


def make_master(log_name,log_offsett):
    """
    Makes a master from slave
    
    :param log_name:
    :param log_offsett:
    """
    mysqlCmd = MysqlCmd()
    
    
    #==========================================================================
    # PREREQUISITS
    #==========================================================================
    pre = """
    #On current Master:
    FLUSH LOGS;
    FLUSH TABLES WITH READ LOCK; #get's the lock now go to master
    SHOW MASTER STATUS \G <--- IMPORTANT this data needs to be passed to here
     log_name, log_offset
    """
    i_did_it = ask_ok(('Make sure you have executed this '
                      ' on the current master ! %s') % pre)
    
    if not i_did_it:
        sys.exit('Do it and come back !')
    
    # initialize the lock for sync it returns when logs reach certain position.
    # we should get this position from the master after lock !
    cmd = "SELECT MASTER_POS_WAIT('%s', %s)" % (log_name,int(log_offsett))
    mysqlCmd(cmd)
    
    
    cmd = "SHOW SLAVE STATUS\G"
    mysqlCmd(cmd)
    #initialize the show status and ask prompt
    is_ok = ask_ok('Is this output ok ?')
    
    if not is_ok:
        raise Exception('Show status was not ok !')


    cmd = 'STOP SLAVE'
    mysqlCmd(cmd)
    
    cmd = 'RESET MASTER'
    mysqlCmd(cmd)
    
    
    print "I'm a new master !"
    
def make_slave(host,port,user,passwd,log_name,log_pos):
    mysqlCmd = MysqlCmd()
    
    cmd = 'STOP SLAVE'
    mysqlCmd(cmd)    
    
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

    mysqlCmd(cmd)

    
    cmd = 'SHOW SLAVE STATUS \G'
    mysqlCmd(cmd)
    
    cmd = 'START SLAVE'
    mysqlCmd(cmd)

    cmd = 'SHOW SLAVE STATUS \G'
    mysqlCmd(cmd)    
    
    print "I'm a new slave !"
    
def help_():
    print '\n'.join([
                    'make_master <log_name> <log_pos>',
                    'make_slave <host> <port> <slaveuser> <passwd> <log_name> <log_pos>',
                ])
    sys.exit()
    
if __name__ == "__main__":
    if len(sys.argv)<2:
        help_()
    cmd = sys.argv[1]
    args = sys.argv[2:]
    if cmd == 'make_master':
        make_master(*args)
    elif cmd == 'make_slave':
        make_slave(*args)    
    else:
        help_()