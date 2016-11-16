# -*- coding: utf-8 -*-

'''ORACLE表结构对比模块

根据配置文件，对比表结构差异

Notes:

1.需要安装ORACLE客户端

'''

# @Date    : 2016-11-07 23:35:18
# @Author  : wudi (wudi@xiyuetech.com)
# @Link    : ${link}
# @Version : 1.0


import ConfigParser
# import os

# env = os.environ['path']
# os.environ['path'] = 'D:\Program Files\oracleClient64' + ';' + env
import cx_Oracle
import time


COMPARESDATA = ['COLUMN_NAME', 'DATA_TYPE', 'DATA_LENGTH', 'DATA_PRECISION', 'DATA_SCALE', 'NULLABLE']
SOURCETABLES = []
TARGETTABLES = []
SOURCECONNSTR = ''
TARGETCONNSTR = ''
ORACLECLIENT = ''


def getConfig():
    config = ConfigParser.ConfigParser()
    f = open('config.ini', "rb")
    config.readfp(f)
    SOURCETABLES = config.get("CONFIG", "SOURCETABLES")
    SOURCETABLES = SOURCETABLES.split(',')
    TARGETTABLES = config.get("CONFIG", "TARGETTABLES")
    TARGETTABLES = TARGETTABLES.split(',')
    SOURCECONNSTR = config.get("CONFIG", "SOURCECONNSTR")
    TARGETCONNSTR = config.get("CONFIG", "TARGETCONNSTR")
    COMPARESDATA = config.get("CONFIG", "COMPARESDATA")

    COMPARESDATA = COMPARESDATA.split(',')
    ORACLECLIENT = config.get("OPTION", "ORACLE_CLIENT")
    f.close()

    return COMPARESDATA, SOURCETABLES, TARGETTABLES, SOURCECONNSTR, TARGETCONNSTR, ORACLECLIENT


def PrintConfig():
    print '-------Config------------------'
    print 'SOURCETABLES      =' + ','.join(SOURCETABLES)
    print 'TARGETTABLES      =' + ','.join(TARGETTABLES)
    print 'SOURCECONNSTR     =' + SOURCECONNSTR
    print 'TARGETCONNSTR     =' + TARGETCONNSTR
    print 'COMPARESDATA      =' + ','.join(COMPARESDATA)
    print 'ORACLE_CLIENT     =' + ORACLECLIENT
    print '------------------------------'


def GetTabDef(connstr, owner, tbname):
    db = cx_Oracle.connect(connstr)
    cursor = db.cursor()
    sqlstr = "select " + ",".join(COMPARESDATA) + " from  ALL_TAB_COLS where owner='%s' and table_name='%s'" % (owner, tbname)
    cursor.execute(sqlstr)
    res = cursor.fetchall()
    cursor.close()
    db.close()
    return res


def CompareTabDef(lowner, ltable, rowner, rtable):
    tbl = GetTabDef(SOURCECONNSTR, lowner, ltable)
    tbr = GetTabDef(TARGETCONNSTR, rowner, rtable)
    cmpcl = []
    match = 'y'
    for cl in tbl:
        cltmp = []
        i = 1
        cltmp.append(cl[0])
        while i < len(COMPARESDATA):
            x = []
            x.append(cl[i])
            cltmp.append(x)
            i = i + 1
        cltmp.append('y')
        # cltmp=['',[],[],[],[],[],'']
        for cl2 in tbr:
            find = False
            if cl2[0] == cl[0]:
                find = True
                break
        if find:
            i = 1
            while i < len(COMPARESDATA):
                cltmp[i].append(cl2[i])
                i = i + 1
            i = 1
            while i <= len(cltmp) - 2:
                if cltmp[i][0] != cltmp[i][1]:
                    cltmp[-1] = 'n'
                    match = 'n'
                    break
                i = i + 1
        else:
            cltmp[-1] = 'n'
            match = 'n'
        cmpcl.append(cltmp)
    return match, cmpcl


def getTabandOwner(tb):
    tb = tb.split('.')
    if len(tb) == 2:
        return tb[0], tb[1]
    else:
        return '', ''


def CompareTabs():
    i = 0
    for tb in SOURCETABLES:
        ownerl, tbl = getTabandOwner(tb)
        ownerr, tbr = getTabandOwner(TARGETTABLES[i])
        if ownerl and tbl and ownerr and tbr:
            st = time.clock()
            m, r = CompareTabDef(ownerl, tbl, ownerr, tbr)
            end = time.clock()
            if m == 'y':
                print "table %s.%s is match %f" % (ownerl, tbl, end - st)
            else:
                print "table %s.%s is not match %f" % (ownerl, tbl, end - st)
                for a in r:
                    if a[-1] == 'n':
                        print ownerl + '.' + tbl + ':' + str(a)
        else:
            print 'table %s with error:no enoufe info'
        i = i + 1


if __name__ == '__main__':
    COMPARESDATA, SOURCETABLES, TARGETTABLES, SOURCECONNSTR, TARGETCONNSTR, ORACLECLIENT = getConfig()
    PrintConfig()
    CompareTabs()
