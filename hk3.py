#!/usr/bin/python
# -*- coding: utf-8 -*-
# Author: Daniel Lam
# Date: 2018-10-22
#desc: hk3.py     ##Housekeep the task transaction records of yesterday

import os, sys
import datetime
import time
import codecs

import mysql.connector

## Constant Declaration
c_equal = 'EQ'
c_between = 'BT'

cmd_fname = 'dist\dms_encode\dms_encode.exe'
l_ltext = os.popen(cmd_fname).read()  # cmd without console window
g_db_server_id = l_ltext.split('\n')[0]
g_db_user = l_ltext.split('\n')[1]
g_db_pswd = l_ltext.split('\n')[2]


def del_rec():
    global db_connector, db_cursor
    dbr_delete_task = 'delete from  ztask where datetime < "%s"'
    dbr_delete_task = dbr_delete_task % (l_datetime)
    print '* Command 1: ', dbr_delete_task
    db_cursor.execute(dbr_delete_task)
    print db_cursor.rowcount, ' records deleted'

    dbr_delete_task_param = 'delete from  ztask_param where datetime < "%s"'
    dbr_delete_task_param = dbr_delete_task_param % (l_datetime)
    print '* Command 2: ', dbr_delete_task_param
    db_cursor.execute(dbr_delete_task_param)
    print db_cursor.rowcount, ' records deleted'

    dbr_delete_task_listing = 'delete from  ztask_listing where datetime < "%s"'
    dbr_delete_task_listing = dbr_delete_task_listing % (l_datetime)
    print '* Command 3: ', dbr_delete_task_listing
    db_cursor.execute(dbr_delete_task_listing)
    print db_cursor.rowcount, ' records deleted'

l_datetime = datetime.datetime.now().strftime("%Y%m%d")
l_datetime = l_datetime+'000000'
print '* Purging records of ', l_datetime

db_connector = mysql.connector.connect(user=g_db_user, password=g_db_pswd,host=g_db_server_id,database='dms')
db_cursor = db_connector.cursor()


# delete records
del_rec()
# commit deletion
db_connector.commit()

raw_input('Press <Enter> to continue...')

