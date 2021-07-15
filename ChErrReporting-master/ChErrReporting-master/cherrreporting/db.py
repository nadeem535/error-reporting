# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 22:51:14 2019

@author: agupt84
"""
# Imports here
import ibm_db as db 
#from datetime import date, timedelta

# Make connection to DB
def make_dbconn(dbname, dbhostname, dbport, dbprotocol, dbuser, dbpwd):
    #print ("In make_dbconn para...")
    try:
        conn = db.connect("DATABASE={}; HOSTNAME={}; PORT={}; PROTOCOL={}; UID={}; PWD={};".format(dbname, dbhostname, dbport, dbprotocol, dbuser, dbpwd), "", "")
        #conn = db.connect("DATABASE=DB0P; HOSTNAME=gwya0002; PORT=50000; PROTOCOL=TCPIP; UID=TSUF2ZY; PWD=JUN09JUN;","","")
        print ("Connection to DB2 is successful...")
        return conn
    except Exception as e:
        print ("Error while making DB Connection :", e)    

def get_data(conn,sql,err_dt,err_logc_del_ind):
    
    #print ("In get_data para...", sql,err_dt,err_logc_del_ind)
    stmt = db.prepare(conn, sql)
    
    #explicitly bind the parameters
    db.bind_param(stmt,1,err_dt)
    db.bind_param(stmt,2,err_logc_del_ind)
    db.execute(stmt)
    
    #process results
    result = db.fetch_assoc(stmt)
    i = 0
    total_data = {}
    #while (result != False) & (i < 3):
    while (result != False):
        total_data.update({i: result})
        result = db.fetch_assoc(stmt)
        i = i + 1
    print("---------------------------------")
    print("Total ERR_RPT data is loaded in Dictionary :", i)
    print("---------------------------------")
    db.close(conn)
    return total_data

def get_rer_data(conn,sql,rer_dt):
    
    #print ("In get_data para...", sql,err_dt,err_logc_del_ind)
    stmt = db.prepare(conn, sql)
    
    #explicitly bind the parameters
    db.bind_param(stmt,1,rer_dt)
#    db.bind_param(stmt,2,err_logc_del_ind)
    db.execute(stmt)
    
    #process results
    result = db.fetch_assoc(stmt)
    i = 0
    total_data = {}
    #while (result != False) & (i < 3):
    while (result != False):
        total_data.update({i: result})
        result = db.fetch_assoc(stmt)
        i = i + 1
    print("---------------------------------")
    print("Total RER data loaded in Dictionary :", i)
    print("---------------------------------")
    db.close(conn)
    return total_data

def get_sys_msg_data(conn,sql,partnid):
    
    #print ("In get_data para...", sql,err_dt,err_logc_del_ind)
    stmt = db.prepare(conn, sql)
    
    #explicitly bind the parameters
    db.bind_param(stmt,1,partnid)
#    db.bind_param(stmt,2,err_logc_del_ind)
    db.execute(stmt)
    
    #process results
    result = db.fetch_assoc(stmt)
    i = 0
    total_data = {}
    #while (result != False) & (i < 3):
    while (result != False):
        total_data.update({i: result})
        result = db.fetch_assoc(stmt)
        i = i + 1
    print("---------------------------------")
    print("Total SYS_MSG data is loaded in Dictionary :", i)
    print("---------------------------------")
    db.close(conn)
    return total_data
