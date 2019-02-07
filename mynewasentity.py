# -*- coding: utf-8 -*-
"""
Created on Fri Jan  4 11:44:59 2019

@author: rajesh
"""

import json

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
from iotfunctions.db import Database
from iotfunctions.metadata import EntityType
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.sql import text
from datetime import datetime
from sqlalchemy import create_engine
sourcedb = create_engine('ibm_db_sa://dash13990:3QXC__hHjpv9@dashdb-entry-yp-lon02-01.services.eu-gb.bluemix.net:50000/BLUDB;')
targetdb = create_engine('ibm_db_sa://dash105893:eDa7LuE0{5DA@dashdb-entry-yp-dal10-02.services.dal.bluemix.net:50000/BLUDB;')


with open('newcredentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

import os
os.environ['DB_CONNECTION_STRING'] = 'DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=TCPIP;UID=%s;PWD=%s;' %(credentials["database"],credentials["hostname"],credentials["port"],credentials["username"],credentials["password"])
os.environ['API_BASEURL'] = 'https://%s' %credentials['as_api_host']
os.environ['API_KEY'] = credentials['as_api_key']
os.environ['API_TOKEN'] = credentials['as_api_token']


db = Database(credentials = credentials)
sourceconn = sourcedb.connect() 
targetconn = targetdb.connect()
dbschema = credentials["connection"]

deviceid = 'KBHWVS1'
table_name = 'newmesdataset9'

#db.drop_table(table_name)

entity = EntityType(table_name,db,
                    Column('assetcode', String(50)),
                    Column('actualproddate', DateTime),
                    Column('productionshift', Integer),
                    Column('shiftduration', Integer),
                    Column('runningprodqty', Float),
                    Column('runninglossduration', Float),
                    **{
                            '_timestamp' : 'createddatetime',
                            '_db_schema' : dbschema
                             })       

entity.register()


########################################Load Data into Entity #####################################################################################

#selectmesdata = text("SELECT prodexecid, assetcode, actualproddate, pe.productionshift, HOUR(S1.ENDTIME - S1.STARTTIME), actualprodqty, defectcount, lossreasoncode, lossduration FROM PRODUCTIONEXECUTION PE, Loss1 L1, SHIFT S1 WHERE PE.actualproddate = L1.proddate AND PE.PRODUCTIONSHIFT = L1.productionshift AND pe.PRODUCTIONSHIFT = S1.SHIFTID")
selectmesdata = text("SELECT datetime, s1.shiftID, HOUR(S1.ENDTIME - S1.STARTTIME), value, sum(lossduration) FROM PORCHEDATA PE, Loss1 L1, SHIFT S1 WHERE pe.TAGNAME = 'hist_KBHWVS1_Werte.sps_Stueckzahl_01' AND time(PE.datetime) > S1.STARTTIME AND  time(PE.datetime) < S1.ENDTIME AND S1.SHIFTID = L1.productionshift AND date(datetime) = L1.PRODDATE GROUP BY datetime, s1.shiftID, HOUR(S1.ENDTIME - S1.STARTTIME), value")
mesdataresultset = sourceconn.execute(selectmesdata).fetchall()

for row in mesdataresultset:
        print(row)
        now = datetime.now()
        #formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
        query = "INSERT INTO %s(deviceid,devicetype,assetcode,actualproddate,productionshift,shiftduration, runningprodqty,runninglossduration,createddatetime) VALUES ('{0}','{1}','{2}','{3}',{4},{5},{6},{7},'{8}')".format(deviceid,table_name,deviceid,row[0],row[1],row[2],float(row[3]), float(row[4]),now) %table_name
        print(query)
        result = targetconn.execute(query)
        print(result)
        
sourceconn.close()
targetconn.close()

