# -*- coding: utf-8 -*-
"""
Created on Sat Apr  3 14:35:24 2021

@author: pratipa.g
"""

import pyodbc
import sqlalchemy as sa
from sqlalchemy import event
import urllib
import pandas as pd
import sys
import numpy as np
import glob
import os
from datetime import datetime

def uploadCSV(db,filepath,table):
    #start = datetime.today()
    server = 'DEV01SVR'
    database = db
    username = 'boon'
    password = 'tryTh1$@h0me'
    driver = '{ODBC Driver 17 for SQL Server}'  
    dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    df = pd.read_csv(filepath)
    params = urllib.parse.quote_plus(dsn)                    
    engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s' % params)
    conn = engine.connect()
    
    cycle = 10000
    
    @event.listens_for(conn, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
    if len(df.index) >= cycle:
        for i in range(0,len(df.index)//cycle):
            print('chunk: ' + str(i))
            dftemp = df[i*cycle:(i*cycle) + cycle]
            dftemp.to_sql(table, con=conn,if_exists = 'append', index=False, schema="dbo")
        dftemp = df[cycle*(len(df.index)//cycle):]
        dftemp.to_sql(table, con=conn,if_exists = 'append', index=False, schema="dbo")

    else:
        df.to_sql(table, con=conn,if_exists = 'append', index=False, schema="dbo")
    
    print('Finish Upload ' + filepath)
    os.remove(filepath)
