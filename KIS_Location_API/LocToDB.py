import pyodbc
import sqlalchemy as sa
from sqlalchemy import event
import urllib
import pandas as pd
import sys
import numpy as np
import glob
import os
import datetime as dt
from datetime import datetime
import requests
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings
import logging

### Connect Blob
account_url = "https://kisnewstorage.blob.core.windows.net"
sas_token = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiyx&se=2023-12-30T17:00:17Z&st=2023-05-25T09:00:17Z&spr=https&sig=OHJEj3pyrlNTvn%2Fl0OajlYwdzI72MNPgcgoQYlgARG0%3D"

####Blob Raw
container_raw = "raw"
blob_service_client_raw = BlobServiceClient(account_url=account_url, credential=sas_token)
container_client_raw = blob_service_client_raw.get_container_client(container=container_raw)

####Blob Loc
container_loc = "loc"
blob_service_client_loc = BlobServiceClient(account_url=account_url, credential=sas_token)
container_client_loc = blob_service_client_loc.get_container_client(container=container_loc)

def func_LineNotify(Message,LineToken):
    url  = "https://notify-api.line.me/api/notify"
    msn = {'message':Message}
    LINE_HEADERS = {"Authorization":"Bearer " + LineToken}
    session  = requests.Session()
    response =session.post(url, headers=LINE_HEADERS, data=msn)
    return response 

def uploadCSV(filepath,table):
    start = datetime.today()
    server = 'dwhsqldev01.database.windows.net'
    database = 'KISRecord'
    username = 'boon'
    password = 'DEE@DA123'
    driver = '{ODBC Driver 17 for SQL Server}'  
    dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password
    df = pd.read_csv(filepath)
    df['positionTime'] = pd.to_datetime(df['positionTime'], format='%d/%m/%Y %H:%M:%S')
    timing = df['positionTime'][0].strftime('%d/%m/%Y %H:00:00')
    logging.info(timing)
    timing = datetime.strptime(timing,'%d/%m/%Y %H:00:00') + dt.timedelta(hours=1)
    logging.info(timing)
    df = df[df['positionTime']<timing]
    df['statusDesc'] = df['statusDesc'].replace(np.nan, '-none-')
    df['notes'] = df['notes'].replace(np.nan, '-none-')
    df = df.rename(columns={"unitName": "EquipmentName","unitId":"Equipment_ID"})
    df['Country'] = np.where((df['Country']!= 'Thailand')&(df['ADM3_PCODE'].str.startswith('TH')),'Thailand',df['Country'])
    df = df.drop_duplicates(subset = ['EquipmentName','statusDesc','positionTime','notes','latitude','longitude'], keep='first')
    row = len(df)
    params = urllib.parse.quote_plus(dsn)

    #delete_q = "Delete from "+table+"] where cast(positionTime as date) = '"+df['positionTime'][0].strftime('%Y-%m-%d')+"' and "
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
    
    print('Finish Upload ' + table)
    os.remove(filepath)
    fpath, fname = os.path.split(filepath)
    blob_client = container_client_loc.get_blob_client(fname)
    blob_client.delete_blob()
    func_LineNotify(fname,'XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e')
    return df
