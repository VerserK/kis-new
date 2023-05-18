# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 14:25:18 2021

@author: pratipa.g
"""
import pandas as pd
import requests
import math
import time
import datetime as dt
import pymssql
import csv
from pandas import json_normalize
from sys import exit
import pyodbc
from pandas import Series as se
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings
import io
from io import StringIO
from . import import_template

sas_token = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiyx&se=2023-12-31T16:42:26Z&st=2023-05-17T08:42:26Z&spr=https&sig=1ISnU4nNO0apxAr9C8sNk2TnBTsgv3Y5b2s4GIWlWKQ%3D"
account_url = "https://kisnewstorage.blob.core.windows.net"
container = "apirecord"
blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
container_client = blob_service_client.get_container_client(container=container)

def func_LineNotify(Message,LineToken):
    url  = "https://notify-api.line.me/api/notify"
    msn = {'message':Message}
    LINE_HEADERS = {"Authorization":"Bearer " + LineToken}
    session  = requests.Session()
    response =session.post(url, headers=LINE_HEADERS, data=msn)
    return response 

def subjob():
    start = dt.datetime.today()
    headers = {"Authorization": "Bearer 06b4aa5b-dafd-4971-b600-0b862b723209"}
    all_exp = pd.DataFrame()
    urlExp = 'https://wolf-prp-prod-head-api.propulsetelematics.com/report/api/subscription/expirations?start=0&limit=50&filter=&offsetUTC=7'
    resExp = requests.get(urlExp, headers=headers)
    js = resExp.json()
    countUnits = js['totalCount']
    n = math.ceil(countUnits/50)
    df = pd.json_normalize(resExp.json()['items'])
    # all_exp = all_exp.append(df)
    all_exp = pd.concat([all_exp,df])
    for i in range(1,n):
        urlExp ='https://wolf-prp-prod-head-api.propulsetelematics.com/report/api/subscription/expirations?start='+str(i*50)+'&limit=50&filter=&offsetUTC=7'
        resExp = requests.get(urlExp, headers=headers)
        df = pd.json_normalize(resExp.json()['items'])
        # all_exp = all_exp.append(df, ignore_index = True)
        all_exp = pd.concat([all_exp,df], ignore_index=True)
        time.sleep(1)
    
    writer = io.BytesIO()
    all_exp.to_csv(writer, index = False)
    blob_client = container_client.get_blob_client('subscription_date.csv')
    blob_client.upload_blob(writer.getvalue(), overwrite = True)
    print('Upload subscription_date Finished')
    print(abs(start-dt.datetime.today()).total_seconds()/60)
    return all_exp
def run():
    subfile = subjob()
    blob_client = BlobClient.from_blob_url("https://kisnewstorage.blob.core.windows.net/apirecord/subscription_date.csv?sp=r&st=2023-05-18T03:55:13Z&se=2023-12-31T11:55:13Z&spr=https&sv=2022-11-02&sr=b&sig=Ut8aMqPiDH09vonN2LIPVRAHjKmzKSMxWvRbk0rYN74%3D")
    download_stream = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
    subfile = pd.read_csv(StringIO(download_stream.readall()), low_memory=False)

    # subfile = pd.read_csv(r"D:\Data for Bridge\KIS\API_Record\subscription_date.csv")

    print("start")
    today = dt.datetime.today()
    blob_client = BlobClient.from_blob_url("https://kisnewstorage.blob.core.windows.net/apirecord/Engine_Detail_Update.csv?sp=r&st=2023-05-18T04:53:36Z&se=2023-12-31T12:53:36Z&spr=https&sv=2022-11-02&sr=b&sig=OpsVZ2C1xZj8j9f%2BBHQyXLm1b1DAHsGtWR%2BdXyEA92s%3D")
    download_stream = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
    engine_id = pd.read_csv(StringIO(download_stream.readall()), low_memory=False)
    # engine_id = pd.read_csv(r"D:\Data for Bridge\KIS\API_Record\Engine_Detail_Update.csv")

    result = pd.merge(engine_id, subfile, how='inner',on='unitName')
    result.loc[(result.subscriptionType == 'Offroad Advanced Tracking 5 - 5 years'),'Subscription_status'] = 1
    result.loc[(result.subscriptionType != 'Offroad Advanced Tracking 5 - 5 years'),'Subscription_status'] = 0
    result['tag'] = 'en'
    result = result.drop(columns = ['deviceSerialNumber','unitSerialNumber','companyName'])
    result.columns = ['Equipment_ID','Equipment_Name','Engine_Type','Subscription_type'\
                    ,'Subscription_end_date','Subscription_status','tag']
    result['Subscription_end_date'] = pd.to_datetime(result['Subscription_end_date'], format='%d/%m/%Y')

    server = 'skcdwhprdmi.siamkubota.co.th'
    database = 'KIS Data'
    username = 'skcadminuser'
    password = 'DEE@skcdwhtocloud2022prd'

    mydb = pymssql.connect(server=server, user=username, password=password, database=database)

    cursor = mydb.cursor()

    query = str("SELECT * FROM Engine_Detail;")
    cursor.execute(query)

    id_db = cursor.fetchall()

    ex_id = []
    ex_name = []
    ex_type = []
    ex_subdate = []
    ex_substat = []
    ex_subtype = []
    for tupe in id_db:
        ex_id.append(tupe[0])
        ex_name.append(tupe[1])
        ex_type.append(tupe[2])
        ex_subdate.append(tupe[3])
        ex_substat.append(tupe[4])
        ex_subtype.append(tupe[6])

    dbf = pd.concat([se(ex_id),se(ex_name),se(ex_type),se(ex_subtype),se(ex_subdate), se(ex_substat)],axis = 1)
    dbf.columns = ['Equipment_ID','Equipment_Name','Engine_Type','Subscription_type'\
                    ,'Subscription_end_date','Subscription_status']
    dbf['tag'] = 'ex'
    dbf['Subscription_end_date'] = pd.to_datetime(dbf['Subscription_end_date'], format='%Y-%m-%d')

    joindf = pd.concat([dbf,result])
    joindf = joindf.sort_values(by=['Equipment_ID','Equipment_Name','Engine_Type','Subscription_type'\
                    ,'Subscription_end_date','Subscription_status','tag'])

    joindf = joindf.drop_duplicates(subset=['Equipment_ID','Equipment_Name','Engine_Type','Subscription_type'\
                    ,'Subscription_end_date','Subscription_status'],keep=False)
    joindf = joindf[joindf['tag'].eq('en')].drop(columns = ['tag'])
    joindf.loc[(joindf.Subscription_type == 'Offroad Advanced Tracking 5 - 5 years'),'Subscription_Date'] = joindf['Subscription_end_date']-pd.DateOffset(years=5)+pd.DateOffset(days=1)
    joindf = joindf.rename(columns={'Engine_Type':'Product'})
    upd = dt.date.today()
    joindf['UpdateTime'] = upd
    joindf = joindf.drop_duplicates(subset=['Equipment_ID'],keep='first')
    # joindf.to_csv(r"D:\Data for Bridge\KIS\API_Record\join_subdrop.csv", index = False)
    writer = io.BytesIO()
    joindf.to_csv(writer, index = False)
    blob_client = container_client.get_blob_client('join_subdrop.csv')
    blob_client.upload_blob(writer.getvalue(), overwrite = True)
    print('Upload join_subdrop Finished')
    #func_LineNotify('Engine detail saved successfully.','pTfbjW6EG1oWMT7rY0N3v50dqRzg038xjSLbHXF9C4y')

    en_id = joindf['Equipment_ID'].tolist()
    en_name = joindf['Equipment_Name'].tolist()

    for i in range(len(en_id)):
        cursor.execute("delete Engine_Detail where Equipment_ID = '"+str(en_id[i])+"';")
        print('this is new entry')
        
    mydb.commit()
    cursor.close()
    blob_client = BlobClient.from_blob_url("https://kisnewstorage.blob.core.windows.net/apirecord/join_subdrop.csv?sp=r&st=2023-05-18T05:09:09Z&se=2023-12-31T13:09:09Z&spr=https&sv=2022-11-02&sr=b&sig=hqTfwV%2BBgcZdUNuzurCAJDNELdgPQ70mMUnc%2Bd0g9E0%3D")
    download_stream = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
    join_subdrop = pd.read_csv(StringIO(download_stream.readall()), low_memory=False)
    import_template.uploadCSV('KIS Data',join_subdrop,'Engine_Detail')

    print("FINISH UPLOADING KIS ENGINE_DETAIL")

    if today.hour in(9,10):
        farm = ['{ODBC Driver 17 for SQL Server}','consentdb.database.windows.net','consent-user','P@ssc0de123','kis-hour-test']
        farm_db = pyodbc.connect(driver=farm[0], server=farm[1], user=farm[2], password=farm[3], database=farm[4])
        farm_cursor = farm_db.cursor()

        farm_cursor.execute(query)
        id_fdb = farm_cursor.fetchall()

        ex_fid = []
        ex_fname = []

        for ftupe in id_fdb:
            ex_fid.append(ftupe[0])
            ex_fname.append(ftupe[1])
            
        f_dbf = pd.concat([se(ex_fid),se(ex_fname)],axis = 1)
        f_dbf.columns = ['Equipment_ID','Equipment_Name']
        f_dbf['tag'] = 'ex'

        total_farm = pd.concat([f_dbf,result[['Equipment_ID','Equipment_Name','tag']]])
        total_farm = total_farm.sort_values(by=['Equipment_ID','Equipment_Name'])

        total_farm['Equipment_ID'] = total_farm['Equipment_ID'].str.rstrip()
        total_farm['Equipment_Name'] = total_farm['Equipment_Name'].str.rstrip()
        total_farm = total_farm.drop_duplicates(subset=['Equipment_ID','Equipment_Name'],keep=False)
        total_farm = total_farm[total_farm['tag'].eq('en')].drop(columns = ['tag'])
        farm_id = total_farm['Equipment_ID'].to_list()
        farm_name = total_farm["Equipment_Name"].to_list()
        print(len(farm_id), len(farm_name))
        for ids in range(len(farm_id)):
            farmq = "Insert into Engine_Detail([Equipment_ID],[EquipmentName]) Values('"+str(farm_id[ids])+\
                    "', N'"+str(farm_name[ids])+"');"
            try:
                farm_cursor.execute(farmq)
            except:
                try:
                    farm_cursor.execute("Update Engine_Detail set EquipmentName = N'"+str(farm_name[ids])+"' where Equipment_ID = '"+str(farm_id[ids])+"';")
                except Exception as a:
                    func_LineNotify('FARM:'+str(a),'pTfbjW6EG1oWMT7rY0N3v50dqRzg038xjSLbHXF9C4y')
        farm_db.commit()
        farm_cursor.close()

# func_LineNotify(str(e),'pTfbjW6EG1oWMT7rY0N3v50dqRzg038xjSLbHXF9C4y')


