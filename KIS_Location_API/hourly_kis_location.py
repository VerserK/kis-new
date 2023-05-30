# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 16:56:25 2021

@author: methee.s
"""

import os
import pandas as pd
from pandas import json_normalize
from aiohttp import ClientSession, TCPConnector
#import nest_asyncio
#nest_asyncio.apply()
import asyncio
import math
import datetime as dt
import time
from datetime import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings
import io
from io import StringIO
import logging
import requests

sas_token = "sp=racwdli&st=2023-05-25T07:09:19Z&se=2023-12-31T15:09:19Z&spr=https&sv=2022-11-02&sr=c&sig=iKsoTBoyaBsVNa7wy7OiSaiylpC8IaJ1EvMlcv5TaM0%3D"
account_url = "https://kisnewstorage.blob.core.windows.net"
container = "raw"
blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
container_client = blob_service_client.get_container_client(container=container)

async def geta(url,headers):
    async with ClientSession() as session:
        async with session.get(url, headers=headers) as resp:  
            data = await resp.json()
    return data

async def posta(url,headers,payload):
    async with ClientSession() as session:
        async with session.post(url, headers=headers, json = payload) as resp:  
            data = await resp.json()
    return data

async def post(session, url, headers, payload):
    async with session.post(url, headers=headers, json = payload) as response:
        return await response.json()
    
async def post_multipayload(url,headers,payloads, loop):
    connector = TCPConnector(limit=2)
    async with ClientSession(connector=connector, loop=loop) as session:
        results = await asyncio.gather(*[post(session, url, headers, payload) for payload in payloads], return_exceptions=True)
        return results
    
def func_LineNotify(Message,LineToken):
    url  = "https://notify-api.line.me/api/notify"
    msn = {'message':Message}
    LINE_HEADERS = {"Authorization":"Bearer " + LineToken}
    session  = requests.Session()
    response =session.post(url, headers=LINE_HEADERS, data=msn)
    return response 

def run(thedate):
    logging.info(thedate)
    if isinstance(thedate, dt.datetime):
        logging.info('### Preparation ###')
        headers = {"Authorization": "Bearer 06b4aa5b-dafd-4971-b600-0b862b723209"}
        start = thedate - dt.timedelta(hours=2)
        logging.info('### Get Unit ID ###')
        blob_client = BlobClient.from_blob_url("https://kisnewstorage.blob.core.windows.net/apirecord/ALL_ID.csv?sp=racwdyi&st=2023-05-25T07:15:23Z&se=2023-12-31T15:15:23Z&spr=https&sv=2022-11-02&sr=b&sig=6OaJxePT75%2F8ivlZyTjVkX9%2Fec2t0FUB5yW%2B8Id0tCg%3D")
        download_stream = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
        all_id = pd.read_csv(StringIO(download_stream.readall()), low_memory=False)
        # all_id = pd.read_csv(r'D:\Data for Bridge\KIS\API_Record\All_ID.csv')
        idList = all_id['unitId'].drop_duplicates().to_list()
        logging.info(len(idList))
        try:
            idList.remove('fd79a9f5-074c-5e94-8179-48e34ffb836b')
        except:
            logging.info('equip was removed.')
        logging.info(len(idList))
        logging.info('### Get Location ###')
        start_time = time.time()

        df = pd.DataFrame()
        payloads = []
        countPoint = []
        urlHis = 'https://wolf-prp-prod-head-api.propulsetelematics.com/report/api/history/units/position-list'
        fromDate = start.strftime('%Y-%m-%dT%H:00:00.500+07:00')
        toDate = start.strftime('%Y-%m-%dT%H:59:59.999+07:00')
        logging.info('### START: ' + fromDate + '\nEND: ' + toDate)
        
        for i in range(0, len(idList), 20):
            payloads.append({"fromDate":fromDate,"toDate":toDate,"start":0,"limit":8317,"unitIds":idList[i:i+20],"showAll":True})
        logging.info('Start 1st Acquire')
        
        # loop = asyncio.get_event_loop()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        resHis = loop.run_until_complete(post_multipayload(urlHis,headers,payloads, loop))
        logging.info('Finish 1st Acquire')

        for i in range(0,len(resHis)):
            #print(i)
            try:
                countPoint.append(resHis[i]['totalCount'])
                df = pd.concat([df,json_normalize(resHis[i]['items'])])
            except Exception as e:
                # logging.info(resHis[i],e)
                func_LineNotify(resHis[i],'XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e')
                func_LineNotify(e,'XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e')
                exit()
    
        j = 0
        payloads = []
        for i in range(0,len(countPoint)):   
            n = math.ceil(countPoint[i]/8317)
            if n > 1:
                for k in range(1,n+1):
                    j += 1
                    payloads.append({"fromDate":fromDate,"toDate":toDate,"start":k*8317,"limit":8317,"unitIds":idList[i*20:(i*20)+20],"showAll":True})
        if(j > 0):
            logging.info('Start 2nd Acquire for:' + str(j) + ' req')
            loop = asyncio.get_event_loop()
            resHis = loop.run_until_complete(post_multipayload(urlHis,headers,payloads, loop))
            logging.info('Finish 2nd Acquire')
            logging.info(resHis)
            for i in range(0,len(resHis)):
                df = pd.concat(df,json_normalize(resHis[i]['items']))
                #print(resHis[i])
        
        df['longitude'] = df['position.coordinates'].str[0].astype(str)
        df['latitude'] = df['position.coordinates'].str[1].astype(str)
        df = df.drop(columns=['positionId', 'position.type', 'position.coordinates'])
        FileName = 'Location_'+ start.strftime('%Y-%m-%dT%H') + '.csv'
        df.sort_values(by='speed', ascending=True, na_position='first')
        df = df.drop_duplicates(subset=['positionTime','unitId', 'unitName','notes', 'statusDesc', 'statusColor', 'latitude', 'longitude'], keep='last')

        writer = io.BytesIO()
        df.to_csv(writer, index = False)
        blob_client = container_client.get_blob_client(FileName)
        blob_client.upload_blob(writer.getvalue(), overwrite = True)
        logging.info('Upload subscription_date Finished')
        # df.to_csv(os.path.join(r'D:\Data for Bridge\KIS\API_Record\raw',FileName),index=False)
        finish = datetime.today()
        finish = finish.strftime('%Y-%m-%d')
        logging.info('Finished Run: '+ finish)
        return FileName
    else:
        logging.info('Wrong input type: INPUT is not DATETIME type')
        return ''

# if __name__ == '__main__':
#     thedate = datetime.today() #- dt.timedelta(hours=13)
#     print(thedate)
#     if thedate <= datetime.today():
#         raw_file = run(thedate)
#         thedate+=dt.timedelta(hours=1)

