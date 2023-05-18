# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 16:56:25 2021

@author: methee.s
"""

import pandas as pd
import requests
from aiohttp import ClientSession, TCPConnector
import asyncio
#import nest_asyncio
#nest_asyncio.apply()
import datetime as dt
import math
import numpy as np
from pandas import json_normalize
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings

sas_token = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiyx&se=2023-12-31T16:42:26Z&st=2023-05-17T08:42:26Z&spr=https&sig=1ISnU4nNO0apxAr9C8sNk2TnBTsgv3Y5b2s4GIWlWKQ%3D"
account_url = "https://kisnewstorage.blob.core.windows.net"
container = "apirecord"
path = "abfss://apirecord@kisnewstorage.dfs.core.windows.net/apirecord/"
blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
container_client = blob_service_client.get_container_client(container=container)

def upload_csv(local_file_name):
    target_file_name = local_file_name
    blob_client = container_client.get_blob_client(target_file_name)
    with open(local_file_name, "rb") as data:
        print("Upload Start")
        content_settings = ContentSettings(content_type='text/plain')
        blob_client.upload_blob(data, overwrite=True, content_settings=content_settings)
        print("Upload Done")

async def get_single(url,headers,payload):
    async with ClientSession() as session:
        async with session.post(url, headers=headers, json = payload) as resp:  
            data = await resp.json()
    return data

async def get(session, url, headers):
    async with session.get(url, headers=headers) as response:
        return await response.json()

async def get_multiurl(urls,header, loop):
    connector = TCPConnector(limit=5)
    async with ClientSession(connector=connector, loop=loop) as session:
        results = await asyncio.gather(*[get(session, url, header) for url in urls], return_exceptions=True)
        return results
def run():   
    start = dt.datetime.today()
    # try:
    #     os.remove(r"D:\Data for Bridge\KIS\API_Record\ALL_ID.csv")
    # except:
    #     print("No file.")
    all_id = pd.DataFrame()
    headers = {"Authorization": "Bearer 06b4aa5b-dafd-4971-b600-0b862b723209"}
    urlID = 'https://wolf-prp-prod-head-api.propulsetelematics.com/wlf/api/units/paginated?start=0&limit=10000&order=NAME&statusIds='

    resID = requests.get(urlID, headers=headers)
    js = resID.json()
    try:
        countUnits = js['units']['totalCount']
    except:
        countUnits = js['totalCount']
        print(countUnits)
    print('TotalUnit = ' + str(countUnits))
    n = math.ceil(countUnits/10000)
    try:
        all_id = json_normalize(js['units']['items'])
    except:
        all_id = json_normalize(js['items'])

    print('Loop Started')

    for i in range(1,n+1):
        urlID = 'https://wolf-prp-prod-head-api.propulsetelematics.com/wlf/api/units/paginated?start='+str(i*10000)+'&limit=10000&order=NAME&statusIds='
        resID = requests.get(urlID, headers=headers)
        js = resID.json()
        try:
            all_id = pd.concat([all_id,json_normalize(js['units']['items'])], ignore_index = True)
            # all_id = all_id.append(json_normalize(js['units']['items']), ignore_index = True)
        except:
            all_id = pd.concat([all_id,json_normalize(js['items'])], ignore_index = True)
            # all_id = all_id.append(json_normalize(js['items']), ignore_index = True)
    print('Loop Finished')
    #all_id[['unitId','unitName','typeCode']].to_csv("D:\Data for Bridge\KIS\API_Record\Engine_Detail_base_check.csv",index=False)
    all_id = all_id[all_id['typeCode'].eq('icon-wolf-general') == False]
    all_id['typeCode'] = np.where(all_id['typeCode'].eq('icon-wolf-trencher_rock_saw'),'SKC_KUBOTA_COMBINE_HARVESTER',all_id['typeCode'])
    all_id['typeCode'] = np.where(all_id['typeCode'].eq('icon-wolf-tractor_1'),'SKC_KUBOTA_TRACTOR',all_id['typeCode'])
    all_id['typeCode'] = np.where(all_id['typeCode'].eq('icon-wolf-excavator'),'SKC_KUBOTA_MINI_EXCAVATOR',all_id['typeCode'])
    all_id['typeCode'] = np.where(all_id['typeCode'].isin(('SKC_KUBOTA_MINI_EXCAVATOR','SKC_KUBOTA_TRACTOR','SKC_KUBOTA_COMBINE_HARVESTER'))==False,'UNDEFINED',all_id['typeCode'])
    all_id[['unitId','unitName','typeCode']].to_csv(upload_csv("Engine_Detail_Update.csv"), index=False)

    for index,row  in all_id.iterrows():
        if 'เสีย' in row['unitName'] or 'เก่า' in row['unitName'] or 'KRDA' in row['unitName']:
            all_id.drop(index,inplace = True)
        elif '(' in row['unitName']:
            all_id.drop(index,inplace = True)
        elif row['unitName'] == 'NG':
            all_id.drop(index,inplace = True)

    export = all_id[['unitId','unitName']]
    export = export.drop_duplicates()
    export.to_csv(upload_csv("ALL_ID.csv"),index=False)
    
    print(abs(start-dt.datetime.today()).total_seconds()/60)