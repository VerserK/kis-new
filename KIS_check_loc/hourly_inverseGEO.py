# -*- coding: utf-8 -*-
"""
Created on Mon Jan 25 12:43:46 2021

@author: methee.s
"""
import os
import pandas as pd
import geopandas as gpd
import shapely.speedups
shapely.speedups.enable()
import time
# import LocToDB
from . import LocToDB
import datetime as dt
import traceback
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings
import logging
import tempfile
from os import listdir
from io import StringIO
import io

### Connect Blob
account_url = "https://kisnewstorage.blob.core.windows.net"
sas_token = "?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiyx&se=2023-12-30T17:00:17Z&st=2023-05-25T09:00:17Z&spr=https&sig=OHJEj3pyrlNTvn%2Fl0OajlYwdzI72MNPgcgoQYlgARG0%3D"

####Blob Raw
container_check = "check"
blob_service_client_check = BlobServiceClient(account_url=account_url, credential=sas_token)
container_client_check = blob_service_client_check.get_container_client(container=container_check)

####Blob Loc
container_beforecheck = "beforecheck"
blob_service_client_beforecheck = BlobServiceClient(account_url=account_url, credential=sas_token)
container_client_beforecheck = blob_service_client_beforecheck.get_container_client(container=container_beforecheck)

####Blob thaadmrtsditos
container_thaadmrtsditos = "thaadmrtsditos"
blob_service_client_thaadmrtsditos = BlobServiceClient(account_url=account_url, credential=sas_token)
container_client_thaadmrtsditos = blob_service_client_thaadmrtsditos.get_container_client(container=container_thaadmrtsditos)

####Blob worldcountries
container_worldcountries = "worldcountries"
blob_service_client_worldcountries = BlobServiceClient(account_url=account_url, credential=sas_token)
container_client_worldcountries = blob_service_client_worldcountries.get_container_client(container=container_worldcountries)

#####Temp path
tempFilePath = tempfile.gettempdir()

### Download file worldcountries to tmp function
blob_list = container_client_worldcountries.list_blobs()
for blob in blob_list:
    logging.info(blob.name)
    print(blob.name)
    blob_client = container_client_worldcountries.get_blob_client(blob.name)
    with open(os.path.join(tempFilePath,blob.name), mode='wb') as sample_blob:
        download_stream = blob_client.download_blob()
        sample_blob.write(download_stream.readall())
    filesDirListInTemp = listdir(tempFilePath)

### Download file thaadmrtsditos to tmp function
blob_list = container_client_thaadmrtsditos.list_blobs()
for blob in blob_list:
    logging.info(blob.name)
    print(blob.name)
    blob_client = container_client_thaadmrtsditos.get_blob_client(blob.name)
    with open(os.path.join(tempFilePath,blob.name), mode='wb') as sample_blob:
        download_stream = blob_client.download_blob()
        sample_blob.write(download_stream.readall())
    filesDirListInTemp = listdir(tempFilePath)
    
def run():
    start_time = time.time()
    
    ## Global Variable ##
    gc_subdist = gpd.read_file(os.path.join(tempFilePath,'tha_admbnda_adm3_rtsd_20220121.shp'))
    gc_world = gpd.read_file(os.path.join(tempFilePath,'World_Countries__Generalized_.shp'))
    logging.info('Start inverse geocoding!!!')
    file_lst = []
    
    ## Main Script ##
    blob_list = container_client_check.list_blobs()
    for blob in blob_list:
        blobname = blob.name
        logging.info("inverse geo file:" + str(blobname))
        blob_client = container_client_check.get_blob_client(blob.name)
        blobstr = blob_client.download_blob(max_concurrency=1, encoding='UTF-8').readall()
        df = pd.read_csv(StringIO(blobstr))
        gdf = gpd.GeoDataFrame(df, geometry = gpd.points_from_xy(df['longitude'],df['latitude']),crs="EPSG:4326")
        pointInPolys = gpd.sjoin(gdf, gc_subdist, how='left')
        pointInPolys = pointInPolys.drop(['geometry','Shape_Leng','ADM3_EN','ADM2_EN','ADM1_EN','ADM0_EN','ADM3_TH','ADM2_TH','ADM1_TH','ADM0_TH','Shape_Area','ADM3_REF','ADM3ALT1EN','ADM3ALT2EN','ADM3ALT1TH','ADM3ALT2TH','ADM2_PCODE','ADM1_PCODE','ADM0_PCODE','date','validOn','validTo','index_right'],axis = 1)
        pointInWorld = gpd.sjoin(gdf, gc_world, how='left')
        pointInPolys['Country'] = pointInWorld['COUNTRY']
        writer = io.BytesIO()
        pointInPolys.to_csv(writer, index = False)
        blob_client = container_client_beforecheck.get_blob_client(blob.name)
        blob_client.upload_blob(writer.getvalue(), overwrite = True)

    logging.info("--- %s seconds ---" % (time.time() - start_time))

    logging.info(dt.datetime.today())
        
    ## Main Script ##
    blob_list = container_client_beforecheck.list_blobs()
    for blob in blob_list:
        blobname = blob.name
        logging.info("upload file:" + str(blobname))
        try:
            try:
                blob_client = container_client_check.get_blob_client(blob.name)
                blob_client.delete_blob()
                pass
            except:
                logging.info('no raw')
            finally:
                blob_client = container_client_beforecheck.get_blob_client(blob.name)
                with open(os.path.join(tempFilePath,blob.name), mode='wb') as sample_blob:
                    download_stream = blob_client.download_blob()
                    sample_blob.write(download_stream.readall())
                # df_saved = LocToDB.uploadCSV(os.path.join(tempFilePath,blob.name),'Engine_Location_Record')
        except:
            traceback.print_exc()
            continue