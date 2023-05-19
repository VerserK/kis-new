import pandas as pd
import pyodbc
import pymssql
import sqlalchemy as sa
import os
import csv
import datetime
import requests
import math
from sys import exit
import numpy as np
import urllib
from . import shapegeocode
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, ContentSettings

start = datetime.datetime.today()
# geopathc = r"C:\Users\akarawat.p\Desktop\Data for Bridge\KIS\world_countries_generalized"
# gc = shapegeocode.geocoder(os.path.join(geopathc,'World_Countries__Generalized_.shp'))

# blob_client = BlobClient.from_blob_url("https://kisnewstorage.blob.core.windows.net/worldcountriesgeneralized/World_Countries__Generalized_.shp?sp=r&st=2023-05-19T02:00:03Z&se=2023-12-31T10:00:03Z&spr=https&sv=2022-11-02&sr=b&sig=aPvt6ij3F5n8WNDqW%2BQUEM%2FNNs8H4UNUkX%2FdrXFUp28%3D")
# with open(file=os.path.join('world_countries', 'World_Countries__Generalized_.shp'), mode="wb") as sample_blob:
#     download_stream = blob_client.download_blob()
#     sample_blob.write(download_stream.readall())
def run():
    gc = shapegeocode.geocoder(os.path.join('Notify_Engine_Location/world_countries', 'World_Countries__Generalized_.shp'))
    print(gc)
# geopath = r"D:\Data for Bridge\KIS\tha_adm_rtsd_itos_20220121_SHP_PART_2"
# thgc = shapegeocode.geocoder(os.path.join(geopath,"tha_admbnda_adm3_rtsd_20220121.shp"))

LineToken = 'upoFwEJRyecKFCBfouLgH0ugnCz3QppFaecAsSsca2M'
LineToken2 = 'pTfbjW6EG1oWMT7rY0N3v50dqRzg038xjSLbHXF9C4y'

# def find_co(thgc,gc,flt,fln,co):
#     try:
#         loc = thgc.geocode(flt, fln,max_dist=0.2)
#         gcc = loc['ADM0_EN']
#     except:
#         try:
#             loc2 = gc.geocode(flt,fln, max_dist = 0.2)
#             gcc = loc2['COUNTRY']
#         except:
#             try:
#                 loc2 = gc.geocode(flt,fln, max_dist = 0.2)
#                 gcc = loc2['COUNTRYAFF']
#             except:
#                 print('cannot find')
#                 gcc = co
#     return gcc

# def func_LineNotify(Message,LineToken):
#     url  = "https://notify-api.line.me/api/notify"
#     msn = {'message':Message}
#     LINE_HEADERS = {"Authorization":"Bearer " + LineToken}
#     session  = requests.Session()
#     response =session.post(url, headers=LINE_HEADERS, data=msn)
#     return response 

# if __name__ == '__main__':
#     file = pd.read_csv(r'D:\Data for Bridge\KIS\API_Record\check_abroad.csv')
#     compare = pd.DataFrame(file,columns=['name'])
#     old_name = compare['name'].tolist()

#     server = 'tcp:consentdb.database.windows.net'
#     database = 'kis-hour-test'
#     username = 'consent-user'
#     password = 'P@ssc0de123'
#     driver = '{ODBC Driver 17 for SQL Server}'  
#     dsn = 'DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+password

#     mydb = pyodbc.connect(dsn)
#     result = pd.read_sql_query('''select positionTime,equipmentName,latitude,longitude,Country from (select * from Engine_location_record as b inner join 
#     (select equipmentName as certain_name, max(positionTime) as latest from Engine_location_record group by equipmentName) as a 
#      on b.equipmentName = a.certain_name and b.positionTime = latest where ADM3_PCODE is null) as c''',mydb)

#     detail_db = pymssql.connect(server='DEV01SVR', user='boon', password='tryTh1$@h0me', database='KisRecord')
#     skl = pd.read_sql_query('''(select Equipment_Name as equipmentName, SKL from Engine_Detail where SKL = '1')''',detail_db)

#     thismess = result.merge(skl, how='inner', on='equipmentName')
#     thismess.drop(columns = 'SKL')

#     print("Finish query")

#     l_time = []
#     l_eq = []
#     l_lat = []
#     l_lon = []
#     l_co = []
#     check_name = []

#     for i in range(len(thismess)):
#         thename = thismess['equipmentName'][i]
#         fco = find_co(thgc, gc, thismess['latitude'][i], thismess['longitude'][i], thismess['Country'][i])
#         if fco != 'Thailand':
#             check_name.append(thename)
#             if thename not in old_name:
#                 if thename not in l_eq:
#                     l_eq.append(thename)
#                     l_time.append(thismess['positionTime'][i])
#                     l_lat.append(thismess['latitude'][i])
#                     l_lon.append(thismess['longitude'][i])
#                     l_co.append(fco)
#                     #print(thismess[th][1],'\ncoor: (',thismess[th][2],',',thismess[th][3],')\n',fco,'\n')
#         else:
#             Message0 = '\n'+str(thename)+' was found near border of Thailand at '+str(thismess['positionTime'][0])
#             print(Message0)
#             Response1 = func_LineNotify(Message0,LineToken2)

#     for name in old_name:
#         if name not in check_name:
#             old_name.remove(name)

#     len_exist = len(old_name)

#     global Current_Date 
#     Current_Date = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

#     if len(l_eq) > 0:
#         old_name+=l_eq
#         print(old_name)
#         update = pd.DataFrame(pd.Series(old_name), columns = ['name'])
#         update = update.to_csv(r'D:\Data for Bridge\KIS\API_Record\check_abroad.csv',index = False)
        
#         totalcount = len(l_eq)
#         total = math.ceil(totalcount/6)
#         count = 0
#         if totalcount > 0:
#             for i in range(1,total+1):
#                 onewin = 6*i - 6
#                 Message1 = "\n----- Today: "+ str(Current_Date)+"-----\n*Total previously reported: "+str(len_exist)+\
#                 "*\n*Total new found today: "+str(len(l_eq))+"*"
#                 for j in range(0,6):
#                     newin = onewin+j
#                     try:
#                         print(l_eq[newin])
#                     except:
#                         break
#                     m = str('\n'+str(count+1)+'. Equipment: '+str(l_eq[newin])+\
#                               '\n - Position: ('+str(l_lat[newin])+', '+str(l_lon[newin])+')'+ \
#                               '\n - LastUpdate: '+str(l_time[newin])+'\n - Country: '+ str(l_co[newin]))
#                     Message1+=m
#                     count+=1
#                     if newin == len(l_eq):
#                         break
#                 print(Message1)
#                 Response1 = func_LineNotify(Message1,LineToken)
#     else:
#         print(old_name)
#         Message1 = "\n----- Today: "+ str(Current_Date)+"-----\n*Total previously reported: "+str(len_exist)+"*\n*Total new found today: 0*"
#         print(Message1)
#         Response1 = func_LineNotify(Message1,LineToken)
#         update = pd.DataFrame(pd.Series(old_name), columns = ['name'])
#         update = update.to_csv(r'D:\Data for Bridge\KIS\API_Record\check_abroad.csv',index = False)

#     end = datetime.datetime.today()
#     print((end-start).total_seconds(),' sec')
