import cx_Oracle
import pandas as pd
import csv
import json
import cred
import pandas as pd
import requests, zipfile, io
import pandas as pd
import os
import urllib
import time
import os
import re
from pathlib import Path

#configuration 
main_directory = r"C:\\Users\\Public\\Albion\\Directed Study"
lib_dir = r"C:\Users\Public\Downloads\instantclient-basic-windows.x64-19.12.0.0.0dbru\instantclient_19_12"
file_name="ds.csv"
cx_Oracle.init_oracle_client(lib_dir)
dsn = cx_Oracle.makedsn(host='dbtest01.albion.edu', port=1521, service_name='devl.albion.edu')
#configuration ends here

os.chdir(main_directory)
u = str(cred.user)
p = str(cred.password)
con = cx_Oracle.connect(user=u, password=p, dsn=dsn)
cursor = con.cursor()

def download_file():
    print("downloading new data...!")
    api = str(cred.api)
    sur = str(cred.sur)
    header = {
        "X-API-TOKEN" : api,
        "Content-Type": "application/json",
        "format": "json",
    }
    payload = json.dumps({
      "format": "csv",
      "useLabels": True
    })

    url = 'https://iad1.qualtrics.com/API/v3/surveys/'+sur+'/export-responses'
    responsePost = requests.post(url, headers=header, data=payload).json()
    progressId = responsePost['result']['progressId']
    #print("\nprogressId : {} ".format(progressId) )
    time.sleep(5)

    payload1 = {}
    url1 = 'https://iad1.qualtrics.com/API/v3/surveys/'+sur+'/export-responses/'+progressId
    responseGet = requests.get(url1, headers=header, data=payload1).json()
    fileId = responseGet['result']['fileId']
    #print("\nfileId : {} \n".format(fileId))


    header1 = {
        "X-API-TOKEN": api,
        "Content-Type": "application/json",
    }


    url2 = 'https://iad1.qualtrics.com/API/v3/surveys/'+sur+'/export-responses/'+fileId+'/file'
    responseGet1 = requests.get(url2, headers=header1, data=payload1)

    file = zipfile.ZipFile(io.BytesIO(responseGet1.content))
    file.extractall()
    fileName = file.namelist()
    download_file.fileName = fileName[0]
    if os.path.isfile(file_name):
        os.remove(file_name)
    os.rename(download_file.fileName, file_name)    
    
def insert_file():
    df = pd.read_csv(file_name,skiprows=range(1, c))
    df = df.fillna('NA')
    df=df[['RecordedDate','Q1_1','Q2','Q14','Q15','Q16','Q10']]
    df['RecordedDate'] = pd.to_datetime(df['RecordedDate'])
    df['Q1_1'] = df['Q1_1'].apply(lambda x: '{0:0>9}'.format(x))
    df['Q2'] = df['Q2'].str.replace(r'[^(]*\(|\)[^)]*', '')
    
    df['course']=df['Q14'].astype(str)+'_'+df['Q15']+'_'+df['Q16']
    
    rep_str = ['_NA_','NA_','_NA','NA','\t']
    for i in rep_str:
        df['course'] = df['course'].str.replace(i,'')
            
    drp_df = ['Q14','Q15','Q16']
    for i in drp_df:
        df.drop(i,axis = 'columns', inplace=True)

    try:
        df[['sub','title','crn']] = df['course'].str.split('|',expand=True)
        df[['sub_code','crse_numb']] = df['sub'].str.split(" ",expand=True)
        drp_df = ['title','course','sub']
        for i in drp_df:
            df.drop(i,axis = 'columns', inplace=True)
        print(df)
        try:
            sql = 'CREATE TABLE ZPRNCG("ZPRNCG_STU_ID" VARCHAR2(9 CHAR),"ZPRNCG_TERM_CODE" VARCHAR2(25 CHAR),"ZPRNCG_CRN" VARCHAR2(5 CHAR),"ZPRNCG_SUBJ_CODE" VARCHAR2(4 CHAR),"ZPRNCG_CRSE_NUMB" VARCHAR2(5 CHAR),"ZPRNCG_ACTIVITY_DATE" DATE,"ZPRNCG_SUBMITBY" VARCHAR2(50 CHAR))'
            cursor.execute(sql)
            con.commit()
        except:
            pass
        for i, row in df.iterrows():
            sql = 'INSERT INTO ZPRNCG(ZPRNCG_ACTIVITY_DATE,ZPRNCG_STU_ID,ZPRNCG_TERM_CODE,ZPRNCG_SUBMITBY,ZPRNCG_CRN,ZPRNCG_SUBJ_CODE,ZPRNCG_CRSE_NUMB) VALUES (:1, :2, :3, :4, :5,:6, :7)'
            cursor.execute(sql,row)
            con.commit()
    except:
        print("No data")


    
def rows_count():
    import pandas as pd
    df = pd.read_csv(file_name)
    total_rows = len(df.index)
    return total_rows

if os.path.isfile(file_name):
    c = rows_count()
    c = c+1
    download_file()
    insert_file()
else:
    print("downloading file")
    download_file()
    c = 4
    insert_file()