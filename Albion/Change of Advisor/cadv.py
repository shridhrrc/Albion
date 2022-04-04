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
main_directory = r"C:\\Users\\Public\\Albion\\Change of Advisor"
lib_dir=r"C:\Users\Public\Downloads\instantclient-basic-windows.x64-19.12.0.0.0dbru\instantclient_19_12"
file_name="change of advisor.csv"
cx_Oracle.init_oracle_client(lib_dir)
dsn = cx_Oracle.makedsn(host='dbtest01.albion.edu', port=1521, service_name='devl.albion.edu')
#configuration ends here

os.chdir(main_directory)
u=str(cred.user)
p=str(cred.password)
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
    os.rename(download_file.fileName, file_name)

def insert_file():
    df = pd.read_csv(file_name,skiprows=range(1, c))
    df=df[['RecordedDate','Q2_1',"Term code",'Q9','Q5',"Advisor ID","Advisor pidm","Advisor email_Address"]]
    df['RecordedDate'] = pd.to_datetime(df['RecordedDate'])
    df['Q2_1'] = df['Q2_1'].apply(lambda x: '{0:0>9}'.format(x))
    df['dept_code']="NULL"
    df['a']="NULL"
    df['b']="N"
    df['c']="NULL"
    df['d']="NULL"
    df['e']="2001-01-01"
    df['e'] = pd.to_datetime(df['e'])
    df['Advisor ID'] = df['Advisor ID'].str.replace("'", ' ')
    print(df)
    try:
        sql = 'CREATE TABLE ZPRADVR("ZPRADVR_STU_ID" VARCHAR2(9 CHAR),"ZPRADVR_TERM_CODE" VARCHAR2(25 CHAR),"ZPRADVR_NEW_ADVISOR_NAME" VARCHAR2(150 CHAR),"ZPRADVR_ADVISOR_ID" VARCHAR2(10 CHAR),"ZPRADVR_ADVISOR_PIDM" NUMBER(8,0),"ZPRADVR_ADVISOR_EMAIL_ID" VARCHAR2(25 CHAR),"ZPRADVR_DEPT_CODE" VARCHAR2(20 CHAR),"ZPRADVR_DEPT_NAME" VARCHAR2(50 CHAR),"ZPRADVR_ACTIVITY_DATE" DATE,"ZPRADVR_SUBMITBY" VARCHAR2(50 CHAR),"ZPRADVR_STATUS" CHAR(1 CHAR), "ZPRADVR_ADVR_CODE" VARCHAR2(10 CHAR), "ZPRADVR_EMAIL_STATUS" VARCHAR2(20 CHAR), "ZPRADVR_EMAIL_SENT_DT" DATE)'
        cursor.execute(sql)
        con.commit()
    except:
        pass
    for i, row in df.iterrows():
        sql = 'INSERT INTO ZPRADVR("ZPRADVR_ACTIVITY_DATE","ZPRADVR_STU_ID","ZPRADVR_TERM_CODE","ZPRADVR_NEW_ADVISOR_NAME","ZPRADVR_SUBMITBY" ,"ZPRADVR_ADVISOR_ID","ZPRADVR_ADVISOR_PIDM","ZPRADVR_ADVISOR_EMAIL_ID","ZPRADVR_DEPT_CODE","ZPRADVR_DEPT_NAME","ZPRADVR_STATUS","ZPRADVR_ADVR_CODE","ZPRADVR_EMAIL_STATUS","ZPRADVR_EMAIL_SENT_DT") VALUES (:1, :2, :3, :4, :5,:6,:7,:8,:9,:10,:11,:12,:13,:14)'
        cursor.execute(sql,row)
        con.commit()


def rows_count():
    import pandas as pd
    df=pd.read_csv(file_name)
    total_rows = len(df.index)
    return total_rows

if os.path.isfile(file_name):
    c=rows_count()
    c=c+1
    download_file()
    insert_file()
else:
    print("downloading file")
    download_file()
    c=rows_count()
    c=4
    insert_file()

