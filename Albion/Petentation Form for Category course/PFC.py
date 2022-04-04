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
main_directory = r"C:\\Users\\Public\\Albion\\Petentation Form for Category course"
file_name="PFC.csv"
cx_Oracle.init_oracle_client()
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
    df=df[['RecordedDate','Q2_1','Q2_2','Q2_3','Q1','Q4','Q9']]
    df['RecordedDate'] = pd.to_datetime(df['RecordedDate'])
    df['Q1'] = pd.to_datetime(df['Q1'])
    df['Q2_1'] = df['Q2_1'].apply(lambda x: '{0:0>9}'.format(x))
    print(df)
    try:
        sql = 'CREATE TABLE ZPRPFC("ZPRPFC_STU_ID" VARCHAR2(9 CHAR),"ZPRPFC_INSTITUTION_ATTENDED" VARCHAR2(25 CHAR),"ZPRPFC_Course_Number_and_Title" VARCHAR2(25),"ZPRPFC_Semester_of_attendance" DATE,"ZPRPFC_detailed_rationale" VARCHAR2(1000 CHAR),"ZPRPFC_ACTIVITY_DATE" DATE,"ZPRPFC_SUBMITBY" VARCHAR2(50 CHAR))'
        cursor.execute(sql)
        con.commit()
    except:
        pass
    for i, row in df.iterrows():
        sql = 'INSERT INTO ZPRPFC("ZPRPFC_ACTIVITY_DATE","ZPRPFC_STU_ID","ZPRPFC_INSTITUTION_ATTENDED","ZPRPFC_Course_Number_and_Title","ZPRPFC_Semester_of_attendance","ZPRPFC_detailed_rationale","ZPRPFC_SUBMITBY") VALUES (:1, :2, :3, :4, :5,:6,:7)'
        cursor.execute(sql,row)
        con.commit()


    
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
    c = 3
    insert_file()