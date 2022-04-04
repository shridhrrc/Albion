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
import numpy as np

#configuration 
main_directory = r"C:\\Users\\Public\\Albion\\Course withdrawl"
#lib_dir = r"C:\Users\Public\Downloads\instantclient-basic-windows.x64-19.12.0.0.0dbru\instantclient_19_12"
file_name = "course withdrawl.csv"
cx_Oracle.init_oracle_client()
dsn = cx_Oracle.makedsn(host='dbtest01.albion.edu', port=1521, service_name='devl.albion.edu')
#configuration ends here

os.chdir(main_directory)
u = str(cred.user)
p = str(cred.password)
con = cx_Oracle.connect(user=u, password=p, dsn=dsn)
cursor = con.cursor()
   
def download_file():
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
    fileName = fileName[0]
    print(fileName)
    if os.path.isfile(file_name):
        os.remove(file_name)
    os.rename(fileName, file_name)
    
def insert_file():
    df = pd.read_csv(file_name,skiprows=range(1, c))
    df.fillna('NA', inplace = True)
    df = df[['RecordedDate','Q1_1','Q2','Q18_1','Q18_2','Q19_1','Q19_2','Q20_1','Q20_2','Q19']] #Based on columns needed to automate
    df['Q1_1'] = df['Q1_1'].apply(lambda x: '{0:0>9}'.format(x))
    df['Q2'] = df['Q2'].str.replace(r'[^(]*\(|\)[^)]*', '')
    df['Course'] = df['Q18_1'].astype(str)+'_'+df['Q19_1']+'_'+df['Q20_1'] #combining 3 column Based on termcode availibility on Qualtrics
    df['TIC'] = df['Q18_2'].astype(str)+'_'+df['Q19_2']+'_'+df['Q20_2'] #combining 3 column Based on termcode availibility on Qualtrics
    df['RecordedDate'] = pd.to_datetime(df['RecordedDate'])
    
    #Replacing of unwanted strings and dropping unwanted columns
    rep_str = ['_NA_','NA_','_NA','NA','\t']
    drp_df = ['Q18_1','Q19_1','Q20_1','Q18_2','Q19_2','Q20_2']
    
    #Functions to replace strings and drop columns
    def replace(replace_data,rep_strings):
        for i in rep_strings:
            df[replace_data] = df[replace_data].str.replace(i,'')
    
    def drop(drop_columns):
        for i in drop_columns:
            df.drop(i,axis = 'columns', inplace=True)
    
    replace('Course', rep_str )
    replace('TIC',rep_str)
    drop(drp_df)
    
    try:
        df[['Course','Course1']] = df['Course'].str.split('|',expand=True)
        df[['CRN','IN_NAME','CT','MD']] = df['TIC'].str.split('|',expand=True)
        df[['SUB_CODE','CRSE_NUM']] = df['Course'].str.split(" ",expand=True)
        
        #dropping of unwanted columns(Data cleaning) 
        drop_columns = ['Course1', 'Course' , 'CT' , 'MD' , 'TIC']
        drop(drop_columns) # sending drop_columns argument to drop function to drop columns
        
        df.CRSE_NUM.fillna(value = '0', inplace=True)
        df['ZPRCWDF_INSTRUCTOR_PIDM'] = 0
        df['ZPRCWDF_STATUS'] = "N"
        df['CRN'] = df['CRN'].str.strip()
        
        print(df)
        try:
            sql = 'CREATE TABLE ZPRCWDF("ZPRCWDF_STU_ID" VARCHAR2(9 CHAR),"ZPRCWDF_TERM_CODE" VARCHAR2(25 CHAR),"ZPRCWDF_CRN" VARCHAR2(5 CHAR),"ZPRCWDF_SUBJ_CODE" VARCHAR2(4 CHAR),"ZPRCWDF_CRSE_NUMB" VARCHAR2(5 CHAR),"ZPRCWDF_INSTRUCTOR_PIDM" NUMBER,"ZPRCWDF_INSTRUCTOR_NAME" VARCHAR2(250 CHAR),"ZPRCWDF_ACTIVITY_DATE" DATE,"ZPRCWDF_SUBMITBY" VARCHAR2(50 CHAR), "ZPRCWDF_STATUS" CHAR(1 CHAR) )'
            cursor.execute(sql)
            con.commit()
        except:
            pass
        for i, row in df.iterrows():
            sql = 'INSERT INTO ZPRCWDF(ZPRCWDF_ACTIVITY_DATE,ZPRCWDF_STU_ID,ZPRCWDF_TERM_CODE,ZPRCWDF_SUBMITBY,ZPRCWDF_CRN,ZPRCWDF_INSTRUCTOR_NAME,ZPRCWDF_SUBJ_CODE,ZPRCWDF_CRSE_NUMB,ZPRCWDF_INSTRUCTOR_PIDM,ZPRCWDF_STATUS) VALUES (:1, :2, :3, :4, :5,:6, :7,:8, :9,:10)'
            cursor.execute(sql,row)
            con.commit()
    except:
        print("No new Data")
   
          

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
    c = rows_count()
    c = 3
    insert_file()
    
    
 