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
main_directory = r"C:\\Users\\Public\\Albion\\Petentation Form"
lib_dir = r"C:\Users\Public\Downloads\instantclient-basic-windows.x64-19.12.0.0.0dbru\instantclient_19_12"
file_name="PF.csv"
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
    df=df[['RecordedDate','Q2_1',"Term code",'Q2_2','Q3',"Q7","Q4","Q8"]]
    df['RecordedDate'] = pd.to_datetime(df['RecordedDate'])
    df['Q2_1'] = df['Q2_1'].apply(lambda x: '{0:0>9}'.format(x))
    print(df)
    try:
        sql = 'CREATE TABLE ZPRPF("ZPRPF_STU_ID" VARCHAR2(9 CHAR),"ZPRPF_STU_EMAIL" VARCHAR2(20 CHAR),"ZPRPF_CLASS" VARCHAR2(20),"ZPRPF_TERM_CODE" VARCHAR2(25 CHAR),"ZPRPF_Exemption Request" VARCHAR2(100 CHAR),"ZPRPF_EXPLAINATION" VARCHAR2(1000 CHAR),"ZPRPF_ACTIVITY_DATE" DATE,"ZPRPF_SUBMITBY" VARCHAR2(50 CHAR))'
        cursor.execute(sql)
        con.commit()
    except:
        pass
    for i, row in df.iterrows():
        sql = 'INSERT INTO ZPRPF("ZPRPF_ACTIVITY_DATE","ZPRPF_STU_ID","ZPRPF_STU_EMAIL","ZPRPF_CLASS","ZPRPF_TERM_CODE","ZPRPF_Exemption Request","ZPRPF_EXPLAINATION","ZPRPF_SUBMITBY") VALUES (:1, :2, :3, :4, :5,:6,:7,:8)'
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
    
def convert_topdf():
    import os
    import glob
    import csv
    from xlsxwriter.workbook import Workbook
    for csvfile in glob.glob(os.path.join('.', '*.csv')):
        workbook = Workbook(csvfile[:-4] + '.xlsx')
        worksheet = workbook.add_worksheet()
        with open(csvfile, 'rt', encoding='utf8') as f:
            reader = csv.reader(f)
            for r, row in enumerate(reader):
                for c, col in enumerate(row):
                    worksheet.write(r, c, col)
        workbook.close()
    workbook = Workbook("PF.xlsx")
    workbook.save("xlsx-to-pdf.pdf", SaveFormat.PDF)

    
convert_topdf()