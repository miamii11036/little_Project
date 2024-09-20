data_URL = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
Table_Attributes_Extraction = ['Name','MC_USD_Billion']
Table_Attributes_final = ['Name','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']
output_CSV_Path = './Largest_banks_data.csv'
Database_name ='Banks.db'
Table_name = 'Largest_banks'
Log_file = 'code_log.txt'

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

    # Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    with open (Log_file,'a')as f:
        f.write(f'{datetime.now()}: {message}\n')


def extract(data_URL, Table_Attributes_Extraction):
    log_progress(f"開始提取資料 {data_URL}")
    try:
        html_page = requests.get(data_URL)
        if html_page.status_code != 200:
            raise Exception(f"HTML資料提取失敗:{data_URL}")
        html_data = BeautifulSoup(html_page.text,'html.parser')
        table = html_data.find_all('tbody')
        rows = table[0].find_all('tr')
        extract_dataframe = pd.DataFrame(columns = Table_Attributes_Extraction)
        for row in rows:
            cell = row.find_all('td')
            if len(cell) != 0:
                extract_dic = {Table_Attributes_Extraction[0]:cell[1].get_text(strip=True),
                                Table_Attributes_Extraction[1]:cell[2].get_text(strip=True)}
                extract = pd.DataFrame(extract_dic, index=[0])
                extract[Table_Attributes_Extraction[1]] = extract[Table_Attributes_Extraction[1]].astype("float64")
                extract_dataframe = pd.concat([extract_dataframe,extract], ignore_index=True) 
        log_progress("資料提取完成")
    except Exception as e:
        log_progress(f"資料提取失敗: {str(e)}")
        raise
    return extract_dataframe

df = extract(data_URL, Table_Attributes_Extraction)

def transform(df, csv_path):
    log_progress("開始轉換匯率")
    try:
        rate_data = pd.read_csv(csv_path)
        #將Currency設置df的index
        rate = rate_data.set_index("Currency")
        #將rate["Rate"]轉變成dictionary
        rate_dic = rate["Rate"].to_dict()
        for col in Table_Attributes_final:
            if col not in df.columns:
                df[col] = None
        df['MC_GBP_Billion'] = [np.round(x*rate_dic['GBP'],2) for x in df['MC_USD_Billion']]
        df['MC_EUR_Billion'] = [np.round(y*rate_dic['EUR'],2) for y in df['MC_USD_Billion']]
        df['MC_INR_Billion'] = [np.round(z*rate_dic['INR'],2) for z in df['MC_USD_Billion']]
        log_progress("資料轉換完成")
        return df
    except FileNotFoundError:
        log_progress(f"匯率資料發生異常: {csv_path}")
        raise
    except Exception as e:
        log_progress(f"轉換期間發生異常:{str(e)}")
        raise

df = transform(df, csv_path)


def load_to_csv(df, output_CSV_Path):
    log_progress(f"開始儲存資料成csv檔案:{output_CSV_Path}")
    try:
        df.to_csv(output_CSV_Path, index=False)
        log_progress(f"儲存資料成csv檔案:{output_CSV_Path}成功")
    except Exception as e:
        log_progress(f"資料儲存成csv檔案時發生異常:{str(e)}")
        raise

load_to_csv(df, output_CSV_Path)


def load_to_db(df, Database_name, Table_name):
    log_progress(f"開始連結{Database_name}")
    try:
        sql_connection = sqlite3.connect(Database_name)
        log_progress(f"連結{Database_name}成功")
    except Exception as e:
        log_progress(f"連結{Database_name}失敗:{str(e)}")
        raise
    log_progress("資料開始存入sqlite3")
    try:
        df.to_sql(Table_name, sql_connection, if_exists="replace",index=False)
        log_progress("資料存入sqlite3成功")
    except Exception as e:
        log_progress(f"資料存入sqlite3時發生異常:{str(e)}")

load_to_db(df, Database_name, Table_name)   

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)