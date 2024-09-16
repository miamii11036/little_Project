#目標:建立ETL模型，在執行python時會自動提取、轉換、載入到目標文件與資料庫中
#目標2:GDP資料單位從百萬美金轉換成十億美金，並只截取經濟規模大於1億美金的國家資料

import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
import requests
from datetime import datetime
import numpy as np

url='https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
csv_path = 'Countries_by_GDP.csv'
table_name = 'Countries_by_GDP'
sql_name = "World_Economies.db"
table_attribs=['Country', 'GDP_USD_million']
log_file='etl_project_log.txt'

#從url提取目標資料並轉換成DataFrame
def extract(url, table_attribs):
    html_page = requests.get(url).text
    html_data = BeautifulSoup(html_page,'html.parser')
    #截取網站中所有的table
    all_table = html_data.find_all('tbody')
    #找到目標table並截取資料
    rows = all_table[2].find_all('tr')
    #初始化目標df的DataFrame
    df = pd.DataFrame(columns=table_attribs)
    
    for row in rows:
        #截取一個row裡頭的單元格資料
        cell = row.find_all('td')
        #設立條件，確保row裡面確實有'td'
        if len(cell) !=0:
            #刪除目標table中的全球資料，只截取tr中第一個td有<a>額外連結且第三個td沒有'—'
            if cell[0].find('a') is not None and '—' not in cell[2]:
                goal_dic = {table_attribs[0]:cell[0].text,
                            table_attribs[1]:cell[2].text}
                goal_df = pd.DataFrame(goal_dic, index=[0])
                df = pd.concat([df,goal_df],ignore_index=True)
    return df


#將GDP資料轉變成數值並換算成十億美金為單位
def transform(df):
    #將'GDP_USD_million'欄中資料的貨幣格式轉成數值格式，好方便做數據清理
    GDP_list = df['GDP_USD_million'].tolist()
    GDP_no_comma = []
    for GDP in GDP_list:
        float_values = float(''.join(GDP.split(',')))
        GDP_no_comma.append(float_values)
    GDP_list = GDP_no_comma

    #將單位轉換成十億美金並更改column name
    GDP_billion = []
    for GDP in GDP_list:
        billion_values = np.round(GDP/1000,2)
        GDP_billion.append(billion_values)
    GDP_list = GDP_billion

    df = df.rename(columns = {'GDP_USD_million':'GDP_USD_billion'})
    df["GDP_USD_billion"] = GDP_list
    #篩選經濟規模大於1億美金的國家
    df = df[df["GDP_USD_billion"]>=0.1]
    return df

#將整理好的df儲存成csv檔案
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_sql(df, table_name, sql_connection):
    df.to_sql(table_name, sql_connection, if_exists="replace",index=False)

#製作ETL執行日誌
def log_progress(message):
    timestamp_format = '%Y-%h-%d:%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open (log_file, 'a') as f:
        f.write(message + ':' + timestamp + '\n')
#製作sql搜尋功能
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

log_progress("啟動ETL")
df = extract(url, table_attribs)
log_progress("Extract 完成")
transform_df = transform(df)
log_progress("Transform 完成")
Load_df = load_to_csv(transform_df,csv_path)
log_progress(f"成功儲存成:{csv_path}")

sql_connection = sqlite3.connect(sql_name)
log_progress(f"成功連結:{sql_name}")

load_sql = load_to_sql(transform_df, table_name,sql_connection)
log_progress(f"成功存入:{sql_connection}")

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billion >= 10"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')
sql_connection.close()
