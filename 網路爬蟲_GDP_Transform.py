#目標1:提取國際貨幣基金組織 (IMF)
#目標2:將GDP資料的單位從百萬美元自動轉換成十億美元(小數點2位)
#目標3:將整理好的資料載入指定的資料庫中，資料庫只會有經濟規模超過1億美元的國家資料

import requests #用於提取url資料
import pandas as pd #用於數據整理與轉換
from bs4 import BeautifulSoup #用於解析HTML檔案
import sqlite3 #目標資料庫
import numpy as np #用於數據整理與轉換

url='https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'

final_csv_file = "Countries_by_GDP.csv"
DB_name = "World_Economies.db"
table_inDB_name = "Countries_by_GDP"
df = pd.DataFrame(columns=['Country','UN region','2023年GDP'])

#提取url資料並存成文本
html_page = requests.get(url).text
#解析HTML檔案
html_data = BeautifulSoup(html_page,"html.parser")
#將檔案中所有的table存入變量中
all_table = html_data.find_all('tbody') #每個元素為一個table
#找到目標table並將裡頭所有row存入變量中 
rows = all_table[2].find_all('tr') #每個元素為一個row

#將rows的資料轉成DataFrame
for row in rows:
    cell = row.find_all('td') #每個元素為一個行內單元格
    if len(cell)>0: #確保提取的<td>不是NULL
        df_dic = {"Country":cell[0].text,
                  "UN region":cell[1].text,
                  "2023年GDP":cell[2].text}
        dic_df = pd.DataFrame(df_dic,index=[0])
        df = pd.concat([df,dic_df],ignore_index=True)
#Transform目標
# 1.刪除全球統整資料
# 2.2023年GDP的資料刪除逗號、轉變成數值、單位變成十億美元
# 3.只篩選經濟規模為1億的國家:刪除GDP資料為NaN與小於1億的國家

df.drop(index=[0], axis=0,inplace=True)
df.reset_index(drop=True, inplace=True)

df['2023年GDP'] = df['2023年GDP'].str.replace(',','')
df['2023年GDP']=pd.to_numeric(df['2023年GDP'], errors='coerce')
df['2023年GDP'] = (df['2023年GDP']/10**6).round(2)

df = df.dropna(subset=['2023年GDP'])
up_1_billion= df[(df['2023年GDP']>=1.00)]

up_1_billion.rename(columns={'2023年GDP':'2023年GDP(十億美元)'},inplace=True)

#Load，將整理好的DataFrame轉入目標CSV檔案
up_1_billion.to_csv(final_csv_file)
#載入目標database
conn = sqlite3.connect(DB_name)
up_1_billion.to_sql(table_inDB_name, conn, if_exists='replace', index=False)
conn.close()

