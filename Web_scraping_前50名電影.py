#目標:extract網站中的目標table資料並存入SQL資料庫中

import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

#sqlite3是一個輕量級的 SQL 資料庫引擎，具有以下特色
    #自給自足：SQLite 是一個自包含的資料庫引擎，不需要額外的伺服器軟體。
    #無伺服器：SQLite 是無伺服器的，這意味著它不需要單獨的伺服器進程來運行。資料庫文件可以直接在應用程式中打開和操作。
    #零配置：SQLite 不需要任何配置或管理，這使得它非常容易使用。
    #高可靠性：SQLite 被設計為高可靠性和高性能的資料庫引擎，適合嵌入式系統和應用程式。

url= 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db' 
table_name = 'Top_50'
csv_path = '/home/project/top_50_films.csv'
df = pd.DataFrame(columns=['Average Rank','Film','Year'])
count = 0

#從url截取資料成html文檔
html_page = requests.get(url).text
#透過BeautifulSoup將html解析
data = BeautifulSoup(html_page,'html.parser')

#從BeautifulSoup分析的資料中 將所有tobody的資料存近變數table中
tables = data.find_all('tbody') #每個list元素是一個tobody
#從變數table中指定第一個tobody(目標資料table) 並將其中所有row資料存近rows變數中
rows = tables[0].find_all('tr') #每個list元素是一行

for row in rows:
    if count<50: #截取目標資料table的前50行資料
        col = row.find_all('td') #每個list元素為行內的values
        if len(col)!=0: #確保每個values不是NULL
            data_dict = {'Average Rank':col[0].contents[0], #抓取每個tr的第一個td，並返回其第一個元素內容
                        'Film':col[1].contents[0], #抓取每個tr的第二個td，並返回其第一個元素內容
                        'Year':col[2].contents[0],} #抓取每個tr的第三個td，並返回其第一個元素內容
                                                    #或直接使用get_text(strip=True)取代.contents只返回乾淨的文本內容
            df1 = pd.DataFrame(data_dict,index=[0]) 
            df = pd.concat([df, df1],
                            ignore_index = True)
            count+=1 #截取第二個row，直到第51個row
    else:
        break
print(df)
#將df的資料存成csv並命名成'/home/project/top_50_films.csv'
df.to_csv(csv_path)

#將DataFrame資料存入SQL資料庫中
conn = sqlite3.connect(db_name) #在sqlite3搜尋/創建名為db_name('Movies.db')資料庫
#df資料存入名為db_name資料庫中 並命名成table_name('Top_50') 如果table_name已經存在則覆寫 並不讓df的index存入SQL中
df.to_sql(table_name, conn, if_exists='replace', index=False) #conn 是資料庫連接物件
conn.close() #關閉SQL

