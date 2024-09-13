#目標:在網站的前100部高評價的電影清單中，找到在rotten tomatoes排名前100名、2000年以後發行的電影，並存入SQL

import pandas as pd
import sqlite3
import requests
from bs4 import BeautifulSoup

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
csv_path = '/home/project/top_25_films.csv'
database_name = 'movie.db'
database_table_name = 'top_25_Rotten_tomatoes'
df = pd.DataFrame(columns=['Film','Year','Rotten tomatoes rank'])
count = 0

html_page = requests.get(url).text
#用beautifulsoup解析html
data = BeautifulSoup(html_page,"html.parser")
#從網頁中找到table
total_table = data.find_all('tbody')
#在指定table中提取所有的row data
rows = total_table[0].find_all('tr')

for row in rows:
    #將每個row所含的不同values拆分成list存入cells
    cells = row.find_all('td')
    #確保table中每個cell所含的值不是NULL
    if len(cells)!=0:
        #將year與tomatoes rank的values轉成文本
        year_text = cells[2].text.strip()
        rank_text = cells[3].text.strip()
        #確保year與tomatoes rank的文本內容都是數值後將文本轉成數值
        if  rank_text.isdigit()==True:
            year = int(year_text)
            rank = int(rank_text)
        #只抓取Year為2000年以後且tomatoes排名前100的row儲存入df_dic
            if year >=2000 and rank <=100:
                data_dic={'Film':cells[1].get_text(strip=True), #從row中抓取film的資料並轉成文本
                          'Year':cells[2].get_text(strip=True),
                          'Rotten tomatoes rank':cells[3].get_text(strip=True)}
                df_data = pd.DataFrame(data_dic,index=[0])
                df = pd.concat([df,df_data],ignore_index=True)
            count+=1
print(df)
            
conn = sqlite3.connect(database_name)
df.to_sql(database_table_name, conn, if_exists='replace', index=False)
conn.close
