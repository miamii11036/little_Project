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
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d:%H:%M:%S'
    now = datetime.now()
    time_stamp = now.strftime(timestamp_format)
    with open (Log_file,'a')as f:
        f.write(message + ':' + time_stamp + '\n')


def extract(data_URL, Table_Attributes_Extraction):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(data_URL).text
    html_data = BeautifulSoup(html_page,'html.parser')
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
    return extract_dataframe

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
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
    return df

def load_to_csv(df, output_CSV_Path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_CSV_Path)


def load_to_db(df, sql_connection, Table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(Table_name, sql_connection, if_exists="replace",index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


log_progress("Preliminaries complete. Initiating ETL process")
extract_df = extract(data_URL, Table_Attributes_Extraction)
log_progress("Data extraction complete. Initiating Transformation process")
df = transform(extract_df,csv_path)
log_progress("Data transformation complete. Initiating Loading process")
load_to_csv(df,output_CSV_Path)
log_progress("Data saved to CSV file")
sql_connection = sqlite3.Connection(Database_name)

log_progress("SQL Connection initiated")
load_to_db(df,sql_connection,Table_name)
log_progress("Data loaded to Database as a table, Executing queries")

query_statement = "SELECT * FROM Largest_banks"
run_query(query_statement,sql_connection)
query_statement_2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(query_statement_2,sql_connection)
query_statement_3 = "SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement_3,sql_connection)

log_progress("Process Complete")
sql_connection.close()
log_progress("Server Connection closed")