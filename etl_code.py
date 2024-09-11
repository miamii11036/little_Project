#前置作業
##將終端的目錄引導至這個.py檔案存放的資料夾
##安裝pandas: python3.11 -m pip install pandas
##用終端下載要處理的資料 wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip 
##解壓縮剛剛下載的zip檔案 unzip 檔案名稱


import pandas as pd
import glob as glob
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = 'log_file.text'
target_file = 'transformed_file.csv'

#csv、json、xml檔案資料的提取function
def extract_csv_file(filename):
    csv_df = pd.read_csv(filename)
    return csv_df
def extract_json_file(filename):
    json_df = pd.read_json(filename, lines=True)
    return json_df
def extract_xml_file(filename):
    xml_df = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])
    xml_tree = ET.parse(filename)
    xml_root = xml_tree.getroot()
    for car in xml_root:
        car_model = car.find('car_model').text
        year_of_manufacture = car.find('year_of_manufacture').text
        price = float(car.find('price').text)
        fuel = car.find('fuel').text
        xml_df = pd.concat(
            [xml_df, pd.DataFrame([{'car_model':car_model,'year_of_manufacture':year_of_manufacture,'price':price,'fuel':fuel}])]
            ,ignore_index=True
        )
    return xml_df

#用glob搜尋對應的檔案格式，並套用剛剛創建的各檔案格式的資料提取function
def extract():
    all_data_df = pd.DataFrame(columns=['car_model','year_of_manufacture','price','fuel'])

    for csvfile in glob.glob('*.csv'):
        all_data_df = pd.concat(
            [all_data_df, pd.DataFrame(extract_csv_file(csvfile))],ignore_index = True
        )
    for jsonfile in glob.glob('*.json'):
        all_data_df = pd.concat(
            [all_data_df, pd.DataFrame(extract_json_file(jsonfile))],ignore_index=True
        )
    for xmlfile in glob.glob('*.xml'):
        all_data_df = pd.concat(
            [all_data_df, pd.DataFrame(extract_xml_file(xmlfile))],ignore_index=True
        )
    return all_data_df

#將價格的資料四捨五入取2位數，方便一眼辨識各car_model的價格
def transform(data):
    data['price'] = data['price'].round(2)
    return data

#將轉換好的資料(transformed_file)儲存成csv檔案(target_file = 'transformed_file.csv')
def log_data(target_file, transformed_file):
    transformed_file.to_csv(target_file)

#建立ETL的運行日誌
def log_progress(message):
    timestamp_format = '%Y-%h-%d:%H:%M:%S'
    now=datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file,'a') as f:
        f.write(timestamp + ',' + message +'\n')
log_progress('ETL Job Started')

log_progress('Extract phase Started')
extracted_data = extract()
log_progress('Extract phase Ended')

log_progress('Transform phase Started')
transformed_data = transform(extracted_data)
print("Transformed Data") 
print(transformed_data) 
log_progress("Transform phase Ended")

log_progress("Load phase Started") 
log_data(target_file,transformed_data) 
log_progress("Load phase Ended")
log_progress("ETL Job Ended")  