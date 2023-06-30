import os

from functools import wraps

import pandas as pd

from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from dotenv import dotenv_values
from sqlalchemy import create_engine, inspect

import xml.etree.ElementTree as ET
import requests
from datetime import datetime, timezone

import csv
from dateutil.relativedelta import relativedelta


from sqlalchemy.exc import SQLAlchemyError


CONFIG = dotenv_values(".env")
if not CONFIG:
    CONFIG = os.environ


API_URL = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?'
DISTRICT_CODE = {'11110': '서울특별시 종로구', '11140': '서울특별시 중구', '11170': '서울특별시 용산구', '11200': '서울특별시 성동구', '11215': '서울특별시 광진구', '11230': '서울특별시 동대문구', '11260': '서울특별시 중랑구', '11290': '서울특별시 성북구', '11305': '서울특별시 강북구', '11320': '서울특별시 도봉구', '11350': '서울특별시 노원구', '11380': '서울특별시 은평구', '11410': '서울특별시 서대문구', '11440': '서울특별시 마포구', '11470': '서울특별시 양천구', '11500': '서울특별시 강서구', '11530': '서울특별시 구로구', '11545': '서울특별시 금천구', '11560': '서울특별시 영등포구', '11590': '서울특별시 동작구', '11620': '서울특별시 관악구', '11650': '서울특별시 서초구', '11680': '서울특별시 강남구', '11710': '서울특별시 송파구', '11740': '서울특별시 강동구'}
SERVICE_KEY='YWKgXMNRjvzmBvtVLad5cydlsspUnFx%2Byn7eNLwWhNwyl1qes%2FwXJUULt5yG8ZuEmwgyUW8KL5ScyobVXvYiQQ%3D%3D'

def get_items(response):
    root = ET.fromstring(response.content)
    item_list = []
    for child in root.find('body').find('items'):
        elements = child.findall('*')
        data = {}
        for element in elements:
            tag = element.tag.strip()
            text = element.text.strip()
            data[tag] = text
        item_list.append(data)  
    return item_list

def extract():
    # year = execution_date.year
    # month = execution_date.month
    # date = str(year) + str(month).zfill(2)
    
    date = '202001'
    items_list = []
    for code in DISTRICT_CODE:
        payload = "LAWD_CD=" + code + "&" + "DEAL_YMD=" + date + "&" + "serviceKey=" + SERVICE_KEY + "&"
        res = requests.get(API_URL + payload)
        item_list = get_items(res)
        items_list.extend(item_list)
    
    items_df = pd.DataFrame(items_list)
    return items_df


def connect_db():
    print(f'Connecting to DB, DB name is {CONFIG["POSTGRES_DB"]}')
    connection_uri = connection_uri = "postgresql://{}:{}@{}:{}/postgres".format(
        CONFIG["POSTGRES_USER"],
        CONFIG["POSTGRES_PASSWORD"],
        CONFIG["POSTGRES_HOST"],
        CONFIG["POSTGRES_PORT"],
    )

    engine = create_engine(connection_uri, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
    conn = engine.connect()
    
    try:
        engine.execute(f'CREATE DATABASE {CONFIG["POSTGRES_DB"]}')
        print(f'crate Database: {CONFIG["POSTGRES_DB"]}')
    except:
        print("Database already exists")
        pass
    
    try:
        engine.execute(f'CREATE SCHEMA raw_data')
        print("create schema: raw_data")
    except:
        print("schema already exists")
        pass
    
    try:
        engine.execute('''
                    CREATE TABLE raw_data.apartment_sale_info(
                    id SERIAL PRIMARY KEY,
                    deal_amount varchar(40),
                    deal_year varchar(4),
                    deal_month varchar(2),
                    deal_day varchar(2),
                    area_for_exclusive_use varchar(10),
                    regional_code varchar(5))
                    ''')
        print("create table: apartment_sale_info")
    except:
        print("table already exists")
        pass
    
    conn.close()
    return engine

engine = connect_db()

raw_df = extract()
print(raw_df)
dfresult = raw_df[['거래금액', '년', '월', '일', '전용면적', '지역코드']]
table_name = 'apartment_sale_info'
try:
    dfresult.to_sql(table_name, con=engine, if_exists='append', index=False)
except SQLAlchemyError as e:
    print("Data insertion failed. Error:", str(e))

engine.dispose()
