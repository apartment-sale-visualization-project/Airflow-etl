import os
from functools import wraps
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from dotenv import dotenv_values
from sqlalchemy import create_engine, inspect
import xml.etree.ElementTree as ET
import requests
from datetime import datetime, timezone
import csv
from dateutil.relativedelta import relativedelta
import pendulum
from functools import wraps
from sqlalchemy.exc import SQLAlchemyError

def connect_db():
    print(f'Connecting to DB, DB name is {CONFIG["POSTGRES_DB"]}')
    connection_uri = "postgresql://{}:{}@{}:{}/postgres".format(
        CONFIG["POSTGRES_USER"],
        CONFIG["POSTGRES_PASSWORD"],
        CONFIG["POSTGRES_HOST"],
        CONFIG["POSTGRES_PORT"],
    )

    engine = create_engine(connection_uri, pool_pre_ping=True, isolation_level='AUTOCOMMIT')
    conn = engine.connect()

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

@task
def start():
    print("Start Task")
    return 1

@task
def extract(start, code, **context):
    year = context["execution_date"].year
    month = context["execution_date"].month
    date = str(year) + str(month).zfill(2)
    payload = "LAWD_CD=" + code + "&" + "DEAL_YMD=" + date + "&" + "serviceKey=" + CONFIG["SERVICE_KEY"] + "&"
    print(f">>> Running extract function: {date}")
    res = requests.get(API_URL + payload)
    item_list = get_items(res)
    return item_list

@task
def transform(raw):
    print(f">>> Running transform function.")
    print(raw)
    use_key = {'거래금액': 'deal_amount', '년': 'deal_year', '월': 'deal_month', '일': 'deal_day', '전용면적': 'area_for_exclusive_use', '지역코드': 'regional_code'}
    transformed=[]
    for sub_dict in raw:
        transformed_dict={}
        for prev_column, new_column in use_key.items():
            transformed_dict[new_column]=sub_dict[prev_column]
        transformed.append(transformed_dict)
    return transformed

@task
def load(transformed, table_name, schema, engine):
    print(f">>> Running load function.")
    print(f"Loading dataframe to DB on table: {table_name}")
    print(transformed)
    df=pd.DataFrame(transformed, columns=['deal_amount', 'deal_year', 'deal_month', 'deal_day', 'area_for_exclusive_use', 'regional_code'])
    df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
    return True

@task
def end(etl):
    print("End Task")
    return 1

with DAG(
    dag_id="apartment_transaction_etl_dag",
    schedule = '0 0 1 * *',
    start_date = pendulum.datetime(2020,1,1)
) as dag:
    start = start()
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ
    API_URL = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?'
    DISTRICT_CODE = {'11110': '서울특별시 종로구', '11140': '서울특별시 중구', '11170': '서울특별시 용산구', '11200': '서울특별시 성동구', '11215': '서울특별시 광진구', '11230': '서울특별시 동대문구', '11260': '서울특별시 중랑구', '11290': '서울특별시 성북구', '11305': '서울특별시 강북구', '11320': '서울특별시 도봉구', '11350': '서울특별시 노원구', '11380': '서울특별시 은평구', '11410': '서울특별시 서대문구', '11440': '서울특별시 마포구', '11470': '서울특별시 양천구', '11500': '서울특별시 강서구', '11530': '서울특별시 구로구', '11545': '서울특별시 금천구', '11560': '서울특별시 영등포구', '11590': '서울특별시 동작구', '11620': '서울특별시 관악구', '11650': '서울특별시 서초구', '11680': '서울특별시 강남구', '11710': '서울특별시 송파구', '11740': '서울특별시 강동구'}

    db_engine = connect_db()
    schema = "raw_data"
    raw_table_name = "apartment_sale_info"
    etl=[]
    for code in DISTRICT_CODE:
        raw_list=transform.override(task_id="transform"+code)(extract.override(task_id="extract"+code)(start, code))
        result = load.override(task_id="load"+code)(raw_list, raw_table_name, schema, db_engine)
        etl.append(result)
    db_engine.dispose()
    end(etl)