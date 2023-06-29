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

def connect_db():
    print("Connecting to DB")
    connection_uri = "postgresql+psycopg2://{}:{}@{}:{}".format(
        CONFIG["POSTGRES_USER"],
        CONFIG["POSTGRES_PASSWORD"],
        CONFIG["POSTGRES_HOST"],
        CONFIG["POSTGRES_PORT"],
    )
    engine = create_engine(connection_uri, pool_pre_ping=True)
    engine.connect()
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
def extract(code, **context):
    year = context["execution_date"].year
    month = context["execution_date"].month
    date = str(year) + str(month).zfill(2)
    payload = "LAWD_CD=" + code + "&" + "DEAL_YMD=" + date + "&" + "serviceKey=" + CONFIG["SERVICE_KEY"] + "&"
    print(f">>> Running extract function: {date}")
    res = requests.get(API_URL + payload)
    item_list = get_items(res)
    return item_list

@task
def transform(df):
    print(f">>> Running transform function.")
    return df

@task
def load(df, table_name, engine):
    print(f">>> Running load function.")
    print(f"Loading dataframe to DB on table: {table_name}")
    df=pd.DataFrame(df, columns=["거래금액","거래유형","건축년도","년","법정동","아파트", "월", "일", "전용면적","중개사소재지","지번","지역코드","층", "해제사유발생일", "해제여부"])
    df.to_sql(table_name, engine, if_exists="append", index=False)

# @logger
# def etl(**context):
#     db_engine = connect_db()
#     raw_df = extract(context["execution_date"])
#     raw_table_name = "apartment_transaction_table"
#     load_to_db(raw_df, raw_table_name, db_engine)
#     db_engine.dispose()

with DAG(
    dag_id="apartment_transaction_etl_dag",
    schedule_interval=relativedelta(months=1),
    start_date=datetime(2022,8,1),
    catchup=False
) as dag:
    CONFIG = dotenv_values(".env")
    if not CONFIG:
        CONFIG = os.environ
    API_URL = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?'
    DISTRICT_CODE = {'11110': '서울특별시 종로구', '11140': '서울특별시 중구', '11170': '서울특별시 용산구', '11200': '서울특별시 성동구', '11215': '서울특별시 광진구', '11230': '서울특별시 동대문구', '11260': '서울특별시 중랑구', '11290': '서울특별시 성북구', '11305': '서울특별시 강북구', '11320': '서울특별시 도봉구', '11350': '서울특별시 노원구', '11380': '서울특별시 은평구', '11410': '서울특별시 서대문구', '11440': '서울특별시 마포구', '11470': '서울특별시 양천구', '11500': '서울특별시 강서구', '11530': '서울특별시 구로구', '11545': '서울특별시 금천구', '11560': '서울특별시 영등포구', '11590': '서울특별시 동작구', '11620': '서울특별시 관악구', '11650': '서울특별시 서초구', '11680': '서울특별시 강남구', '11710': '서울특별시 송파구', '11740': '서울특별시 강동구'}

    db_engine = connect_db()
    raw_table_name = "apartment_transaction_table"
    for code in DISTRICT_CODE:
        raw_list=transform(extract(code))
        load(raw_list, raw_table_name, db_engine)
    db_engine.dispose()