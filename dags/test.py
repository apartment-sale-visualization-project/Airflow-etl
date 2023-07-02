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



default_args = {
    'start_date': pendulum.now().subtract(days=5),
    'schedule_interval': '@daily'
}

with DAG(
    "test_start_date13",
    default_args=default_args
) as dag:
    @task()
    def transform():
        print(f">>> Running transform function.")
    transform()