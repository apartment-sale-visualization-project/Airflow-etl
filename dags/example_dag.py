from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 8, 1),  # DAG 시작 날짜 설정
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'example_dag1',
    default_args=default_args,
    description='Example DAG',
    schedule_interval='@monthly'
    # schedule_interval='0 0 * * *',  # 매일 자정에 실행하도록 스케줄 설정
) as dag:
    @task()
    def get_start()->dict:
        return {'id':'start'}

    get_start()