import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
import pandas as pd
import os
import pytz
import requests
import json
from sqlalchemy import create_engine
import logging
import shutil
# import FinanceDataReader as fdr
# import html5lib

from keys import *

local_tz = pytz.timezone('Asia/Seoul')
# today = datetime.now(local_tz).strftime("%Y%m%d")

url = "https://openapi.naver.com/v1/search/news.json"

header = {
    "X-Naver-Client-Id" : naver_client(),
    "X-Naver-Client-Secret":naver_secret()
}

types = {'Name':'str','Code':'str'}
search_name = list(pd.read_csv("/opt/airflow/stock_data/code.csv")['Name'])
name_df = pd.read_csv("/opt/airflow/stock_data/code.csv", dtype=types).rename(columns={"Name":"name","Code":"stock_code"})

# rds와 연결
user = 'ants'
password = rds_password()
host= end_point()
port = 3306
database = 'datawarehouse'
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 28, tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='get_news',
    default_args=default_args,
    description='뉴스데이터 하루 1번',
    schedule_interval="0 */3 * * *",  # 필요에 따라 변경
    catchup=False,
)

def get_data(**kwargs):
    today = (datetime.now(local_tz) - timedelta(hours=3)).strftime("%Y%m%d%H%M")
    kwargs['ti'].xcom_push(key="today", value=today)
    rds_df = pd.DataFrame()
    for x in search_name:    
        param = {
            "query" : f"{x}",
            "display" : 100,
            "start" : 1,
            "sort" : "sim",
        }
        r = requests.get(url, params=param, headers=header)
        data = r.json()['items']
        
        with open(f"/opt/airflow/stock_data/data/{today}_{x}.json", "w", encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        df = pd.DataFrame(data)
        df['pubDate'] = pd.to_datetime(df['pubDate'], format='%a, %d %b %Y %H:%M:%S %z')
        df = df[df['pubDate'] >= f"{today}"]
        df['name'] = f"{x}"
        df.drop(columns="link", inplace=True)
        rds_df = pd.concat([rds_df,df], ignore_index=True)
        rds_df['title'] = rds_df['title'].str.replace('<b>', '').str.replace('</b>', '')
        rds_df['description'] = rds_df['description'].str.replace('<b>', '').str.replace('</b>', '')
        logging.info(f"{x} 완료")
        
    rds_df = pd.merge(rds_df, name_df, on = 'name', how='left')
    rds_df.drop_duplicates(subset=['title'], inplace=True)
    columns = ['stock_code','name','pubDate','title','description','originallink']
    rds_df = rds_df[columns]
    rds_df.to_csv(f"/opt/airflow/stock_data/data/news_{today}.csv")
    rds_df.to_sql('news', index=False, if_exists="append", con=engine)q


def upload_file(**kwargs):
    today = kwargs['ti'].xcom_pull(key="today", task_ids="get_news")
    for x in search_name:
        upload = LocalFilesystemToS3Operator(
            task_id='upload_file',
            aws_conn_id='aws_s3_default',
            filename=f'/opt/airflow/stock_data/data/{today}_{x}.json',
            dest_bucket='antsdatalake',
            dest_key=f'news/{today}_{x}.json',
            replace=True 
        )
        upload.execute(context=kwargs)
        logging.info(f"{x} 업로드 완료")

get_news = PythonOperator(
    task_id='get_news',
    python_callable=get_data,
    dag=dag,
)

upload_json = PythonOperator(
    task_id='upload_json',
    python_callable=upload_file,
    provide_context=True,
    dag=dag,
)


get_news >> upload_json