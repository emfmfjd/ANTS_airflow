import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import time
import logging
import boto3
from datetime import datetime
import pendulum
import pytz
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
import sqlalchemy
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os
import time

from keys import *

local_tz = pytz.timezone('Asia/Seoul')
today = datetime.now(local_tz).strftime("%Y%m%d")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 30, tzinfo=local_tz),
    'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    dag_id='maket_data',
    default_args=default_args,
    description='코스피 코스닥 주가지수 데이터',
    schedule_interval="*/1 9-15 * * 1-5",
    # schedule_interval=None,
    catchup=False,
)

user = 'ants'
password = rds_password()
host= end_point()
port = 3306
database = 'datawarehouse'
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

def get_data(**kwargs):
    url = "https://cyberir.koscom.co.kr/cyberir/main.do?custId=koscom"

    for _ in range(5):
        get_time = datetime.now(local_tz).strftime("%Y%m%d%H%M%S")
        response = requests.get(url)
        xml_data = response.content.decode('utf-8')

        soup = BeautifulSoup(xml_data, features="xml")
        korstocks = soup.find_all('KorStock')
        data = []

        for korstock in korstocks:
            stock_name = korstock.find('StockName').text
            current_point = korstock.find('CurrentPoint').text
            up_down_point = korstock.find('UpDownPoint').text
            up_down_rate = korstock.find('UpDownRate').text
            up_down_flag = korstock.find('UpDownFlag').text.strip()

            data.append({
                'StockName': stock_name,
                'CurrentPoint': current_point,
                'UpDownPoint': up_down_point,
                'UpDownRate': up_down_rate,
                'UpDownFlag': up_down_flag
            })

        df = pd.DataFrame(data)
        df['price_time'] = datetime.strptime(get_time, "%Y%m%d%H%M%S")
        
        file_path = f"/opt/airflow/stock_data/data/market_{today}.csv"
        file_exists = os.path.isfile(file_path)

        # DataFrame을 CSV 파일로 저장 (파일이 존재하면 추가 모드로 저장)
        df.to_csv(file_path, mode='a', index=False, header=not file_exists)
        df.to_sql('market', index=False, if_exists="append", con=engine)
        time.sleep(10)


get_market = PythonOperator(
    task_id='get_market',
    python_callable=get_data,
    provide_context=True,
    dag=dag,
)

get_market