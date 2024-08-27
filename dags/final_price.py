import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.models import Variable
import time
import logging
import boto3
import pendulum
import pytz
import os, shutil

from keys import *

local_tz = pytz.timezone('Asia/Seoul')  # 예시로 서울 시간대
today = datetime.now(local_tz).strftime("%Y%m%d")

token = read_token()
appkey = app_key()
appsecret = app_secret()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8, tzinfo=local_tz),
    'retries': 3,
    # 'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    dag_id='final_price',
    default_args=default_args,
    description='오후 6시에 종가 get',
    schedule_interval="0 18 * * 1-5",  # 필요에 따라 변경
    catchup=False,
)

# def read_token():
#     with open('../stock_data/access_token.txt', 'r') as file:
#         access_token = file.read()
#     return access_token

token = read_token()
appkey = app_key()
appsecret = app_secret()

error_list = []

url_base = "https://openapi.koreainvestment.com:9443"
path = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
headers = {
    "Content-Type": "application/json; charset=utf-8",
    "authorization": f"Bearer {token}",
    "appKey": appkey,
    "appSecret": appsecret,
    "personalSeckey": "",
    "tr_id": "FHKST03010100",
    "custtype": "P",
}

def read_id():
    dict_dtype = {'Name': 'str', 'Code': 'str'}
    df = pd.read_csv("/opt/airflow/stock_data/data/code.csv", dtype=dict_dtype)
    stock = list(df['Code'])
    return stock

def get_price(**kwargs):
    df = pd.DataFrame()
    code = read_id()
    for x in code:
        time.sleep(0.06)
        query = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_date_1": f"{today}",
            "fid_input_date_2": f"{today}",
            "fid_input_iscd": f"{x}",
            "fid_period_div_code": "D",
            "fid_org_adj_prc": "0",
        }
        # GET 요청 보내기
        try:
            response = requests.get(url=f"{url_base}{path}", headers=headers, params=query)
            data = response.json()

            # print(data)
            add_df = pd.DataFrame(data['output2'])
            add_df['stock_code'] = f"{x}"
            add_df['hts_avls'] = data['output1']['hts_avls']
            add_df['prdy_vol'] = data['output1']['prdy_vol']
            df = pd.concat([add_df,df], ignore_index=True)

        except Exception as e:
            print(f"Error: {e}")
            error_list.append(f"{x}")

    df.to_csv(f"/opt/airflow/stock_data/data/{today}.csv", encoding='utf-8', index=False)

def upload_file(**kwargs):

    upload = LocalFilesystemToS3Operator(
        task_id='upload_file',
        aws_conn_id='aws_s3_default',
        filename=f'/opt/airflow/stock_data/data/{today}.csv',
        dest_bucket='antsdatalake',
        dest_key=f'once_time/{today}.csv',
        replace=True 
    )
    upload.execute(context=kwargs)

def remove_dir():
    if os.path.isdir("/opt/airflow/stock_data/data"):
        shutil.rmtree("/opt/airflow/stock_data/data")

get_data = PythonOperator(
    task_id='get_final_price',
    python_callable=get_price,
    # op_kwargs={'div_code': "J", 'itm_no': "005930"},
    dag=dag,
)

upload_csv = PythonOperator(
    task_id='upload_csv',
    python_callable=upload_file,
    provide_context=True,
    dag=dag,
)

remove_data = PythonOperator(
    task_id='remove_data',
    python_callable=remove_dir,
    provide_context=True,
    dag=dag,
)

get_data >> upload_csv >> remove_data