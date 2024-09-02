import requests
import json
import pandas as pd
from datetime import datetime, timedelta
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

from keys import *

# def pull_token(**kwargs):
#     token = kwargs['ti'].xcom_pull(task_ids='push_task', key='access_token')
#     return token

local_tz = pytz.timezone('Asia/Seoul')  # 예시로 서울 시간대

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
    dag_id='stock_api',
    default_args=default_args,
    description='실시간 주식데이터 api 5분마다 실행',
    schedule_interval="*/5 9-15 * * 1-5",
    # schedule_interval=None,
    catchup=False,
)

def url_fetch(error_list, tr_cont, params, appendHeaders=None, postFlag=False, **kwargs):
    url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appKey": appkey,
        "appSecret": appsecret,
        "personalSeckey": "",
        "tr_id": "FHKST01010100",
        "tr_cont": tr_cont,
        "custtype": "P",
        "seq_no": "",
        "mac_address": mac_add(),
        "phone_num": phone(),
        "ip_addr": "54.181.1.178",
        "hashkey": "",
        "gt_uid": ""
    }

    if appendHeaders:
        headers.update(appendHeaders)

    try:
        res = requests.get(url, headers=headers, params=params)
        
        res.raise_for_status()  # Raise an exception for HTTP errors

        return res.json()  # Return JSON response
    except requests.exceptions.RequestException as e:
        #print(f"Error: {e}")
        error_list.append(params['FID_INPUT_ISCD'])
        logging.error(f"Error: {e}")
        return None

def get_inquire_price(error_list, div_code="J", itm_no="", tr_cont=""):
    params = {
        "FID_COND_MRKT_DIV_CODE": div_code,
        "FID_INPUT_ISCD": itm_no
    }
    res = url_fetch(error_list, tr_cont, params)

    if res:
        current_data = pd.DataFrame([res['output']])
        current_data['stock_code'] = f"{itm_no}"
        return current_data
    else:
        return None


def read_id():
    dict_dtype = {'Name': 'str', 'Code': 'str'}
    df = pd.read_csv("/opt/airflow/stock_data/code.csv", dtype=dict_dtype)
    stock = list(df['Code'])
    return stock

def get_price(**kwargs):
    today = datetime.now(local_tz).strftime("%y%m%d%H%M")
    kwargs['ti'].xcom_push(key = "today",value=today)
    dataframes = []

    error_list = []
    stock = read_id()
    for x in stock:
        inquire_data = get_inquire_price(error_list, div_code="J", itm_no=f"{x}")
        dataframes.append(inquire_data)
        time.sleep(0.03)
    res_df = pd.concat(dataframes, ignore_index=True)
    logging.info("한바퀴 완료")
    df = []
    while error_list:
        for x in error_list:
            inquire_data = get_inquire_price(error_list, div_code="J", itm_no=f"{x}")
            df.append(inquire_data)
            time.sleep(0.03)
            error_list.remove(x)
    #res_df2 = pd.concat(dataframes, ignore_index=True)
    #final_df = pd.concat([res_df, res_df2], ignore_index=True)
    if df:
        res_df2 = pd.concat(df, ignore_index=True)
        final_df = pd.concat([res_df, res_df2], ignore_index=True)
    else:
        final_df = res_df
    final_df['price_time'] = f"{today}"
    final_df.to_csv(f"/opt/airflow/stock_data/data/{today}.csv", encoding='utf-8', index=False)

def upload_file(**kwargs):
    today = kwargs['ti'].xcom_pull(task_ids='get_realtime_data', key='today')

    upload = LocalFilesystemToS3Operator(
        task_id='upload_file',
        aws_conn_id='aws_s3_default',
        filename=f'/opt/airflow/stock_data/data/{today}.csv',
        dest_bucket='antsdatalake',
        dest_key=f'real_time/{today}.csv',
        replace=True 
    )
    upload.execute(context=kwargs)

# Fetch data task
get_data = PythonOperator(
    task_id='get_realtime_data',
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

# Task dependencies
get_data >> upload_csv