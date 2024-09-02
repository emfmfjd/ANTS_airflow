import pymysql
import requests
import pandas as pd
from datetime import datetime, timedelta

from sqlalchemy import create_engine
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
from io import StringIO

from keys import *

local_tz = pytz.timezone('Asia/Seoul')  # 예시로 서울 시간대
today = datetime.now(local_tz).strftime("%Y%m%d")

token = read_token()
appkey = app_key()
appsecret = app_secret()

types = {'Name':'str','Code':'str'}
code_df = pd.read_csv("/opt/airflow/stock_data/code.csv", dtype=types)

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
    df = pd.read_csv("/opt/airflow/stock_data/code.csv", dtype=dict_dtype)
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

# def to_rds(**kwargs):
#     today = kwargs['ti'].xcom_pull(task_ids='get_realtime_data', key='today')

#     df_types = {'stck_bsop_date' : "str",
#     'stck_clpr' : "int",
#     'stck_oprc' : "int",
#     'stck_hgpr' : "int",
#     'stck_lwpr' : "int",
#     'acml_vol' : "int",
#     'acml_tr_pbmn' : "int",
#     'flng_cls_code' : "int",
#     'prtt_rate' : "float",
#     'mod_yn' : "str",
#     'prdy_vrss_sign' : "int",
#     'prdy_vrss' : "int",
#     'revl_issu_reas' : "str",
#     'hts_avls' : "int",
#     'prdy_vol' : "int",
#     'stock_code' : "str"}

#     # s3랑 연결 설정
#     access = aws_access_key() # git 올릴 때를 위한 암호화 
#     secret = aws_secret()
    
#     s3 = boto3.client(
#         's3',
#         aws_access_key_id= access, 
#         aws_secret_access_key= secret,
#         region_name='ap-northeast-2'
#     )
    
#     bucket_name = 'antsdatalake'
#     folder = 'once_time/' 
    
#     # rds와 연결
#     user = 'ants'
#     password = rds_password()
#     host= end_point()
#     port = 3306
#     database = 'datawarehouse'
#     engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

#     conn = pymysql.connect(host=host, user=user, passwd=password, db=database)

#     s3_key = f"{today}"
#     response = s3.get_object(Bucket=bucket_name, Key=s3_key)
#     csv_content = response['Body'].read().decode('utf-8')

#     df = pd.read_csv(StringIO(csv_content), dtype=df_types) #stock_code가 자동으로 int로 읽어와서 앞에 0이 다 사라져서 dtype 수기로 정해줌...

#     df.drop(columns=['stck_oprc','acml_vol', 'stck_hgpr','stck_lwpr','acml_vol','acml_tr_pbmn','flng_cls_code','prtt_rate','mod_yn','prdy_vrss_sign','prdy_vrss','revl_issu_reas'], inplace=True)
#     df.rename(columns={"stck_bsop_date":"date","stck_clpr":"closing_price","hts_avls":"hts_total","prdy_vol":"prev_trading"}, inplace=True)
#     df = pd.merge(df, code_df, on = 'stock_code', how='left') #stock_code로 name 연결

#     df_columns = ['stock_code', 'name', 'date', 'closing_price', 'hts_total', 'prev_trading'] # 컬럼 순서 지정
#     upload_df = df[df_columns]

#     upload_df.to_sql('once_time', index=False, if_exists="append", con=engine)

#     upload_df['MA5'] = round(upload_df.groupby('stock_code')['closing_price'].transform(lambda x: x.rolling(window=5, min_periods=1).mean()),1) # 이동 평균 처리해주기
#     upload_df['MA20'] = round(upload_df.groupby('stock_code')['closing_price'].transform(lambda x: x.rolling(window=20, min_periods=1).mean()),1)
#     upload_df['MA60'] = round(upload_df.groupby('stock_code')['closing_price'].transform(lambda x: x.rolling(window=60, min_periods=1).mean()),1)
#     upload_df['MA120'] = round(upload_df.groupby('stock_code')['closing_price'].transform(lambda x: x.rolling(window=120, min_periods=1).mean()),1)
#     upload_df['date'] = pd.to_datetime(upload_df['date'], format="%Y%m%d") #date 날짜 형식으로 변경 sql로 보낼때 인식할 수 있게
#     # 날짜 형식으로 보냈는데 왜 sql에서 datetime으로 뜨는지..sql에서 date로 변경처리

# def upload_file(**kwargs):
#     today = kwargs['ti'].xcom_pull(task_ids='get_realtime_data', key='today')

#     upload = LocalFilesystemToS3Operator(
#         task_id='upload_file',
#         aws_conn_id='aws_s3_default',
#         filename=f'/opt/airflow/stock_data/data/{today}.csv',
#         dest_bucket='antsdatalake',
#         dest_key=f'real_time/{today}.csv',
#         replace=True 
#     )
#     upload.execute(context=kwargs)

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