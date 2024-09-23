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

types = {'Name':'str','Code':'str'}
code_df = pd.read_csv("/opt/airflow/stock_data/code.csv", dtype=types).rename(columns={"Name":"name","Code":"stock_code"})

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

user = 'ants'
password = rds_password()
host= end_point()
port = 3306
database = 'datawarehouse'
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

conn = pymysql.connect(host=host, user=user, passwd=password, db=database)

def read_id():
    dict_dtype = {'Name': 'str', 'Code': 'str'}
    df = pd.read_csv("/opt/airflow/stock_data/code.csv", dtype=dict_dtype)
    stock = list(df['Code'])
    return stock

def get_price(**kwargs):
    global set_rds  # 전역 변수 선언
    df = pd.DataFrame()
    code = read_id()
    for x in code:
        time.sleep(0.06)  # 대기 시간 조정 가능
        query = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_date_1": f"{today}",
            "fid_input_date_2": f"{today}",
            "fid_input_iscd": f"{x}",
            "fid_period_div_code": "D",
            "fid_org_adj_prc": "0",
        }
        try:
            response = requests.get(url=f"{url_base}{path}", headers=headers, params=query)
            data = response.json()

            add_df = pd.DataFrame(data['output2'])
            add_df['stock_code'] = f"{x}"
            add_df['hts_avls'] = data['output1']['hts_avls']
            add_df['prdy_vol'] = data['output1']['prdy_vol']
            df = pd.concat([add_df, df], ignore_index=True)

        except Exception as e:
            logging.error(f"Error fetching data for stock code {x}: {e}")
            error_list.append(f"{x}")

    logging.info("Data fetching complete.")

    if not df.empty:
        df.to_csv(f"/opt/airflow/stock_data/data/{today}.csv", encoding='utf-8', index=False)
        df.drop(columns=['stck_oprc', 'acml_vol', 'stck_hgpr', 'stck_lwpr', 'acml_vol', 'acml_tr_pbmn', 'flng_cls_code', 'prtt_rate', 'mod_yn', 'prdy_vrss_sign', 'prdy_vrss', 'revl_issu_reas'], inplace=True)
        df.rename(columns={"stck_bsop_date": "date", "stck_clpr": "closing_price", "hts_avls": "hts_total", "prdy_vol": "prev_trading"}, inplace=True)
        df = pd.merge(df, code_df, on='stock_code', how='left')

        df_columns = ['stock_code', 'name', 'date', 'closing_price', 'hts_total', 'prev_trading']
        upload_df = df[df_columns]

        upload_df.to_sql('once_time', index=False, if_exists="append", con=engine)
        set_rds = True  # RDS 업데이트를 위한 플래그 설정
    else:
        set_rds = False

def to_rds():
    if set_rds == True:
        select_sql = "SELECT * FROM `once_time` WHERE `date` >= CURDATE() - INTERVAL 180 DAY;"
        rds_df = pd.read_sql(select_sql, conn)
        rds_df['date'] = pd.to_datetime(rds_df['date'])
        rds_df = rds_df.sort_values(by=['stock_code', 'date'])
        print("read 완료")

        # 업데이트할 날짜
        target_date = pd.Timestamp(f'{today}')
        rds_df[rds_df['date'] == target_date]

        rds_df['MA5'] = rds_df.groupby('stock_code')['closing_price'].transform(lambda x: x.rolling(window=5, min_periods=1).mean()).round(1)
        rds_df['MA20'] = rds_df.groupby('stock_code')['closing_price'].transform(lambda x: x.rolling(window=20, min_periods=1).mean()).round(1)
        rds_df['MA60'] = rds_df.groupby('stock_code')['closing_price'].transform(lambda x: x.rolling(window=60, min_periods=1).mean()).round(1)
        rds_df['MA120'] = rds_df.groupby('stock_code')['closing_price'].transform(lambda x: x.rolling(window=120, min_periods=1).mean()).round(1)

        # 특정 날짜의 데이터만 추출
        subset_df = rds_df[rds_df['date'] == target_date]

        # 배치 업데이트를 위한 SQL 쿼리 생성
        update_sql = """
            UPDATE `once_time` 
            SET MA5 = %s, MA20 = %s, MA60 = %s, MA120 = %s
            WHERE stock_code = %s AND `date` = %s
        """

        # 데이터베이스 커넥션 및 커서 오픈
        with conn.cursor() as cursor:
            data = [
                (row['MA5'], row['MA20'], row['MA60'], row['MA120'], row['stock_code'], row['date']) 
                for _, row in subset_df.iterrows()
            ]
            cursor.executemany(update_sql, data)
            conn.commit()



get_data = PythonOperator(
    task_id='get_final_price',
    python_callable=get_price,
    dag=dag,
)

update_rds = PythonOperator(
    task_id='update_rds',
    python_callable=to_rds,
    provide_context=True,
    dag=dag,
)

get_data >> update_rds