import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
import pandas as pd
import os
import pytz
# import FinanceDataReader as fdr
import html5lib
import shutil

local_tz = pytz.timezone('Asia/Seoul')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8, tzinfo=local_tz),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='get_code',
    default_args=default_args,
    description='get stock code from krx',
    schedule_interval="50 8 * * 1-5",  # 필요에 따라 변경
    catchup=False,
)

# def make_data():
#     if not os.path.isdir("/opt/airflow/stock_data/data"):
#         os.makedirs("/opt/airflow/stock_data/data/")

def get_code():
    krx_url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
    stk_data = pd.read_html(krx_url, header=0, encoding='cp949')[0]
    stk_data = stk_data[['회사명', '종목코드']]
    stk_data = stk_data.rename(columns={'회사명': 'Name', '종목코드': 'Code'})
    stk_data['Code'] = stk_data['Code'].apply(lambda input: '0' * (6 - len(str(input))) + str(input))
    stk_data.to_csv("/opt/airflow/stock_data/code.csv", encoding='utf-8', index=False)

def make_dir():
    if not os.path.exists("/opt/airflow/stock_data/data"):
        os.makedirs("/opt/airflow/stock_data/data", exist_ok=True)

def remove_dir():
    if os.path.isdir("/opt/airflow/stock_data/data"):
        shutil.rmtree("/opt/airflow/stock_data/data")


get_stock_code = PythonOperator(
    task_id='get_stock_code',
    python_callable=get_code,
    dag=dag,
)

mkdir_data = PythonOperator(
    task_id='mkdir_data',
    python_callable=make_dir,
    dag=dag,
)

remove_data = PythonOperator(
    task_id='remove_data',
    python_callable=remove_dir,
    provide_context=True,
    dag=dag,
)

remove_data >> get_stock_code >> mkdir_data