#이 코드 깃허브에 바로 올리면 안됩니다!!!!!!!!!!!!!!!!

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
from bs4 import BeautifulSoup
import os

from keys import *

# 기본 설정
appkey = app_key()
appsecret = app_secret()
url_base = 'https://openapi.koreainvestment.com:9443'
path = '/oauth2/tokenP'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'daily_token',
    default_args=default_args,
    description='Retrieve an access token from Korea Investment API',
    # schedule_interval='0 9 * * *',  # 매일 아침 9시에 실행
    schedule_interval="55 8 * * 1-5",
)

# 토큰 발급 함수
def get_access_token(**kwargs):
    data = {
        "grant_type": "client_credentials",
        "appkey": appkey,
        "appsecret": appsecret
    }

    res = requests.post(url=f"{url_base}{path}", json=data)

    if res.status_code == 200:
        soup = BeautifulSoup(res.text, 'html.parser')
        html_text = soup.get_text()
        token_info = json.loads(html_text)
        access_token = token_info.get('access_token')
        print("Access Token:", access_token)

        # Access Token을 파일로 저장
        with open("/opt/airflow/stock_data/access_token.txt", "w") as token_file:
            token_file.write(access_token)

        # 필요한 경우 XCom을 통해 다른 작업에 토큰 전달
        kwargs['ti'].xcom_push(key='access_token', value=access_token)
    else:
        print("Token 발급 실패:", res.status_code, res.text)
        raise ValueError("Token 발급 실패")

# PythonOperator를 사용하여 함수 호출
get_token_task = PythonOperator(
    task_id='get_access_token',
    python_callable=get_access_token,
    provide_context=True,
    dag=dag,
)

# DAG 실행 순서 정의 (여기서는 하나의 작업만 존재)
get_token_task

