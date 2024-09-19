import pymysql
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
import pytz
from keys import *

# 서울 시간대 설정
local_tz = pytz.timezone('Asia/Seoul')
today = datetime.now(local_tz).strftime("%Y%m%d")

# RDS 연결 정보
user = 'ants'
password = rds_password()
host = end_point()
port = 3306
database = 'datawarehouse'
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

# 기본 인자 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 8, tzinfo=local_tz),
    'retries': 3,
}

# DAG 정의
dag = DAG(
    dag_id='filtered_data',
    default_args=default_args,
    description='once_time 테이블에서 최근 데이터를 저장하고 오래된 데이터를 삭제하는 DAG',
    schedule_interval="30 18 * * 1-5",  # 평일 오후 6시 30분에 실행
    catchup=False,
)

# Python 함수 정의
def store_and_delete_data():
    conn = pymysql.connect(host=host, user=user, passwd=password, db=database)
    
    # Step 1: once_time 테이블에서 가장 최근 데이터를 가져옴
    query_once_time = "SELECT * FROM `once_time` ORDER BY `date` DESC "
    
    latest_data = pd.read_sql(query_once_time, conn)
    
    # Step 2: 가져온 데이터를 filtered_once_time 테이블에 저장
    latest_data = latest_data[['stock_code', 'name', 'closing_price', 'date']]
    latest_data.to_sql(name='filtered_once_time', con=engine, if_exists='append', index=False)

    latest_data.to_sql(name='filtered_once_time', con=engine, if_exists='append', index=False)
    print(f"가장 최근 데이터를 filtered_once_time 테이블에 저장했습니다: {latest_data}")
    
    # Step 3: filtered_once_time 테이블에서 가장 오래된 데이터 삭제
    query_oldest = "SELECT `date` FROM `filtered_once_time` ORDER BY `date` ASC "
    oldest_date = pd.read_sql(query_oldest, conn).iloc[0]['date']
    
    delete_query = f"DELETE FROM `filtered_once_time` WHERE `date` = '{oldest_date}'"
    
    with conn.cursor() as cursor:
        cursor.execute(delete_query)
        conn.commit()
    
    conn.close()
    print(f"오래된 데이터가 삭제되었습니다: {oldest_date}")

# PythonOperator로 함수 실행 정의
store_and_delete_data_task = PythonOperator(
    task_id='filtered_data',
    python_callable=store_and_delete_data,
    dag=dag,
)
