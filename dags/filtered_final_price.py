import pymysql
import pandas as pd
from datetime import datetime
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

def store_and_delete_data():
    try:
        print("MySQL 연결 시도 중...")
        conn = pymysql.connect(host=host, user=user, passwd=password, db=database)
        print("MySQL 연결 성공!")

        with conn.cursor() as cursor:
            # Step 1: 가장 최근 날짜를 기준으로 데이터를 삽입 또는 업데이트
            recent_date_query = '''
            SELECT MAX(date) FROM once_time;
            '''
            cursor.execute(recent_date_query)
            recent_date = cursor.fetchone()[0]
            print(f"가장 최근 데이터 날짜: {recent_date}")

            filter_query = '''
            INSERT INTO filtered_once_time (stock_code, name, closing_price, date)
            SELECT stock_code, name, closing_price, date
            FROM once_time
            WHERE date = %s
            ON DUPLICATE KEY UPDATE
                closing_price = VALUES(closing_price),
                name = VALUES(name);
            '''
            print(f"최근 데이터 삽입 쿼리 실행 중...\n{filter_query}")
            cursor.execute(filter_query, (recent_date,))
            conn.commit()
            print(f"최근 데이터를 filtered_once_time 테이블에 삽입 또는 업데이트 완료")

            # Step 2: 가장 오래된 날짜를 기준으로 데이터 삭제
            oldest_date_query = '''
            SELECT MIN(date) FROM filtered_once_time;
            '''
            cursor.execute(oldest_date_query)
            oldest_date = cursor.fetchone()[0]
            print(f"가장 오래된 데이터 날짜: {oldest_date}")

            delete_query = '''
            DELETE FROM filtered_once_time
            WHERE date = %s;
            '''
            print(f"오래된 데이터 삭제 쿼리 실행 중...\n{delete_query}")
            cursor.execute(delete_query, (oldest_date,))
            conn.commit()
            print("가장 오래된 데이터를 삭제 완료")

    except Exception as e:
        print(f"오류 발생: {e}")
    
    finally:
        if conn:
            conn.close()
            print("MySQL 연결을 닫았습니다.")

# PythonOperator로 함수 실행 정의
store_and_delete_data_task = PythonOperator(
    task_id='filtered_data',
    python_callable=store_and_delete_data,
    dag=dag,
)
