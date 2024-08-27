from airflow import DAG
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from datetime import datetime

# DAG 정의
with DAG(dag_id='example_local_to_s3',
         start_date=datetime(2024, 8, 13),
         schedule_interval='@daily') as dag:

    upload_file = LocalFilesystemToS3Operator(
        task_id='upload_file',
        aws_conn_id='aws_s3_default',  # 앞서 설정한 Connection ID
        filename='/opt/airflow/stock_data/test.csv',  # 서버에 있는 CSV 파일 경로
        dest_bucket='antsdatalake',  # 업로드할 S3 버킷 이름
        dest_key='test/test.csv',  # S3 내에서의 파일 경로
        replace=True  # 동일한 키가 존재할 경우 덮어쓸지 여부
    )

