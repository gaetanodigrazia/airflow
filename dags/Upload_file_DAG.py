from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python import PythonOperator

hook = S3Hook('minio_connection')


def uploadFileToMinIo(filename: str, key: str, bucket_name: str) -> None:
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG(
    dag_id='Upload_File_DAG',
    schedule_interval='@daily',
    start_date=datetime(2022, 8, 5),
    catchup=False
) as dag:
    task_upload_to_minIo = PythonOperator(
        task_id='upload_file_task',
        python_callable=uploadFileToMinIo,
        op_kwargs={
            'filename': '/opt/airflow/files/sample.txt',
            'key': 'sample.txt',
            'bucket_name': 'samplebucket'
        }
    )
