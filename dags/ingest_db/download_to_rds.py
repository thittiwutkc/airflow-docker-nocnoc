from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from io import BytesIO
from sqlalchemy import create_engine
import requests
from ingest_db.default_download_to_rds import default_config
import pytz


def read_data_and_ingest_to_db(file_id, table_name, connection_string):
    download_link = f"https://drive.google.com/uc?id={file_id}"
    response = requests.get(download_link)
    bangkok_timezone = pytz.timezone('Asia/Bangkok')

    if response.status_code == 200:
        byte_io = BytesIO(response.content)
        byte_io.seek(0)
        df = pd.read_csv(byte_io)
        df.columns = df.columns.str.strip().str.lower()
        df.columns = df.columns.str.replace(' ', '_')
        df['timestamp'] = datetime.now(bangkok_timezone)
        engine = create_engine(connection_string)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Ingest_db table:{table_name} Task Done.")

    else:
        print("Failed to download file. Status code:", response.status_code)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 10),
}

with DAG(
    'ingest_data_from_google_drive',
    default_args=default_args,
    description='A DAG to ingest data from Google Drive into a database',
    schedule_interval=None,  
) as dag:
    ingest_task = PythonOperator(
        task_id='ingest_task',
        python_callable=read_data_and_ingest_to_db,
        op_kwargs={'file_id': default_config['url'], 'table_name': default_config['table_name'], 'connection_string': default_config['connection_string']}
    )
