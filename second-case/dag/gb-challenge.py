from datetime import datetime
import logging
import re
import pandas as pd
import pandas_gbq
import requests
from bs4 import BeautifulSoup
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import (BigQueryCreateEmptyTableOperator, 
BigQueryCreateEmptyDatasetOperator)
from airflow.operators.python import PythonOperator

GIT_URL = 'https://github.com/iuriqc/gb_challenges/tree/main/second-case/files'
REGEX = r"Base_[0-9]{4}\.xlsx$"

PROJECT_ID = 'gb-challenge'
DATASET_ID = 'tables'
TABLE_RAW = 'tables.raw'

with DAG(
    dag_id="teste",
    schedule_interval=None,
    start_date=datetime(2023, 2, 4),
    catchup=False,
    tags=['teste'],
    description='Teste',
) as dag:

    def get_data_from_git(url:str, files_regex):
        """
        Function to read files from GitHub repo and return a dataframe
        """
        logging.info("Getting data from GitHub")
        reqs = requests.get(url)
        soup = BeautifulSoup(reqs.text, 'html.parser')

        files = []

        for link in soup.find_all('a'):
            result = link.get('href')
            if re.search(files_regex, result):
                files.append(f"https://github.com{result}?raw=true")
            else:
                pass
        
        table = pd.DataFrame()
        
        logging.info("Reading files...")
        for file in files:
            df = pd.read_excel(file)
            table = pd.concat([table,df], ignore_index=True)
        logging.info("Process finished")
        
        return table
    
    def create_raw_table_bq(project_id:str, table_id:str):
        """
        Create raw table in BigQuery
        """
        df = get_data_from_git(GIT_URL, REGEX)

        logging.info("Saving table in BQ")
        pandas_gbq.to_gbq(df, table_id=table_id, project_id=project_id)
        logging.info("Table created")
    
    start = DummyOperator(task_id="start", dag=dag)

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        gcp_conn_id="airflow-to-bq",
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        exists_ok=True
    )

    create_table = BigQueryCreateEmptyTableOperator(
            task_id="create_raw_table",
            gcp_conn_id="airflow-to-bq",
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_RAW.split('.')[1],
            schema_fields=[
                {"name": "ID_MARCA", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "MARCA", "type": "STRING", "mode": "REQUIRED"},
                {"name": "ID_LINHA", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "LINHA", "type": "STRING", "mode": "REQUIRED"},
                {"name": "DATA_VENDA", "type": "DATE", "mode": "REQUIRED"},
                {"name": "QTD_VENDA", "type": "INTEGER", "mode": "REQUIRED"},
            ]
        )
    
    fill_table = PythonOperator(
        task_id="fill_raw_table",
        python_callable=create_raw_table_bq,
        op_kwargs={
            "project_id":PROJECT_ID,
            "table_id":TABLE_RAW,
        },
        dag=dag,
    )

    end = DummyOperator(task_id="end", dag=dag)

    start >> create_dataset >> create_table >> fill_table >> end