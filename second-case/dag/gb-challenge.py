from datetime import datetime
import logging
import re
import pandas as pd
import pandas_gbq
import requests
from bs4 import BeautifulSoup
from airflow.models import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.providers.google.cloud.operators.bigquery import (BigQueryCreateEmptyTableOperator, 
BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator)
from airflow.operators.python import PythonOperator
from helper.sqls import *

GIT_URL = 'https://github.com/iuriqc/gb_challenges/tree/main/second-case/files'
REGEX = r"Base_[0-9]{4}\.xlsx$"

PROJECT_ID = 'gb-challenge'
DATASET_ID = 'TABLES'
TABLE_RAW = 'TABLES.RAW'
TABLE_RAW_SCHEMA = [
                {"name": "ID_MARCA", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "MARCA", "type": "STRING", "mode": "REQUIRED"},
                {"name": "ID_LINHA", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "LINHA", "type": "STRING", "mode": "REQUIRED"},
                {"name": "DATA_VENDA", "type": "DATE", "mode": "REQUIRED"},
                {"name": "QTD_VENDA", "type": "INTEGER", "mode": "REQUIRED"},
            ]
CONN_ID = "airflow-to-bq"

TASKS = []

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

    def get_credentials(gcp_conn_id):
        """
        Get credentials from Airflow connection
        """
        gcp = GoogleCloudBaseHook(gcp_conn_id=gcp_conn_id)
        credentials = gcp._get_credentials()

        return credentials

    def fill_raw_table_bq(project_id:str, table_id:str, if_exists:str, table_schema:list):
        """
        Create raw table in BigQuery
        """
        df = get_data_from_git(GIT_URL, REGEX)

        credentials = get_credentials(CONN_ID)

        logging.info("Saving table in BQ")
        pandas_gbq.to_gbq(df, 
                          table_id, 
                          project_id=project_id, 
                          if_exists=if_exists, 
                          credentials=credentials, 
                          table_schema=table_schema,
                          )
        logging.info("Table created")

    start = DummyOperator(task_id="start", dag=dag)

    check_pip = BashOperator(
      task_id="pip_task",
      bash_command='pip freeze',
      #bash_command='pip install openpyxl',
      dag=dag,
  )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        gcp_conn_id=CONN_ID,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        exists_ok=True
    )

    create_raw_table = BigQueryCreateEmptyTableOperator(
            task_id="create_raw_table",
            gcp_conn_id=CONN_ID,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_RAW.split('.')[1],
            schema_fields=TABLE_RAW_SCHEMA
        )

    fill_raw_table = PythonOperator(
        task_id="fill_raw_table",
        python_callable=fill_raw_table_bq,
        op_kwargs={
            "project_id":PROJECT_ID,
            "table_id":TABLE_RAW,
            "if_exists":"replace",
            "table_schema":TABLE_RAW_SCHEMA,
        },
        dag=dag,
    )

    tables_list = [f'TABELA_{i}' for i in range(1,5)]
    for table in tables_list:
        create_view_table = BigQueryInsertJobOperator(
            task_id=f'create_{table}',
            configuration={
                "query": {
                    "query": f'sql_{table}',
                    "useLegacySql": False,
                    "create_disposition": "CREATE_IF_NEEDED",
                    "writeDisposition": "WRITE_TRUNCATE",
                    'destinationTable': {
                        'projectId': PROJECT_ID,
                        'datasetId': DATASET_ID,
                        'tableId': table,
                    },
                }
            }
        )

        fill_raw_table >> create_view_table
        TASKS.append(create_view_table)

    end = DummyOperator(task_id="end", dag=dag)

    chain(
        start,
        check_pip,
        create_dataset,
        create_raw_table,
        fill_raw_table,
        *TASKS,
        end
    )

   # start >> check_pip >> create_dataset >> create_raw_table >> fill_raw_table >> end