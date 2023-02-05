from datetime import datetime
import logging
import re
import pandas as pd
import pandas_gbq
import requests
from bs4 import BeautifulSoup
import tweepy as tw
from airflow.models import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.bash import BashOperator
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.providers.google.cloud.operators.bigquery import (BigQueryCreateEmptyTableOperator, 
BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator)
from airflow.operators.python import PythonOperator
from helper.sqls import *
from helper.keys import *

GIT_URL = 'https://github.com/iuriqc/gb_challenges/tree/main/second-case/files'
REGEX = r"Base_[0-9]{4}\.xlsx$"

PROJECT_ID = 'gb-challenge'
DATASET_ID = 'TABLES'
TABLE_RAW = 'TABLES.RAW'
TABLE_FINAL = 'TABLES.FINAL'
TABLE_RAW_SCHEMA = [
                {"name": "ID_MARCA", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "MARCA", "type": "STRING", "mode": "REQUIRED"},
                {"name": "ID_LINHA", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "LINHA", "type": "STRING", "mode": "REQUIRED"},
                {"name": "DATA_VENDA", "type": "DATE", "mode": "REQUIRED"},
                {"name": "QTD_VENDA", "type": "INTEGER", "mode": "REQUIRED"},
            ]
TABLE_FINAL_SCHEMA = [
                {"name": "NOME", "type": "STRING", "mode": "REQUIRED"},
                {"name": "TEXTO", "type": "STRING", "mode": "REQUIRED"},
            ]
CONN_ID = "airflow-to-bq"

TASKS = []
TASKS_AUX = []

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

    def fill_table_bq(df, project_id:str, table_id:str, if_exists:str, table_schema:list):
        """
        Create raw table in BigQuery
        """
        credentials = get_credentials(CONN_ID)

        logging.info(f"Saving table {table_id} in BQ")
        pandas_gbq.to_gbq(df=df, 
                          table_id=table_id, 
                          project_id=project_id, 
                          if_exists=if_exists, 
                          credentials=credentials, 
                          table_schema=table_schema,
                          )
        logging.info("Table created")
    
    def get_best_selling_line(query:str, project_id:str, max_results:int):
        """
        Get Best Selling Line in 12/2019
        """
        credentials = get_credentials(CONN_ID)

        logging.info("Reading data from BQ")
        df = pandas_gbq.read_gbq(query=query, 
                          project_id=project_id, 
                          credentials=credentials, 
                          max_results=max_results,
                          )
        logging.info("Data fetched")

        return df['LINHA'][0]
    
    def get_data_from_twitter(bearer_token:str,
                              consumer_key:str,
                              consumer_secret:str,
                              access_token:str,
                              access_token_secret:str,
                              ):
        """Get data from Twitter"""
        client = tw.Client(bearer_token=bearer_token,
                   consumer_key=consumer_key, 
                   consumer_secret=consumer_secret, 
                   access_token=access_token, 
                   access_token_secret=access_token_secret,
                   return_type=dict,
                  )
        
        best_selling_line = get_best_selling_line(sql_best_selling_line,PROJECT_ID,1)
        
        tweets = client.search_recent_tweets(query=f'boticario {best_selling_line} lang:pt',
                                       max_results=50,
                                       tweet_fields = 'author_id',
                                       expansions='author_id',
                                      )
        
        users = pd.DataFrame.from_dict(tweets['includes']['users'])
        data = pd.DataFrame.from_dict(tweets['data']).drop(['id','edit_history_tweet_ids'], axis=1)
        data.rename(columns={'author_id': 'id'}, inplace=True)

        result = users.merge(data,on='id',how='inner').drop(['id','name'], axis=1)
        result.rename(columns={'username': 'NOME','text': 'TEXTO'}, inplace=True)

        return result

    start = DummyOperator(task_id="start", dag=dag)

    """check_pip = BashOperator(
      task_id="pip_task",
      #bash_command='pip freeze',
      bash_command='pip install openpyxl tweepy',
      dag=dag,
    )"""

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
        python_callable=fill_table_bq,
        op_kwargs={
            "df":get_data_from_git(GIT_URL, REGEX),
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
            gcp_conn_id=CONN_ID,
            configuration={
                "query": {
                    "query": locals()[f'sql_{table}'],
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

        TASKS_AUX.append(create_view_table)

    TASKS.append(TASKS_AUX)

    create_final_table = BigQueryCreateEmptyTableOperator(
        task_id="create_final_table",
        gcp_conn_id=CONN_ID,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_FINAL.split('.')[1],
        schema_fields=TABLE_FINAL_SCHEMA
    )

    best_selling_line = PythonOperator(
        task_id="best_selling_line",
        python_callable=fill_table_bq,
        op_kwargs={
            "df":get_data_from_twitter(bearer_token, 
                                       api_key, 
                                       api_secret_key, 
                                       access_token, 
                                       access_token_secret),
            "project_id":PROJECT_ID,
            "table_id":TABLE_FINAL,
            "if_exists":"replace",
            "table_schema":TABLE_FINAL_SCHEMA,
        },
        dag=dag,
    )

    end = DummyOperator(task_id="end", dag=dag)
    
    chain(
        start,
        # check_pip,
        create_dataset,
        create_raw_table,
        fill_raw_table,
        *TASKS,
        create_final_table,
        best_selling_line,
        end
    )
