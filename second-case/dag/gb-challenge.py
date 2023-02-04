from datetime import datetime
import logging
import os
from os import path
import pandas as pd
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

default_args={
    "start_date": datetime(2023, 2, 4),
    "depends_on_past": False
    }

with DAG(
    "teste",
    default_args = default_args,
    schedule_interval = None,
    tags = ['teste'],
    description = 'Teste',
    catchup = False 
) as dag:
    
    start = DummyOperator(task_id="start",dag=dag)