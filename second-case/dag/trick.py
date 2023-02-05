from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="PIP",
    schedule_interval=None,
    start_date=datetime(2023, 2, 4),
    catchup=False,
    tags=['pip'],
    description='PIP',
) as dag:

    check_pip = BashOperator(
      task_id="pip_task",
      bash_command='pip install openpyxl tweepy',
      dag=dag,
  )

    check_pip
