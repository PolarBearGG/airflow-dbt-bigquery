from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta


with DAG(
    'bash_DAG',
    start_date=datetime(2021, 12, 23),
    description='An Airflow DAG to invoke simple bash commands',
    schedule_interval=timedelta(days=1),
) as dag:
    test = BashOperator(
        task_id='test',
        bash_command='pip3 freeze .'
    )

    test
