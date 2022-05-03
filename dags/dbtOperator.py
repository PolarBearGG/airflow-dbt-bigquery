from airflow import DAG
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator
)
from airflow.utils.dates import days_ago

default_args = {
    'dir': '/opt/airflow/dbt/project',
    'start_date': days_ago(0)}

with DAG(dag_id='dbt_DAG', description='An Airflow DAG to invoke simple dbt commands', default_args=default_args, schedule_interval='@daily') as dag:

    dbt_seed = DbtSeedOperator(
        task_id='dbt_seed',
        profiles_dir='/opt/airflow/dbt/project'
    )

    dbt_snapshot = DbtSnapshotOperator(
        task_id='dbt_snapshot',
        profiles_dir='/opt/airflow/dbt/project'
    )

    dbt_run = DbtRunOperator(
        task_id='dbt_run',
        profiles_dir='/opt/airflow/dbt/project'
    )

    dbt_test = DbtTestOperator(
        task_id='dbt_test',
        retries=0,
        # Failing tests would fail the task, and we don't want Airflow to try again
        profiles_dir='/opt/airflow/dbt/project'
    )

    dbt_run >> dbt_test
