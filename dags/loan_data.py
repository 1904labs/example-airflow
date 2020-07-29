#Demo
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

DESTINATION_BUCKET = 'sc-flow-dev-data'
DESTINATION_DIRECTORY = "transferred"

dag_params = {
    'dag_id': 'PostgresToCloudStorageExample',
    'start_date': datetime(2020, 7, 7),
    'schedule_interval': timedelta(days=1),
}

with DAG(**dag_params) as dag:
    move_results = PostgresToGoogleCloudStorageOperator(
        task_id="move_results",
        bucket=DESTINATION_BUCKET,
        filename=DESTINATION_DIRECTORY + "/{{ execution_date }}" + "/{}.json",
        sql='''SELECT *, due_date::date - effective_date::date as calc FROM loan_data;''',
        retries=3,
        postgres_conn_id="postgres_poc"
    )