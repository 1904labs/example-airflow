from datetime import timedelta
from datetime import datetime
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

import uuid
import csv


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 7, 7),
    'email': ['nbruce@1904labs.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

pg_movies_dirs = """\
select 
	m."name", 
	m."year", 
	d."first_name" as director_first_name,
	d."last_name" as director_last_name,
	STRING_AGG(dg."genre", ',') as directors_genres
from movies as m

join movies_directors md 
on m."id" = md."movie_id" 
and m."year" > 2005
join directors d
on md."director_id" = d."id"
join directors_genres dg 
on d.id = dg."director_id" 
	
group by "name", "year", director_first_name, director_last_name"""

mysql_movies_actors = """\
select
    m.name as movie,
    m.year year,
    a.first_name as actor_first_name,
    a.last_name as actor_last_name,
    a.gender as actor_gender
from movies m

join roles r
on m.id = r.movie_id

join actors a
on r.actor_id = a.id

where m.year > 2005
"""


def postgres_to_file(conn_id, sql, filename):
    pg_hook = PostgresHook(conn_name_attr='spacecadets_postgres')
    db_to_file(hook=pg_hook,
               sql=sql,
               filename=filename)


def mysql_to_file(conn_id, sql, filename):
    mysql_hook = MySqlHook(conn_name_attr=conn_id)
    db_to_file(hook=mysql_hook,
               sql=sql,
               filename=filename)


def db_to_file(hook, sql, filename):
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    col_names = [i[0] for i in cursor.description]
    data = cursor.fetchall()

    with open(filename, 'w+') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(col_names)
        writer.writerows(data)


def csv_to_json(filename):
    with open(filename, 'r') as infile:
        reader = csv.reader(infile)
        headers = next(reader)
        new_filename = filename.rsplit('.', 1)[0] + '.json'
        with open(new_filename, 'w+') as outfile:
            for row in reader:
                row_json = {headers[i]: value for i, value in enumerate(row)}
                outfile.write(str(row_json))
                outfile.write("\n")


mysql_base_filename = str(uuid.uuid4())
pg_base_filename = str(uuid.uuid4())


mysql_csv_filename = '/tmp/' +  mysql_base_filename + '.csv'
pg_csv_filename = '/tmp/' + pg_base_filename + '.csv'

mysql_json_filename = '/tmp/' +  mysql_base_filename + '.json'
pg_json_filename = '/tmp/' + pg_base_filename + '.json'

BUCKET = 'spacecadets-281921_data'
PROJECT_ID = "spacecadets-281921"

GCS_FILENAME = "airflow/imdb/{0}/{1}/.json"

with DAG(
    'imdb_data_pull',
    default_args=default_args,
    start_date=datetime(2020, 7, 7),
    description='A DAG that pulls individual tables from the IMDB database, writes them to files, joins them and denormalizes the data, writes the new data into Cloud Storage',
    schedule_interval=timedelta(days=1),
) as dag:
    mysql_poc_pull = PythonOperator(
        task_id='MySQL_TO_FILE',
        python_callable=mysql_to_file,
        op_kwargs={'conn_id': 'spacecadets_mysql',
                'sql': mysql_movies_actors,
                'filename': mysql_csv_filename}
    )

    pg_poc_pull = PythonOperator(
        task_id='PG_TO_FILE',
        python_callable=postgres_to_file,
        postgres_conn_id='spacecadets_postgres',
        op_kwargs={'conn_id': 'spacecadets_postgres',
                'sql': pg_movies_dirs,
                'filename': pg_csv_filename
        },
    )

    upload_pg_file = LocalFilesystemToGCSOperator(
        task_id="PG_UPLOAD_FILE",
        src=pg_csv_filename,
        dst=GCS_FILENAME.format('movies_directors', pg_base_filename),
        bucket=BUCKET,
    )

    upload_mysql_file = LocalFilesystemToGCSOperator(
        task_id="MYSQL_UPLOAD_FILE",
        src=mysql_csv_filename,
        dst=GCS_FILENAME.format('movies_directors', mysql_base_filename),
        bucket=BUCKET,
    )

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    print_date = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    mysql_poc_pull.set_upstream(print_date)
    pg_poc_pull.set_upstream(print_date)

    upload_mysql_file.set_upstream(mysql_poc_pull)
    upload_pg_file.set_upstream(pg_poc_pull)

