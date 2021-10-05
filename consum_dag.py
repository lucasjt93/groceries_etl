from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from consum_project import scrapper, ticket_parser
from datetime import timedelta, datetime

""" Need to set: export AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=300.0 """

default_args = {
    'owner': 'lucasjt',
    'depends_on_past': False,
    'email': ['lucasjuliantoledo@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30)
}

with DAG(
    'consum_dag',
    default_args=default_args,
    description='Consum pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 10, 4)
) as dag:

    t1 = PythonOperator(
        task_id='scrap_tickets',
        python_callable=scrapper
    )

    t2 = BashOperator(
        task_id='pdftotext',
        bash_command='{{ "~/consum/consum_project/parsetickets.sh" }}'
    )

    t3 = PythonOperator(
        task_id='parse_tickets',
        python_callable=ticket_parser
    )

    t1 >> t2 >> t3
