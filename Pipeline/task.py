import datetime
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models import XCom
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from Pipeline import data as p

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    # 'end_date': datetime(2018, 12, 30),
    'depends_on_past': False,
    'email': ['ruchi.bhardwaj@nineleaps.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}
dag = DAG(dag_id='covid',
          default_args=default_args,
          description="Collecting covid data",
          schedule_interval=timedelta(days=1),
          )


t1 = PythonOperator(task_id="fetch_data", python_callable=p.fetch_covid_state_data, dag=dag)

t2 = PythonOperator(task_id="load_data", python_callable=p.load_data, dag=dag)

t3 = PythonOperator(task_id="percentage", python_callable=p.read_the_data, provide_context=True, dag=dag)

t1 >> t2 >> t3