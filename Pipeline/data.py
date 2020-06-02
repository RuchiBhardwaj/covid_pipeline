import csv
import datetime
import json
import os

import requests
import datetime
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.models import XCom
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from google.cloud import bigquery

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"] = "/home/nineleaps/PycharmProjects/COVID19_Airflow/Pipeline/covid19data-279110-8e7c1d3b9597.json"
client = bigquery.Client()
filename = '/home/nineleaps/PycharmProjects/COVID19_Airflow/Pipeline/covid_data/covid_data_{}.csv'.format(
    datetime.datetime.today().strftime('%Y-%m-%d'))
dataset_id = 'covid19datatable'
table_id = 'covid19'

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


def fetch_covid_state_data():
    req = requests.get('https://api.covidindiatracker.com/state_data.json')
    url_data = req.text
    data = json.loads(url_data)
    count = 0
    covid_data = [['date', 'state', 'number_of_cases']]
    date = datetime.datetime.today().strftime('%Y-%m-%d')
    for state in data:
        covid_data.append([date, state.get('state'), state.get('aChanges')])
        count += 1
    with open(filename, "w") as f:
        # with open("covid_data/covid_data_{}.csv".format(date) ,"w") as f:
        writer = csv.writer(f)
        writer.writerows(covid_data)
    return count


def load_data(**kwargs):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
    return job.output_rows
    # kwargs['ti'].xcom_push(key='value from pusher 1', value=job.output_rows)


def read_the_data(**kwargs):

    ti = kwargs['ti']
    v1 = ti.xcom_pull(task_ids='load_data')
    count = ti.xcom_pull(task_ids='fetch_data')
    print("percentage = {}".format((v1/count)*100))


t1 = PythonOperator(task_id="fetch_data", python_callable=fetch_covid_state_data, dag=dag)

t2 = PythonOperator(task_id="load_data", python_callable=load_data, dag=dag)

t3 = PythonOperator(task_id="percentage", python_callable=read_the_data, provide_context=True, dag=dag)

t1 >> t2 >> t3


