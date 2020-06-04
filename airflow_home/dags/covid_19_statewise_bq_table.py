from scripts.etl_task import *
from datetime import timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


dag_config = Variable.get("bigquery_variables", deserialize_json=True)
BQ_CONN_ID = dag_config["bq_conn_id"]
BQ_PROJECT = dag_config["bq_project"]
BQ_TABLE = dag_config["bq_table"]
BQ_DATASET = dag_config["bq_dataset"]

"""covid dag configuration arguments."""
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 5, 31, 12, 30, 00),
    'concurrency': 1,
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),

}

with DAG('bq_covid_stats', default_args=default_args, schedule_interval='00 10 * * *')as dag:

    task1 = PythonOperator(task_id='create_csv',
                           python_callable=fetch_daily_data,
                           op_kwargs={'pipeline_date':'{{ (execution_date - '
                                                      'macros.timedelta(days=1)).strftime("%Y-%m-%d")}}'},
                           provide_context=True)


    task2 = PythonOperator(task_id='upload_data',
                           python_callable=upload_csv_to_big_table,
                           op_kwargs={'table_id': BQ_TABLE,
                                      'dataset_id': BQ_DATASET,
                                      'file_date': '{{ (execution_date - macros.timedelta(days=1)).'
                                                   'strftime("%Y-%m-%d")}}',
                                      'partition_date': '{{yesterday_ds_nodash}}'
                                      },
                           provide_context=True)

    task3 = PythonOperator(task_id='upload_percentage',
                           python_callable=find_percentage,
                           provide_context=True
                           )


    task1 >> task2 >> task3
