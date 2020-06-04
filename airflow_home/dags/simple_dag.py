import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def greet(r_date):
    print('Writing in file')
    with open('./greet.txt', 'a+', encoding='utf8') as f:
        now = dt.datetime.now()
        f.write(str(r_date) + '\n')
    return 'Greeted'
def respond():
    return 'Greet Responded Again'

default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2020, 6, 2, 12, 30, 00),
    'concurrency': 1,
    'depends_on_past': True,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),

}

with DAG('simple_dag', default_args=default_args, schedule_interval='*/1 * * * *')as dag:
    opr_hello = BashOperator(task_id='say_hi',
                             bash_command='echo Hi !!')
    opr_greet = PythonOperator(task_id='greet',
                               python_callable=greet,
                               op_kwargs={'r_date':'{{ (execution_date - macros.timedelta(days=5)).strftime("%Y-%m-%d") }} '})
    opr_sleep = BashOperator(task_id='sleep_me',
                             bash_command='sleep 2')
    opr_respond = PythonOperator(task_id='respond',
                                 python_callable=respond)

    opr_hello >> opr_greet >> opr_sleep >> opr_respond
