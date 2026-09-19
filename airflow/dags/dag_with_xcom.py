from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag

def push_xcom(**context):
   context['task_instance'].xcom_push(key='my_key', value='Hello XCom!')

def pull_xcom(**context):
   message = context['task_instance'].xcom_pull(task_ids='push_task', key='my_key')
   print(f"Message from XCom: {message}")

@dag(
    start_date=datetime(2024, 8, 1),
    schedule_interval='@daily',
    catchup=False
)
def dag_with_xcom():
   push_task = PythonOperator(
       task_id='push_task',
       python_callable=push_xcom,
       provide_context=True
   )

   pull_task = PythonOperator(
       task_id='pull_task',
       python_callable=pull_xcom,
       provide_context=True
   )
   push_task >> pull_task

dag_with_xcom()
