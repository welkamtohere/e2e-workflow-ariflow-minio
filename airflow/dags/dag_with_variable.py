from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

dag = DAG(
  dag_id="dag_with_variable",
  start_date=datetime(2026, 1, 1)
)

task_1 = EmptyOperator(
  task_id = "first_task",
  dag     = dag
),
task_2 = EmptyOperator(
  task_id = "second_task",
  dag     = dag
)
task_3 = EmptyOperator(
  task_id = "third_task",
  dag     = dag
)

task_1 >> task_2 >> task_3
