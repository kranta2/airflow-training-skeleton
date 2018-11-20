import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {"owner":"karoliina", "start_date":airflow.utils.dates.days_ago(1)}

dag = DAG(
    dag_id="exercise2",
    default_args=args,
)

BashOperator(
    task_id="print_execution_date", bash_command="echo {{ execution_date }}", dag=dag
)

BashOperator(
    task_id="wait_1", bash_command="sleep 1", dag=dag
)

BashOperator(
    task_id="wait_5", bash_command="sleep 5", dag=dag
)

BashOperator(
    task_id="wait_10", bash_command="sleep 10", dag=dag
)

DummyOperator(
    task_id="the_end", dag=dag
)
#BashOperator(
#    task_id="the_end", bash_command="echo {{ execution_date }}", dag=dag
#)

print_execution_date >> [wait_1, wait_5, wait_10] >> the_end
