import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

args = {
    "owner":"karoliina",
    "start_date":airflow.utils.dates.days_ago(14),
}

weekday_person_to_email = {
    0: "Bob", # Monday
    1: "Joe", # Tuesday
    2: "Alice", # Wednesday
    3: "Joe", # Thursday
    4: "Alice", # Friday
    5: "Alice", # Saturday
    6: "Alice", # Sunday
}

dag = DAG(
    dag_id="excercise3",
    default_args=args,
)

def p_day(execution_date, **context):
    print(execution_date.weekday())

t1 = PythonOperator(
    task_id="print_weekday", python_callable=p_day, dag=dag, provide_context=True
)

def get_who(execution_date, **context):
    who = weekday_person_to_email[execution_date.weekday()]
    return "email_".format(str.lower(who))

t2 = BranchPythonOperator(
    task_id="branching", dag=dag, python_callable=get_who, provide_context=True
)

t3 = DummyOperator(
    task_id="final_task", dag=dag, trigger_rule=TriggerRule.ONE_SUCCESS
)

# email bob
t4 = DummyOperator(
    task_id="email_bob", dag=dag
)

# email alice
t5 = DummyOperator(
    task_id="email_alice", dag=dag
)

# email joe
t6 = DummyOperator(
    task_id="email_joe", dag=dag
)

# t1 >> t2 >> [t4, t5, t6] >> t3
t1 >> t3
