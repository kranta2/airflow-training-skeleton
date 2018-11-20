import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

args = {
    "owner":"karoliina",
    "start_date": airflow.utils.dates.days_ago(3),

}

dag = DAG(
    dag_id="exercise4",
    default_args=args,
    schedule_interval="0 0 * * *",
)

pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="...",
    sql="SELECT * FROM"
        "land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket = "airflow_training_kranta",
    filename = "result_{{ ds }}.json",
    postgress_conn_id = "postgres_id",
    dag=dag,
)