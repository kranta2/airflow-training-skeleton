import airflow
from airflow.models import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)

from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataProcPySparkOperator,
    DataprocClusterDeleteOperator,
)

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
    task_id="postgres_to_gcs",
    sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    bucket = "airflow_training_kranta",
    filename = "land_registry_price_pair_uk/{{ ds }}/properties_{}.json",
    postgres_conn_id = "postgres_id",
    dag=dag,
)

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="dataproc_create_cluster",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-32fd1524d41dfd35",
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

compute_aggregates = DataProcPySparkOperator(
    task_id="compute_aggregates",
    main="../other/build_statistics.py",
    cluster_name="analyse-pricing-{{ ds }}",
    arguments=["{{ ds }}"],
    dag=dag,
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="dataproc_delete_cluster",
    cluster_name="analyse-pricing-{{ ds }}",
    dag=dag,
    project_id="airflowbolcom-32fd1524d41dfd35",
)

[pgsl_to_gcs, dataproc_create_cluster] >> compute_aggregates >> dataproc_delete_cluster

