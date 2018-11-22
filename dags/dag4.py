from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from tempfile import NamedTemporaryFile
from airflow.utils.decorators import apply_defaults

class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action
    :param http_conn_id: The connection to run the operator against
    :type http_conn_id: string
    :param endpoint: The relative part of the full url. (templated)
    :type endpoint: string
    :param gcs_path: The path of the GCS to store the result
    :type gcs_path: string
    """

    template_fields = ("endpoint", "gcs_path")
    template_ext = ()
    ui_color = "#f4a460"
    @apply_defaults
    def __init__(
            self,
            endpoint,
            gcs_bucket,
            gcs_path,
            method="GET",
            http_conn_id="http_default",
            gcs_conn_id="google_cloud_default",
            *args,
            **kwargs
    ):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.method = method
        self.endpoint = endpoint
        self.gcs_bucket = gcs_bucket
        self.gcs_path = gcs_path
        self.gcs_conn_id = gcs_conn_id

    def execute(self, context):
        http = HttpHook(self.method, http_conn_id=self.http_conn_id)
        self.log.info("Calling HTTP method")
        response = http.run(self.endpoint)
        with NamedTemporaryFile() as tmp_file_handle:
            tmp_file_handle.write(response.content)
            tmp_file_handle.flush()
            hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.gcs_conn_id)
            hook.upload(
                bucket=self.gcs_bucket,
                object=self.gcs_path,
                filename=tmp_file_handle.name,
            )


##################

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

http_to_gcs = HttpToGcsOperator(
    task_id="http_to_gcs",
    dag=dag,
    http_conn_id="https://europe-west1-gdd-airflow-training.cloudfunctions.net/airflow-training-transform-valutas",
    endpoint="?date={{ ds }}&from=GBP&to=EUR",
    gcs_bucket="airflow_training_kranta",
    gcs_path="currency/{{ ds ]}/exchange_{}.json"
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

pgsl_to_gcs >> dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster
