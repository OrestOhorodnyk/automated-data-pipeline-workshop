import os
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import (
	BigQueryCreateEmptyDatasetOperator,
)
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration


PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
SERVICE_ACCOUNT = os.environ.get("SERVICE_ACCOUNT")
SOURCE_BUCKET = os.environ.get("SOURCE_BUCKET")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET")
DATAFLOW_TEMP_FILES = os.environ.get("DATAFLOW_TEMP_FILES")
LOCATION = os.environ.get("LOCATION")
SUBNETWORK = os.environ.get("SUBNETWORK")
DATASET_ID = {
	"dev": "sales_dev",
	"prod": "sales"
}
RUNNER = {
	"dev": "DirectRunner",
	"prod": "DataflowRunner"
}
ENV = os.environ.get('ENVIRONMENT')
DESTINATION_DATASET = DATASET_ID[ENV]

CUR_DIR = os.path.abspath(os.path.dirname(__file__))
BEAM_PIPELINE_LOCATION = f"{CUR_DIR}/beam_jobs/sales_ingest_df_job.py"
SETUP_FILE_LOCATION = f"{CUR_DIR}/beam_jobs/setup.py"
default_args = {
	"retries": 0,
}
PIPELINE_ARGS = {
	"project-id": PROJECT_ID,
	"destination-dataset-id": DESTINATION_DATASET,
	"gcs-source-bucket": SOURCE_BUCKET,
	"gcs-temp-location": f"{DATAFLOW_TEMP_FILES}/bq-temp",
	"temp_location": f"{DATAFLOW_TEMP_FILES}/tmp",
	"staging_location": f"{DATAFLOW_TEMP_FILES}/stg",
	"service_account_email": SERVICE_ACCOUNT,
	"save_main_session": True,
	"setup_file": SETUP_FILE_LOCATION,
	"subnetwork": SUBNETWORK,
}
REQUIRED_PACKAGES = ['apache-beam[gcp]', 'google-cloud-storage']

@dag(
	start_date=datetime(year=2022, month=10, day=14),
	default_args=default_args,
	schedule_interval="00 01,13 * * *",
	catchup=False,
	tags=["sales-data"],
)
def sales_dag():
	"""
	DAG responsible for scheduling sales data ingestion.
	"""
	start = EmptyOperator(task_id="start")
	end = EmptyOperator(task_id="end")
	create_dataset = BigQueryCreateEmptyDatasetOperator(
		task_id="create_sales_dataset",
		project_id=PROJECT_ID,
		dataset_id=DESTINATION_DATASET,
		location="EU",
	)

	ingest_data = BeamRunPythonPipelineOperator(
		task_id="dataflow_job_for_sales_ingestion",
		py_file=BEAM_PIPELINE_LOCATION,
		runner=RUNNER.get(ENV, "dev"),
		pipeline_options=PIPELINE_ARGS,
		py_interpreter="python3",
		py_requirements=REQUIRED_PACKAGES,
		py_system_site_packages=True,
		execution_timeout=timedelta(hours=1),
		dataflow_config=DataflowConfiguration(
			job_name="sales_ingets",
			append_job_name=False,
			location=LOCATION,
		),
	)
	start >> create_dataset >> ingest_data >> end


sales_dag = sales_dag()
