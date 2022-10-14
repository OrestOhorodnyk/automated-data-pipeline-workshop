# Prerequisites 
1. Google storage buckets created 
   * The `source_bucket` - to store source files 
   * The `dataflow_temp_files` - technical bucket to store tmp files
2. Service account with the following roles: 
    * BigQuery Admin
    * BigQuery Job User
    * Composer Administrator
    * Composer Worker
    * Compute Admin
    * Dataflow Developer
    * Dataflow Worker
    * Service Account User
    * Storage Admin
3. Creat a folder `secret` and save a service key.json to that folder
4. Generate dummy data with `dummy_file_generator.py` and copy files to the `source_bucket`

## How to run the beam job locally
1. Open terminal
2. Create a virtual environment with python 3.7 and install required packages from `df_requirements.txt`
   * `virtualenv -p python3.7 venv`
   * activate venv `source venv/bin/activate`
3. Set GOOGLE_APPLICATION_CREDENTIALS env variable
   * `export GOOGLE_APPLICATION_CREDENTIALS=<path to the service account key .json>`
4. Create dataset `saves_dev`
5. To start execution run the following command:
```bash
python dags/beam_jobs/sales_ingest_df_job.py \
--runner=DirectRunner \
--project-id=<project id> \
--destination-dataset-id=sales_dev \
--gcs-source-bucket=<source_bucket> \
--temp_location=<dataflow_temp_files>/df 
```
The result of the execution should be two tables in tables created `raw` and `transaction_summary` 
with data from thr files

## How to run deploy the beam job to the GCP Dataflow
1) The same steps as run locally but the last one, the command should be as following:
```bash
python dags/beam_jobs/sales_ingest_df_job.py \
    --runner=DataflowRunner \
    --job_name=sales-data \
    --region=<region e.g. europe-west1> \
    --project-id=<project id> \
    --destination-dataset-id=sales_dev \
    --gcs-source-bucket=<source bucket nam e. g. sales-data-16355>  \
    --gcs-temp-location=<dataflow_temp_files>/bq \
    --temp_location=<dataflow_temp_files>/df \
    --staging_location=<dataflow_temp_files>/stg \
    --setup_file=dags/beam_jobs/setup.py \
    --save_main_session \
    --subnetwork <link to subnetwork> \ 
    --service_account_email=<service account email>
```

2) Navigate to `Google cloud console -> Dataflow -> Jobs` to see running job with name specified in the command.
The execution time should be about 7 min.

## Airflow setup with docker compose
1) Crete required folders: 
```
mkdir -p ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
2) Edd the wfilliwing env variables:
```bash
GOOGLE_CLOUD_PROJECT: <project id>
SERVICE_ACCOUNT: <service account email>
SOURCE_BUCKET: <source_bucket>
DATAFLOW_TEMP_FILES: <dataflow_temp_files>
LOCATION: 'europe-west1'
```
3) Build docker image with airflow: `docker build --progress=plain -t airflow-2.3.3-custom:latest --no-cache .`
4) Init airflow `docker-compose up airflow-init`
5) Run docker compose `docker-compose up -d`
6) Open in browser `http://localhost:8080/` username and password `airflow`
7) On Airflow Ui navigate to Admin -> UI > Connections Set Connection Id `google_cloud_default` and Connection Type Google Cloud
8) Navigate to DAG tab, search dag by tag `sales-data`
9) Run the sales_dag

## Cloud Composer
1) Run the composer instance with the following attributes:
   * Use the service account with required permissions 
   * Specify all env variables as in docker compose
2) Copy the `load_sales_data.py` and `beam_jobs` to the composer DAG folder
3) Navigate to Airflow UI and run the dag

## Useful links
1) [Service account](https://cloud.google.com/iam/docs/service-accounts)
2) [Subnetwork](https://cloud.google.com/dataflow/docs/guides/specifying-networks)
3) [IAM](https://cloud.google.com/iam/docs/understanding-roles)
4) [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
