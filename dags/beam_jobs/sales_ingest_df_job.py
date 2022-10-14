import argparse
import logging
from itertools import chain
from typing import (
	Iterator,
	List,
)

import apache_beam as beam
from apache_beam.io.gcp.internal.clients.bigquery import TableReference
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import storage

from bq_schemas.sales import (
	RAW_TABLE_SCHEMA,
	TRANSACTION_SUMMARY_TABLE_SCHEMA,
)

RAW_TABLE_NAME = 'raw'
TRANSACTION_SUMMARY_TABLE_NAME = 'transaction_summary'


class BucketFileReader:
	def __init__(self, project: str, bucket: str, folders: List = None):
		self.project = project
		self.bucket = bucket
		self.folders = folders

	def get_file_names(self) -> List[str]:
		bucket = storage.Client(project=self.project).bucket(self.bucket)
		files = []
		if self.folders:
			for folder in self.folders:
				files.append(bucket.list_blobs(prefix=folder))
		else:
			files.append(bucket.list_blobs())
		f = [file.name for file in chain(*files)]
		return f


class ReadFileContent(beam.DoFn):
	def __init__(self, project: str, bucket_name: str):
		self.project = project
		self.bucket_name = bucket_name

	def setup(self):
		self.bucket = storage.Client(project=self.project).bucket(self.bucket_name)

	def process(self, file_name: str, *args, **kwargs) -> Iterator[str]:
		logging.info(f"Downloading blob {file_name}")
		file = self.bucket.blob(file_name)
		yield from file.download_as_text().splitlines()


class RowToDict(beam.DoFn):

	def process(self, row: str, *args, **kwargs):
		transaction_id, store_id, country, transaction_dt, currency, price_per_unit, quantity, product_id = row.split(
			',')
		yield {
			"transaction_id": transaction_id,
			"store_id": store_id,
			"country": country,
			"transaction_dt": transaction_dt,
			"currency": currency,
			"price_per_unit": price_per_unit,
			"quantity": quantity,
			"product_id": product_id
		}


class CalculateTransactionSummary(beam.DoFn):
	def process(self, element: tuple, *args, **kwargs):
		general_information, sold_products = element
		transaction_id, store_id, country, transaction_dt, currency = general_information
		total_amount = sum([float(product['price_per_unit']) * float(product['quantity']) for product in sold_products])
		yield {
			"transaction_id": transaction_id,
			"store_id": store_id,
			"country": country,
			"transaction_dt": transaction_dt,
			"currency": currency,
			"total_amount": total_amount
		}


def main(argv=None, save_main_session=True):
	parser = argparse.ArgumentParser()

	parser.add_argument(
		"--project-id", required=True, help="GCP Project ID"
	)

	parser.add_argument(
		"--destination-dataset-id",
		required=True,
		help="Dataset ID to store results",
	)

	parser.add_argument(
		"--gcs-temp-location",
		required=False,
		help="The GCS bucket to store temporary files "
		     "before data goes to the BigQuery, usually "
		     "the same place as for the Dataflow job",
	)
	parser.add_argument(
		"--gcs-source-bucket",
		required=False,
		help="The GCS bucket with input files",
	)
	parser.add_argument(
		"--folders",
		required=False,
		help="Optional parameter to read from specific List of folders, "
		     "if not passed all files from all folders will be processed",
	)

	known_args, pipeline_args = parser.parse_known_args(argv)
	pipeline_args.extend([
		'--project=' + known_args.project_id
	])
	pipeline_options = PipelineOptions(pipeline_args)
	pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

	reader = BucketFileReader(
		project=known_args.project_id,
		bucket=known_args.gcs_source_bucket,
		folders=known_args.folders,
	)
	raw_table = TableReference(
		projectId=known_args.project_id,
		datasetId=known_args.destination_dataset_id,
		tableId=RAW_TABLE_NAME,
	)

	transaction_summary_table = TableReference(
		projectId=known_args.project_id,
		datasetId=known_args.destination_dataset_id,
		tableId=TRANSACTION_SUMMARY_TABLE_NAME,
	)

	with beam.Pipeline(options=pipeline_options) as pipeline:
		lines_to_dict = (
				pipeline
				| "Files to read"
				>> beam.Create(reader.get_file_names())
				| "Reshuffle file names" >> beam.Reshuffle()
				| "Read each file content"
				>> beam.ParDo(
			ReadFileContent(
				project=known_args.project_id,
				bucket_name=known_args.gcs_source_bucket
			)
		)
				| beam.ParDo(RowToDict())
		)

		transaction_summary = (
				lines_to_dict
				| beam.GroupBy(lambda s: (
			s.pop("transaction_id"), s.pop("store_id"), s.pop("country"), s.pop("transaction_dt"), s.pop("currency")))
				| beam.ParDo(CalculateTransactionSummary())
		)

		lines_to_dict | "Write raw data" >> beam.io.WriteToBigQuery(
			table=raw_table,
			schema=RAW_TABLE_SCHEMA,
			# schema='SCHEMA_AUTODETECT',
			write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
			create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
			custom_gcs_temp_location=known_args.gcs_temp_location,
			additional_bq_parameters={
				"timePartitioning": {"type": "DAY", "field": "transaction_dt"},
			})

		transaction_summary | "Write transaction summary" >> beam.io.WriteToBigQuery(
			table=transaction_summary_table,
			schema=TRANSACTION_SUMMARY_TABLE_SCHEMA,
			write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
			create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
			custom_gcs_temp_location=known_args.gcs_temp_location,
			additional_bq_parameters={
				"timePartitioning": {"type": "MONTH", "field": "transaction_dt"},
			})


if __name__ == '__main__':
	logging.getLogger().setLevel(logging.INFO)
	main()
