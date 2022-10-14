RAW_TABLE_SCHEMA = {
	"fields": [
		{
			"mode": "NULLABLE",
			"name": "product_id",
			"type": "INTEGER"
		},
		{
			"mode": "NULLABLE",
			"name": "quantity",
			"type": "INTEGER"
		},
		{
			"mode": "NULLABLE",
			"name": "price_per_unit",
			"type": "FLOAT"
		},
		{
			"mode": "NULLABLE",
			"name": "transaction_id",
			"type": "STRING"
		},
		{
			"mode": "NULLABLE",
			"name": "currency",
			"type": "STRING"
		},
		{
			"description": "bq-datetime",
			"mode": "NULLABLE",
			"name": "transaction_dt",
			"type": "TIMESTAMP"
		},
		{
			"mode": "NULLABLE",
			"name": "store_id",
			"type": "STRING"
		},
		{
			"mode": "NULLABLE",
			"name": "country",
			"type": "STRING"
		}
	]
}

TRANSACTION_SUMMARY_TABLE_SCHEMA = {
	"fields": [
		{
			"mode": "NULLABLE",
			"name": "total_amount",
			"type": "FLOAT"
		},
		{
			"mode": "NULLABLE",
			"name": "transaction_id",
			"type": "STRING"
		},
		{
			"mode": "NULLABLE",
			"name": "currency",
			"type": "STRING"
		},
		{
			"description": "bq-datetime",
			"mode": "NULLABLE",
			"name": "transaction_dt",
			"type": "TIMESTAMP"
		},
		{
			"mode": "NULLABLE",
			"name": "store_id",
			"type": "STRING"
		},
		{
			"mode": "NULLABLE",
			"name": "country",
			"type": "STRING"
		}
	]
}
