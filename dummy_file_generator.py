"""
This module will generate csv file with dummy data with the following structure:
transaction_id,store_id,country,transaction_dt,currency,price_per_unit,quantity,product_id

The granularity is one row for one line item.
"""
from dataclasses import dataclass, field, asdict
from datetime import timedelta, datetime
from random import (
	choice,
	randrange,
	uniform,
)
from typing import List
from uuid import uuid4

from dataclass_dict_convert import dataclass_dict_convert

COUNTRY_TO_ABRIVIATION = {
	'GERMANY': 'DE',
	'INDIA': 'IN',
	'United Kingdom': 'GB',
	'United States': 'US'

}

country_to_currency = {
	'DE': 'Euro',
	'IN': 'Indian Rupees',
	'GB': 'Pound sterling',
	'US': "US Dollar"
}

COUNTRIES = list(COUNTRY_TO_ABRIVIATION.values())
GERMANY_SOP_IDS = [f'DE_{i}' for i in range(1, 4)]


def random_date(start: datetime, end: datetime):
	delta = end - start
	int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
	random_second = randrange(int_delta)
	return start + timedelta(seconds=random_second)


@dataclass_dict_convert
@dataclass
class Product:
	product_id: int = field(init=False)
	price_per_unit: float = field(init=False)
	quantity: int = field(init=False)

	def __post_init__(self):
		self.product_id = randrange(10000)
		self.price_per_unit = uniform(1.1, 2999.99)
		self.quantity = randrange(1, 10)

	def dict(self):
		return {k: str(v) for k, v in asdict(self).items()}


@dataclass_dict_convert
@dataclass
class Row:
	store_id: str
	country: str
	transaction_dt: str
	currency: str
	transaction_id: str = field(init=False)
	products: List[Product] = field(init=False)

	def __post_init__(self):
		self.transaction_id = str(uuid4())
		self.products = self.get_products()

	@staticmethod
	def get_products() -> List[Product]:
		number_of_products = randrange(1, 10)
		products: List[Product] = []
		for i in range(number_of_products):
			products.append(Product())
		return products

	def dict(self):
		return {k: str(v) for k, v in asdict(self).items()}


def generate_row(country: str, number_of_shops: int, number_of_transactions, date: str):
	star_date = datetime.strptime(f'{date} 9:00 AM', '%m-%d-%Y %I:%M %p')
	end_date = datetime.strptime(f'{date} 6:00 PM', '%m-%d-%Y %I:%M %p')
	shop_ids = [f'{country}_{i}' for i in range(1, number_of_shops + 1)]
	for i in range(1, number_of_transactions):
		yield Row(
			store_id=choice(shop_ids),
			country=country,
			currency=country_to_currency[country],
			transaction_dt=str(random_date(star_date, end_date))
		)


def create_file(country: str, number_of_shops: int, number_of_transactions: int, date: str):
	with open(f'data/{country}_{date}.csv', 'w') as the_file:
		# the_file.write('transaction_id,store_id,country,transaction_dt,currency,price_per_unit,quantity,product_id\n')
		for i in generate_row(
				country=country,
				number_of_shops=number_of_shops,
				number_of_transactions=number_of_transactions,
				date=date
		):
			d: dict = i.to_dict()
			prods = d.pop('products')
			for j in prods:
				merged = {**d, **j}
				csv_row = f"{merged['transaction_id']}," \
				          f"{merged['store_id']}," \
				          f"{merged['country']}," \
				          f"{merged['transaction_dt']}," \
				          f"{merged['currency']}," \
				          f"{merged['price_per_unit']}," \
				          f"{merged['quantity']}," \
				          f"{merged['product_id']}"
				print(csv_row)
				the_file.write(f'{csv_row}\n')


if __name__ == '__main__':
	for country in country_to_currency.keys():
		create_file(
			country=country,
			number_of_shops=3,
			number_of_transactions=500,
			date='10-13-2022'
		)
