import os
import random

import sqlalchemy


def init_db():
    connection_details = {
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'db_name': os.environ.get('DB_NAME')
    }
    engine = sqlalchemy.engine.create_engine(
        'mysql+mysqlconnector://{user}:{password}@{host}/{db_name}'.format(**connection_details),
        echo=True)
    print('Initializing db')
    conn = engine.connect()
    print('Initialized db')
    return conn


def get_customers():
    result = run_query('select * from customer')
    data = get_rows_dict(result)
    return data


def get_rows_dict(result: sqlalchemy.engine.result.ResultProxy):
    columns = result.keys()
    return [dict(zip(columns, values)) for values in result.fetchall()]


def run_query(query: str):
    result = db_engine.execute(query)
    return result


def get_products():
    return get_rows_dict(run_query('select * from product'))


def close_connection():
    db_engine.close()


def get_product_id():
    # load sample product
    product = get_random_product()
    return product['product_id']


COMPANIES = []


def get_random_company():
    global COMPANIES
    if len(COMPANIES) == 0:
        COMPANIES = get_companies()

    return random.choice(COMPANIES)


def get_companies():
    return get_rows_dict(run_query('select * from company'))


def get_company_products(company_id):
    query = 'select * from product where company_id=%s'
    result = db_engine.execute(query, (company_id,))
    return get_rows_dict(result)


db_engine: sqlalchemy.engine.Connection = init_db()


def get_random_company_product(company_id):
    products = get_company_products(company_id)
    if len(products) > 0:
        return random.choice(products)
    return None


PRODUCTS = []


def get_random_product():
    global PRODUCTS
    if len(PRODUCTS) == 0:
        PRODUCTS = get_products()

    return random.choice(PRODUCTS)


CUSTOMERS = []


def get_random_customer():
    global CUSTOMERS
    if len(CUSTOMERS) == 0:
        CUSTOMERS = get_customers()

    customer = random.choice(CUSTOMERS)
    return customer
