import os
import random
from datetime import timedelta, datetime

import pytz
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from faker import Faker

try:
    from dags import load_env, db, models
except:
    import load_env, db, models

faker = Faker()

USER_AGENTS = [faker.firefox, faker.chrome, faker.safari,
               faker.internet_explorer, faker.opera]

ALL_TIMEZONES = list(pytz.all_timezones)
HOST_NAME = 'https://example.com'
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'apache_logs')


def error_request(username, time_zone=None):
    log = get_request_log(username, path=faker.file_path(), time_zone=time_zone)
    log.response_status = random.choice(models.HTTPMethod.HTTP_STATUS_ERROR)
    return log


def get_request_log(username: str, path: str, status=None, method=None, remote_host=None, time_zone=None, referer=None):
    log = models.WebLog()
    log.user_agent = get_user_agent()
    log.request_path = path
    log.username = username
    log.request_method = models.HTTPMethod.GET if not method else method
    log.parsed_request_time = get_random_date(time_zone=time_zone)
    log.request_time_zone_offset = log.parsed_request_time.strftime('%z')
    log.referer = referer if referer else faker.uri()
    log.response_size = 1024 * faker.random_int(500, 10000)
    log.response_status = 200 if not status else status
    log.remote_host = remote_host if remote_host else faker.ipv4()
    return log


def customer_login_request(username: str, time_zone=None):
    login_path = '/login'
    log = get_request_log(
        username, path=login_path,
        method=models.HTTPMethod.POST,
        time_zone=time_zone
    )
    return log


def customer_order_request(username: str, product_id: int, time_zone=None):
    path = '/customer/order?product_id={0}'.format(str(product_id))
    log = get_request_log(username, path=path, method=models.HTTPMethod.POST, time_zone=time_zone)
    return log


def company_buy_from_supplier_request(company_name: str, supplier_id: int, product_id: int, time_zone=None):
    path = '/company/supplier?supplier_id={0}&product_id={0}'.format(str(supplier_id), str(product_id))
    log = get_request_log(company_name, path=path, method=models.HTTPMethod.POST, time_zone=time_zone)
    return log


def company_place_customer_order(company_name: str, customer_id: int, product_id: int, time_zone=None):
    path = '/company/place_order?customer_id={0}&produce_id={1}'.format(str(customer_id), str(product_id))
    log = get_request_log(company_name, path=path, method=models.HTTPMethod.POST, time_zone=time_zone)
    return log


def get_random_timezone():
    random_timezone = pytz.timezone(random.choice(ALL_TIMEZONES))
    return random_timezone


def get_random_date(time_zone=None):
    tz = time_zone if time_zone else get_random_timezone()
    random_date = datetime.now(tz)
    return random_date


def get_user_agent():
    agent = random.choice(USER_AGENTS)
    return agent()


def get_ip():
    return faker.ipv4()


def get_sample_user_request():
    username = get_username()
    total_orders = random.randint(1, 5)
    time_zone = get_random_timezone()
    login_log = customer_login_request(username, time_zone=time_zone)
    user_requests = [login_log]
    prev_log = login_log

    if faker.pybool():
        user_requests.append(error_request(username, time_zone=time_zone))
    # add some customer orders
    for _i in range(total_orders):
        product_id = db.get_product_id()

        log = customer_order_request(username=username, product_id=product_id, time_zone=time_zone)
        # ensure same ip for user login and placing order
        log.remote_host = login_log.remote_host

        # same device for follow up request
        log.user_agent = login_log.user_agent

        # set referer to previous request path

        log.referer = os.path.join(HOST_NAME, prev_log.request_path)

        user_requests.append(log)
        prev_log = log

    return user_requests


def get_username():
    # load sample user
    customer = db.get_random_customer()
    return customer['user_name']


def get_log_file_name(date: datetime):
    utc_d = datetime.utcfromtimestamp(date.timestamp())
    d = utc_d.strftime('%Y_%m_%d')
    ts = str(utc_d.timestamp())
    return '{0}_{1}.log'.format(d, ts)


def get_log_file_path(filename: str):
    return os.path.join(LOGS_DIR, filename)


def generate_logs(*args, **kwargs):
    # generate some random logs
    log_lines = []
    for _i in range(100):
        logs = get_sample_user_request()
        for log in logs:
            log_lines.append(models.WebLog.to_log_line(log))

    file_name = get_log_file_name(datetime.utcnow())
    file_path = get_log_file_path(file_name)
    with open(file_path, 'w') as f:
        f.write('\n'.join(log_lines))
        f.close()



def log_generator(*args, **kwargs):
    print('Kwargs: ', args, kwargs)
    generate_logs()
    db.close_connection()



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['badu.boahen@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(dag_id='web_logs_generator',
          description='A dag for generating sample apache web logs',
          schedule_interval='*/2 * * * *',
          start_date=datetime(2020, 5, 6),
          catchup=False)
log_python_task = PythonOperator(task_id='log_generator',
                                 python_callable=log_generator,
                                 dag=dag)
