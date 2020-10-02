import datetime
import json
import os

import pytz
import requests
from faker import Faker

try:
    import models as models
    import db as db
except Exception as ex:
    print('Ex: ', ex)
    try:
        from dags import models
        from dags import db
    except Exception as exx:
        print('Ex: ', exx)
        pass

faker = Faker()
HTTP_METHODS = ['GET', 'POST', 'DELETE', 'PUT']
HTTP_POST = 'POST'
HTTP_GET = 'GET'

HTTP_METHODS_WIEGHTS = [60, 20, 10, 10]
HTTP_STATUS_CODES = ["200", "404", "500", "301"]

START_DATE = datetime.datetime(2019, 5, 1)
END_DATE = datetime.datetime.now()
REQUEST_PATHS = [
    '/products/',
    '/customers/',
    '/orders/',
    '/login',
    '/logout',
    '/device'
]

LOGS_DIR = os.path.join(os.path.dirname(__file__), 'apache_logs')

try:
    os.mkdir(LOGS_DIR)
except:
    pass
CUSTOMERS = set()
PRODUCTS = set()
COMPANIES = set()

ALL_TIMEZONES = list(pytz.all_timezones)
TOTAL_TIMEZONES = len(ALL_TIMEZONES)


def get_location_by_ip(ip: str):
    try:
        headers = {'User-Agent': 'curl/7.64.1'}
        response = requests.get('https://ipinfo.io/{0}'.format(ip), {'headers': headers})
        data = response.content
        print(response.content)
        info = json.loads(data)
        return info
    except:
        return None


def to_utc(date: datetime.datetime):
    dd = datetime.datetime.utcfromtimestamp(date.timestamp())
    return dd


def get_log_file_name(date: datetime.datetime):
    utc_d = to_utc(date)
    d = utc_d.strftime('%Y_%m_%d')
    ts = str(utc_d.timestamp())
    return '{0}_{1}.log'.format(d, ts)


def get_log_file_path(filename: str):
    return os.path.join(LOGS_DIR, filename)


def write_log_to_file(log: models.WebLog):
    log_line = models.WebLog.to_log_line(log)
    log_file = get_log_file_name(log.parsed_request_time)
    file_path = get_log_file_path(log_file)

    with open(file_path, 'a') as f:
        f.write(log_line + '\n')
        f.close()


def generate_log_line():
    pass
