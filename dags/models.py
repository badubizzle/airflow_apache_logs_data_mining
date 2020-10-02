import datetime
import json
import re
import uuid
from urllib.parse import urlparse, parse_qs

import requests
import user_agents
from faker import Faker

faker_instance = Faker()

IP_INFO_CACHE = {}


def get_faker():
    global faker_instance
    return faker_instance


def get_unique_int():
    return uuid.uuid4().int >> 64


class Product:

    def __init__(self):
        self.name = None
        self.price = None
        self.id = None


class ProductCatalog:
    def __init__(self):
        self.product_id = None
        self.price = None
        self.company_id = None

    @classmethod
    def generate_product(cls, company_id=None, product_id=None):
        product_id = product_id if product_id else get_unique_int()
        price = float(get_faker().random_int(10, 100))
        product = ProductCatalog()
        product.product_id = product_id
        product.price = price
        product.company_id = company_id if company_id else get_unique_int()
        return product


PRODUCTS_DB = {

}


def load_dummy_products():
    global PRODUCTS_DB
    PRODUCTS_DB = {}
    while len(PRODUCTS_DB) < 10:

        product = ProductCatalog.generate_product()
        if product.product_id not in PRODUCTS_DB:
            PRODUCTS_DB[product.product_id] = product


load_dummy_products()


class MarketLead:
    CSV_HEADER = ['id', 'quote_product_id', 'quote_price', 'quote_value', 'sale_flag', 'order_id', 'customer_id',
                  'company_id', 'created_date']

    def __init__(self):
        self.id = None
        self.quote_product_id = None
        self.quote_price = None
        self.quote_value = None
        self.sale_flag = False
        self.order_id = None
        self.customer_id = None
        self.company_id = None
        self.created_date = datetime.datetime.utcnow()

    @classmethod
    def generate_lead(cls, company_id=None, product_id=None, customer_id=None):
        lead = MarketLead()
        lead.id = get_unique_int()
        lead.order_id = get_unique_int()
        lead.quote_price = float(get_faker().random_int(10, 100))
        lead.customer_id = customer_id if customer_id else get_unique_int()
        lead.company_id = company_id if company_id else get_unique_int()
        lead.quote_value = float(get_faker().random_int(10, 100))
        lead.quote_product_id = product_id if product_id else get_unique_int()

        return lead

    def to_dict(self):
        data = self.__dict__
        data['created_date'] = self.created_date.isoformat()

        return data


class UserDevice:

    def __init__(self):
        self.is_pc = None
        self.is_mobile = None
        self.is_tablet = None
        self.is_iphone = None
        self.is_mac = None
        self.browser = None
        self.device_name = None


class WebLog:
    LOG_FORMAT = '{remote_host} {user_identity} {username} [{formatted_request_time}] "{request_method} {request_path} HTTP/1.0" {response_status} {response_size} "{referer}" "{user_agent}"'
    WEB_LOG_REGEX = r'(?P<remote_host>[(\d\.)]+) (?P<user_identity>.*?) (?P<username>.*?) \[(?P<request_time>.*?) (?P<request_time_zone_offset>.*?)\] "(?P<request_method>\w+) (?P<request_path>.*?) HTTP/(?P<http_version>.*?)" (?P<response_status>\d+) (?P<response_size>\d+) "(?P<referer>.*?)" "(?P<user_agent>.*?)"'
    LOG_REGEX_COMPILED = re.compile(WEB_LOG_REGEX)

    COMPANY_PLACE_CUSTOMER_PATH = '/company/order'
    CUSTOMER_ORDER_PATH = '/customer/order'
    LOGIN_PATH = r'/login'

    CSV_HEADER = ['user', 'request_type', 'request_path', 'country',
                  'city', 'device', 'is_pc', 'is_mobile',
                  'request_date', 'timestamp',
                  'product_id', 'customer_id', 'company_id']

    def __init__(self):
        self.raw_log = None
        self.remote_host = None
        self.user_identity = '-'
        self.username = None
        self.user_type = None
        self.request_time: str = None
        self.parsed_request_time: datetime.datetime = None
        self.request_time_zone_offset = None
        self.request_time_utc = None
        self.request_path = None
        self.request_method = None
        self.response_status = None
        self.response_size = None
        self.referer = None
        self.user_agent = None
        self.ip_info = None
        self.country = None
        self.city = None
        self.user_device: UserDevice = None
        self.request_type = None
        self.request_params = {}
        self.is_error = None

    def to_dict(self):
        data = {'user': self.username}
        data['request_type'] = self.request_type
        data['request_path'] = self.request_path
        data['country'] = self.country
        data['city'] = self.city
        data['device'] = self.user_device.device_name
        data['is_pc'] = self.user_device.is_pc
        data['is_mobile'] = self.user_device.is_mobile
        data['request_date'] = self.parsed_request_time.isoformat()
        data['timestamp'] = self.parsed_request_time.timestamp()
        data['product_id'] = self.request_params.get('product_id')
        data['customer_id'] = self.request_params.get('customer_id')
        data['company_id'] = self.request_params.get('company_id')
        return data

    @classmethod
    def parse_request_type(cls, request_path):
        p = urlparse(request_path)
        if p:
            params = {k: ",".join(v) for k, v in parse_qs(p.query).items()}

            if p.path == WebLog.LOGIN_PATH:
                return 'login', {}
            elif p.path == WebLog.CUSTOMER_ORDER_PATH:
                return 'customer_order', params
            elif p.path == WebLog.COMPANY_PLACE_CUSTOMER_PATH:
                return 'company_placed_order', params

        return None, {}

    @classmethod
    def get_location_by_ip(cls, ip: str):
        global IP_INFO_CACHE
        if ip in IP_INFO_CACHE:
            return IP_INFO_CACHE.get(ip)
        try:
            headers = {'User-Agent': 'curl/7.64.1'}
            response = requests.get(
                'https://ipinfo.io/{0}'.format(ip), {'headers': headers})
            json_string = response.content
            info = json.loads(json_string)
            IP_INFO_CACHE[ip] = info
            return info
        except:
            return None

    @classmethod
    def to_log_line(cls, log):
        data = log.__dict__
        data['formatted_request_time'] = WebLog.format_request_time(
            log.parsed_request_time)
        return WebLog.LOG_FORMAT.format(**data)

    @classmethod
    def parse(cls, line: str):
        match = WebLog.LOG_REGEX_COMPILED.match(line)
        data_dict = match.groupdict()
        log = WebLog()
        log.raw_log = line
        log.referer = data_dict.get('referer')
        log.remote_host = data_dict.get('remote_host')
        log.request_method = data_dict.get('request_method')
        log.request_path = data_dict.get('request_path')
        log.request_time = data_dict.get('request_time')

        log.request_time_zone_offset = data_dict.get(
            'request_time_zone_offset')
        log.parsed_request_time = WebLog.string_to_date_time(
            log.request_time, log.request_time_zone_offset)
        log.user_agent = data_dict.get('user_agent')
        log.response_status = data_dict.get('response_status')
        log.response_size = data_dict.get('response_size')
        log.username = data_dict.get('username')
        log.user_identity = data_dict.get('user_identity')
        log.ip_info = WebLog.get_location_by_ip(log.remote_host)
        if isinstance(log.ip_info, dict):
            log.city = log.ip_info.get('city')
            log.country = log.ip_info.get('country')
        log.user_device = WebLog.parse_user_agent(log.user_agent)
        log.is_error = int(log.response_status) >= 400
        log.request_type, log.request_params = WebLog.parse_request_type(
            log.request_path)
        return log

    @classmethod
    def string_to_date_time(cls, date_time, offset):
        d = datetime.datetime.strptime(
            date_time + ' ' + offset, "%d/%b/%Y:%H:%M:%S %z")
        return d

    @classmethod
    def format_request_time(cls, time: datetime):
        return time.strftime('%d/%b/%Y:%H:%M:%S %z')

    @classmethod
    def parse_user_agent(cls, user_agent: str):
        result = user_agents.parse(user_agent)
        if result:
            d = UserDevice()
            d.browser = result.browser.family
            d.device_name = result.device.family
            d.is_mac = result.is_pc and d.device_name == 'Mac'
            d.is_pc = result.is_pc
            d.is_iphone = result.is_mobile and 'iphone' in d.device_name.lower()
            d.is_tablet = result.is_tablet
            d.is_mobile = result.is_mobile
            return d

        return None


class HTTPMethod:
    GET = 'GET'
    POST = 'POST'
    DELETE = 'DELETE'
    PUT = 'PUT'
    HTTP_METHODS = [GET, POST, DELETE, PUT]
    HTTP_METHODS_WIEGHTS = [60, 20, 10, 10]

    HTTP_CODE_OK = '200'
    HTTP_CODE_NOT_FOUND = '404'
    HTTP_CODE_BAD_REQUEST = '400'
    HTTP_CODE_SERVER_ERROR = '500'
    HTTP_CODE_REDIRECT = '301'
    HTTP_STATUS_CODES = [HTTP_CODE_OK,
                         HTTP_CODE_NOT_FOUND, HTTP_CODE_BAD_REQUEST, HTTP_CODE_SERVER_ERROR, HTTP_CODE_REDIRECT]
    HTTP_STATUS_ERROR = [HTTP_CODE_NOT_FOUND,
                         HTTP_CODE_BAD_REQUEST, HTTP_CODE_SERVER_ERROR]
