from datetime import datetime, timedelta
import json

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.redis.operators.redis_publish import RedisPublishOperator
from airflow.models import DAG
import redis


def get_rates(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='pg')
    api_hook = HttpHook(http_conn_id='exchangerate', method='GET')

    # If either of these raises an exception then we'll be notified via
    # Airflow
    resp = api_hook.run('')
    resp = json.loads(resp.content)

    # These are the only valid pairs the DB supports at the moment. Anything
    # else that turns up will be ignored.
    valid_pairs = (
        'AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS',
        'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT', 'BGN',
        'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BRL', 'BSD',
        'BTC', 'BTN', 'BWP', 'BYN', 'BYR', 'BZD', 'CAD',
        'CDF', 'CHF', 'CLF', 'CLP', 'CNY', 'COP', 'CRC',
        'CUC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP',
        'DZD', 'EEK', 'EGP', 'ERN', 'ETB', 'EUR', 'FJD',
        'FKP', 'GBP', 'GEL', 'GGP', 'GHS', 'GIP', 'GMD',
        'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG',
        'HUF', 'IDR', 'ILS', 'IMP', 'INR', 'IQD', 'IRR',
        'ISK', 'JEP', 'JMD', 'JOD', 'JPY', 'KES', 'KGS',
        'KHR', 'KMF', 'KPW', 'KRW', 'KWD', 'KYD', 'KZT',
        'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LVL',
        'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MNT',
        'MOP', 'MRO', 'MTL', 'MUR', 'MVR', 'MWK', 'MXN',
        'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR',
        'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR',
        'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF',
        'SAR', 'SBD', 'SCR', 'SDG', 'SEK', 'SGD', 'SHP',
        'SLL', 'SOS', 'SRD', 'STD', 'SVC', 'SYP', 'SZL',
        'THB', 'TJS', 'TMT', 'TND', 'TOP', 'TRY', 'TTD',
        'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'UYU', 'UZS',
        'VEF', 'VND', 'VUV', 'WST', 'XAF', 'XAG', 'XAU',
        'XCD', 'XDR', 'XOF', 'XPD', 'XPF', 'XPT', 'YER',
        'ZAR', 'ZMK', 'ZMW', 'ZWL')

    rates_insert = """INSERT INTO rates (pair, valid_until, rate)
                      VALUES (%s, %s, %s);"""

    # If this raises an exception then we'll be notified via Airflow
    valid_until = resp['date']

    for (iso2, rate) in resp['rates'].items():

        rate = float(rate)
        iso2 = iso2.upper().strip()
        if iso2 not in valid_pairs or rate < 0:
            continue

        pg_hook.run(rates_insert, parameters=(iso2,
                                              valid_until,
                                              rate))


def cache_latest_rates(ds, **kwargs):
    redis_conn = redis.StrictRedis(host='redis')

    pg_hook = PostgresHook(postgres_conn_id='pg')
    latest_rates = """SELECT DISTINCT ON (pair)
                             pair, rate
                      FROM   rates
                      ORDER  BY pair, valid_until DESC;"""

    for iso2, rate in pg_hook.get_records(latest_rates):
        redis_conn.set(iso2, rate)

args = {
    'owner': 'yuliya',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Run at the top of the hour Monday to Friday.
# Note: This doesn't line up with the market hours of
# 10PM Sunday till 10PM Friday GMT.
dag = DAG(dag_id='rates',
          default_args=args,
          schedule_interval='0 * * * 1,2,3,4,5',
          dagrun_timeout=timedelta(seconds=30))

create_rates_table_task = PostgresOperator(
    task_id="create_rates_table",
    postgres_conn_id="pg",
    dag=dag,
    sql="""
	DROP TYPE IF EXISTS USD_PAIR;
	CREATE TYPE USD_PAIR AS ENUM (
    'AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS',
    'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT', 'BGN',
    'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BRL', 'BSD',
    'BTC', 'BTN', 'BWP', 'BYN', 'BYR', 'BZD', 'CAD',
    'CDF', 'CHF', 'CLF', 'CLP', 'CNY', 'COP', 'CRC',
    'CUC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP',
    'DZD', 'EEK', 'EGP', 'ERN', 'ETB', 'EUR', 'FJD',
    'FKP', 'GBP', 'GEL', 'GGP', 'GHS', 'GIP', 'GMD',
    'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG',
    'HUF', 'IDR', 'ILS', 'IMP', 'INR', 'IQD', 'IRR',
    'ISK', 'JEP', 'JMD', 'JOD', 'JPY', 'KES', 'KGS',
    'KHR', 'KMF', 'KPW', 'KRW', 'KWD', 'KYD', 'KZT',
    'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LVL',
    'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MNT',
    'MOP', 'MRO', 'MTL', 'MUR', 'MVR', 'MWK', 'MXN',
    'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR',
    'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR',
    'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF',
    'SAR', 'SBD', 'SCR', 'SDG', 'SEK', 'SGD', 'SHP',
    'SLL', 'SOS', 'SRD', 'STD', 'SVC', 'SYP', 'SZL',
    'THB', 'TJS', 'TMT', 'TND', 'TOP', 'TRY', 'TTD',
    'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'UYU', 'UZS',
    'VEF', 'VND', 'VUV', 'WST', 'XAF', 'XAG', 'XAU',
    'XCD', 'XDR', 'XOF', 'XPD', 'XPF', 'XPT', 'YER',
    'ZAR', 'ZMK', 'ZMW', 'ZWL');

        --DROP TABLE IF EXISTS rates;
	CREATE TABLE IF NOT EXISTS rates (
                pk SERIAL,
                pair USD_PAIR,
                valid_until TIMESTAMP WITH TIME ZONE,
                rate FLOAT,
        CONSTRAINT pk PRIMARY KEY (pk, pair, valid_until)
        );""",
)

get_rates_task = \
    PythonOperator(task_id='get_rates',
                   provide_context=True,
                   python_callable=get_rates,
                   dag=dag)

cache_latest_rates_task = \
    PythonOperator(task_id='cache_latest_rates',
                   provide_context=True,
                   python_callable=cache_latest_rates,
                   dag=dag)

create_rates_table_task.set_downstream(get_rates_task)
get_rates_task.set_downstream(cache_latest_rates_task)
