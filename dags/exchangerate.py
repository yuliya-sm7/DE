from airflow.utils.dates import days_ago
import json

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import DAG


def get_rates(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='pg')
    api_hook = HttpHook(http_conn_id='exchangerate', method='GET')
    currency = 'BTC'
    resp = api_hook.run('latest',
		{'base':currency, 'symbols':'USD'})
    resp = json.loads(resp.content)

    rates_insert = """INSERT INTO rates
			(currency, valid_from, rate)
                      	VALUES (%s, %s, %s)
		      ON CONFLICT (currency, valid_from)
			DO UPDATE SET
		 	rate= EXCLUDED.rate;"""

    for cur, rate in resp['rates'].items():
    	pg_hook.run(rates_insert, parameters=(cur, days_ago(0), rate))


args = {
    'owner': 'yuliya',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'retries': 2,
}

dag = DAG(dag_id='rates',
          default_args=args,
          schedule_interval='0 */3 * * *')

get_rates_task = \
    PythonOperator(task_id='get_rates',
                   provide_context=True,
                   python_callable=get_rates,
                   dag=dag)
