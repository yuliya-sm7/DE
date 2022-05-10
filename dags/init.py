import json

from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import DAG


def get_rates(ds, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='pg')
    api_hook = HttpHook(http_conn_id='exchangerate', method='GET')
    currency = 'BTC'
    resp = api_hook.run('timeseries',
		{'base':currency, 'symbols':'USD', 'start_date':days_ago(365), 'end_date':days_ago(0)})
    resp = json.loads(resp.content)

    rates_insert = """INSERT INTO rates
			(currency, valid_from, rate)
                      	VALUES (%s, %s, %s);"""

    for date, pairs in resp['rates'].items():
        for cur, rate in pairs.items():
    	    pg_hook.run(rates_insert, parameters=(cur, date, rate))


args = {
    'owner': 'yuliya',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'retries': 2,
}

dag = DAG(dag_id='init',
          default_args=args,
          schedule_interval=None)

create_rates_table_task = PostgresOperator(
    task_id="create_rates_table",
    postgres_conn_id="pg",
    dag=dag,
    sql="""
	DROP TABLE IF EXISTS rates;
	CREATE TABLE rates (
                currency VARCHAR,
                valid_from TIMESTAMP,
                rate FLOAT,
        PRIMARY KEY (currency, valid_from)
        );""",
)

get_rates_task = \
    PythonOperator(task_id='get_rates',
                   provide_context=True,
                   python_callable=get_rates,
                   dag=dag)

create_rates_table_task.set_downstream(get_rates_task)
