from __future__ import print_function

import os
from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators.bash_operator import BashOperator


def get_boolean_env_variable(env_variable_name, default=True):
    raw_env = os.environ.get(env_variable_name)
    if raw_env is None or len(raw_env) == 0:
        return default
    else:
        return raw_env.lower() in ['true', 'yes']


# TODO start_date must be in UTC
default_dag_args = {
    'depends_on_past': False,
    'start_date': datetime(2018, 7, 1),
    'email': ['evge.medvedev@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'ethereumetl_load_dag',
        catchup=False,
        # Daily at 1am
        schedule_interval='30 1 * * *',
        default_args=default_dag_args) as dag:
    setup_command = \
        'set -o xtrace && ' \
        'echo "OUTPUT_BUCKET: $OUTPUT_BUCKET" && ' \
        'echo "EXECUTION_DATE: $EXECUTION_DATE" && ' \
        'echo "ETHEREUMETL_REPO_BRANCH: $ETHEREUMETL_REPO_BRANCH" && ' \
        'EXPORT_LOCATION_URI=gs://$OUTPUT_BUCKET/export && ' \
        'git clone --branch $ETHEREUMETL_REPO_BRANCH http://github.com/medvedev1088/ethereum-etl && cd ethereum-etl && ' \
        'export CLOUDSDK_PYTHON=/usr/local/bin/python'

    output_bucket = os.environ.get('OUTPUT_BUCKET')
    if output_bucket is None:
        raise ValueError('You must set OUTPUT_BUCKET environment variable')
    ethereumetl_repo_branch = os.environ.get('ETHEREUMETL_REPO_BRANCH', 'master')

    environment = {
        'EXECUTION_DATE': '{{ ds }}',
        'ETHEREUMETL_REPO_BRANCH': ethereumetl_repo_branch,
        'OUTPUT_BUCKET': output_bucket
    }

    load_blocks = get_boolean_env_variable('LOAD_BLOCKS', True)
    load_transactions = get_boolean_env_variable('LOAD_TRANSACTIONS', True)
    load_receipts = get_boolean_env_variable('LOAD_RECEIPTS', True)
    load_logs = get_boolean_env_variable('LOAD_LOGS', True)
    load_contracts = get_boolean_env_variable('LOAD_CONTRACTS', True)
    load_transfers = get_boolean_env_variable('LOAD_TRANSFERS', True)


    def add_load_tasks(task, file_format):
        wait_sensor = GoogleCloudStorageObjectSensor(
            task_id='wait_latest_{}'.format(task),
            dag=dag,
            timeout=60 * 30,
            poke_interval=60,
            bucket=output_bucket,
            object='export/{}/block_date={}/{}.{}'.format(task, '{{ds}}', task, file_format)
        )
        source_format = 'CSV' if file_format == 'csv' else 'NEWLINE_DELIMITED_JSON'
        skip_leading_rows = '--skip_leading_rows=1' if file_format == 'csv' else ''
        bash_command = \
            setup_command + ' && ' + \
            ('bq --location=US load --replace --source_format={} {} ' +
             'ethereum_blockchain.{} $EXPORT_LOCATION_URI/{}/*.{} ./schemas/gcp/{}.json ').format(
                source_format, skip_leading_rows, task, task, file_format, task)

        load_operator = BashOperator(
            task_id='load_{}'.format(task),
            execution_timeout=60 * 30,
            bash_command=bash_command,
            dag=dag,
            env=environment)
        wait_sensor >> load_operator


    if load_blocks:
        add_load_tasks('blocks', 'csv')

    if load_transactions:
        add_load_tasks('transactions', 'csv')

    if load_receipts:
        add_load_tasks('receipts', 'csv')

    if load_logs:
        add_load_tasks('logs', 'json')

    if load_contracts:
        add_load_tasks('contracts', 'json')

    if load_transfers:
        add_load_tasks('erc20_transfers', 'csv')
