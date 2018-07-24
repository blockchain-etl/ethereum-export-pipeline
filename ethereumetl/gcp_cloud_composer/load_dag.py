from __future__ import print_function

import os
from datetime import datetime, timedelta

from airflow import models
from airflow.operators import bash_operator


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
        'echo "OUTPUT_BUCKET: $OUTPUT_BUCKET" && ' \
        'echo "EXECUTION_DATE: $EXECUTION_DATE" && ' \
        'echo "ETHEREUMETL_REPO_BRANCH: $ETHEREUMETL_REPO_BRANCH" && ' \
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

    if load_blocks:
        load_blocks_operator = bash_operator.BashOperator(
            task_id='load_blocks',
            bash_command=setup_command + ' && ' + 'bq --location=US load --replace --source_format=CSV --skip_leading_rows=1 '
                                                  'ethereum.blocks gs://$OUTPUT_BUCKET/blocks/*.csv ./schemas/gcp/blocks.json ',
            dag=dag,
            env=environment)

    if load_transactions:
        load_transactions_operator = bash_operator.BashOperator(
            task_id='load_transactions',
            bash_command=setup_command + ' && ' + 'bq --location=US load --replace --source_format=CSV --skip_leading_rows=1 '
                                                  'ethereum.transactions gs://$OUTPUT_BUCKET/transactions/*.csv ./schemas/gcp/transactions.json ',
            dag=dag,
            env=environment)
