from __future__ import print_function

import os
from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
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
        'set -o xtrace && set -o pipefail && ' \
        'echo "OUTPUT_BUCKET: $OUTPUT_BUCKET" && ' \
        'echo "ETHEREUMETL_REPO_BRANCH: $ETHEREUMETL_REPO_BRANCH" && ' \
        'EXPORT_LOCATION_URI=gs://$OUTPUT_BUCKET/export && ' \
        'git clone --branch $ETHEREUMETL_REPO_BRANCH http://github.com/medvedev1088/ethereum-etl && cd ethereum-etl && ' \
        'export CLOUDSDK_PYTHON=/usr/local/bin/python'

    output_bucket = os.environ.get('OUTPUT_BUCKET')
    if output_bucket is None:
        raise ValueError('You must set OUTPUT_BUCKET environment variable')
    ethereumetl_repo_branch = os.environ.get('ETHEREUMETL_REPO_BRANCH', 'master')

    environment = {
        'ETHEREUMETL_REPO_BRANCH': ethereumetl_repo_branch,
        'OUTPUT_BUCKET': output_bucket
    }

    bigquery_destination_project_id = os.environ.get('BIGQUERY_DESTINATION_PROJECT_ID', 'bigquery-public-data')


    def add_load_tasks(task, file_format, extra_options=''):
        wait_sensor = GoogleCloudStorageObjectSensor(
            task_id='wait_latest_{task}'.format(task=task),
            dag=dag,
            timeout=60 * 60,
            poke_interval=60,
            bucket=output_bucket,
            object='export/{task}/block_date={datestamp}/{task}.{file_format}'.format(
                task=task, datestamp='{{ds}}', file_format=file_format)
        )
        source_format = 'CSV' if file_format == 'csv' else 'NEWLINE_DELIMITED_JSON'
        skip_leading_rows = '--skip_leading_rows=1' if file_format == 'csv' else ''
        load_bash_command = \
            setup_command + ' && ' + \
            ('bq --location=US load --replace --source_format={source_format} {skip_leading_rows} {extra_options} ' +
             'ethereum_blockchain_raw.{task} $EXPORT_LOCATION_URI/{task}/*.{file_format} ./schemas/gcp/raw/{task}.json ').format(
                task=task, source_format=source_format, skip_leading_rows=skip_leading_rows,
                extra_options=extra_options, file_format=file_format)

        load_operator = BashOperator(
            task_id='load_{task}'.format(task=task),
            execution_timeout=timedelta(minutes=30),
            bash_command=load_bash_command,
            dag=dag,
            env=environment)

        wait_sensor >> load_operator
        return load_operator


    def add_enrich_tasks(task, time_partitioning_field='block_timestamp', dependencies=None):
        time_partitioning_field_option = '--time_partitioning_field ' + time_partitioning_field if time_partitioning_field is not None else ''
        project_id_prefix = bigquery_destination_project_id + ':' if bigquery_destination_project_id else ''
        enrich_bash_command = \
            setup_command + ' && ' + \
            'CURRENT_TIMESTAMP=$(date +%s%N) && ' + \
            ('bq mk --table --description "$(cat ./schemas/gcp/enriched/descriptions/{task}.txt | tr \'\n\' \' \')"' +
             ' {time_partitioning_field_option} ' +
             'ethereum_blockchain_temp.{task}_$CURRENT_TIMESTAMP ./schemas/gcp/enriched/{task}.json').format(
                task=task, time_partitioning_field_option=time_partitioning_field_option) + ' && ' + \
            ('bq --location=US query --destination_table ethereum_blockchain_temp.{task}_$CURRENT_TIMESTAMP ' +
             '--use_legacy_sql=false ' +
             '"$(cat ./schemas/gcp/enriched/sqls/{task}.sql | tr \'\n\' \' \')"').format(
                task=task) + ' && ' + \
            ('bq --location=US cp --force ethereum_blockchain_temp.{task}_$CURRENT_TIMESTAMP ' +
             'ethereum_blockchain.{task}').format(
                task=task) + ' && ' + \
            ('bq --location=US cp --force ethereum_blockchain_temp.{task}_$CURRENT_TIMESTAMP ' +
             '{project_id_prefix}ethereum_blockchain.{task}').format(
                task=task, project_id_prefix=project_id_prefix) + ' && ' + \
            ('bq --location=US rm --force --table ethereum_blockchain_temp.{task}_$CURRENT_TIMESTAMP').format(
                task=task)

        enrich_operator = BashOperator(
            task_id='enrich_{task}'.format(task=task),
            execution_timeout=timedelta(minutes=30),
            bash_command=enrich_bash_command,
            dag=dag,
            env=environment)

        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> enrich_operator
        return enrich_operator


    def add_validate_tasks(task, sql, dependencies=None):
        validated_task = BigQueryOperator(
            task_id='validate_{task}'.format(task=task),
            bql=sql,
            use_legacy_sql=False,
            dag=dag)
        if dependencies is not None and len(dependencies) > 0:
            for dependency in dependencies:
                dependency >> validated_task
        return validated_task


    load_blocks_task = add_load_tasks('blocks', 'csv')
    load_transactions_task = add_load_tasks('transactions', 'csv')
    load_receipts_task = add_load_tasks('receipts', 'csv')
    load_logs_task = add_load_tasks('logs', 'json')
    load_contracts_task = add_load_tasks('contracts', 'json')
    load_tokens_task = add_load_tasks('tokens', 'csv', '--allow_quoted_newlines')
    load_token_transfers_task = add_load_tasks('token_transfers', 'csv')

    enrich_blocks_task = add_enrich_tasks(
        'blocks', time_partitioning_field='timestamp', dependencies=[load_blocks_task])
    enrich_transactions_task = add_enrich_tasks(
        'transactions', dependencies=[load_blocks_task, load_transactions_task, load_receipts_task])
    enrich_logs_task = add_enrich_tasks(
        'logs', dependencies=[load_blocks_task, load_logs_task])
    enrich_contracts_task = add_enrich_tasks(
        'contracts', dependencies=[load_blocks_task, load_contracts_task])
    enrich_tokens_task = add_enrich_tasks(
        'tokens', time_partitioning_field=None, dependencies=[load_tokens_task])
    enrich_token_transfers_task = add_enrich_tasks(
        'token_transfers', dependencies=[load_blocks_task, load_token_transfers_task])

    # The query below will fail when the condition is not met
    # Have to use this trick since the Python 2 version of BigQueryCheckOperator doesn't support standard SQL
    # and legacy SQL can't be used to query partitioned tables.
    validate_blocks_sql = '''
    SELECT IF(
    (SELECT MAX(number) FROM `bigquery-public-data.ethereum_blockchain.blocks`) + 1 = 
    (SELECT COUNT(*) FROM `bigquery-public-data.ethereum_blockchain.blocks`) AND 
    (SELECT COUNT(*) FROM `bigquery-public-data.ethereum_blockchain.blocks` WHERE DATE(timestamp) = '{{ds}}') > 0, 1, 
    CAST((SELECT 'Total number of blocks except genesis is not equal to last block number or there are no blocks on {{ds}}') AS INT64))
    '''
    add_validate_tasks('blocks', validate_blocks_sql, [enrich_blocks_task])

    validate_transactions_sql = '''
    SELECT IF((SELECT sum(transaction_count) FROM `bigquery-public-data.ethereum_blockchain.blocks`) = 
    (SELECT COUNT(*) FROM `bigquery-public-data.ethereum_blockchain.transactions`) AND 
    (SELECT COUNT(*) FROM `bigquery-public-data.ethereum_blockchain.transactions` WHERE DATE(block_timestamp) = '{{ds}}') > 0, 1, 
    CAST((SELECT 'Total number of transactions is not equal to sum of transaction_count in blocks table or there are no transactions on {{ds}}') AS INT64))
    '''
    add_validate_tasks('transactions', validate_transactions_sql, [enrich_blocks_task, enrich_transactions_task])

    validate_logs_sql = '''
        SELECT IF(
        (SELECT COUNT(*) FROM `bigquery-public-data.ethereum_blockchain.logs` WHERE DATE(block_timestamp) = '{{ds}}') > 0, 1, 
        CAST((SELECT 'There are no logs on {{ds}}') AS INT64))    
        '''
    add_validate_tasks('logs', validate_logs_sql, [enrich_logs_task])

    validate_token_transfers_sql = '''
    SELECT IF(
    (SELECT COUNT(*) FROM `bigquery-public-data.ethereum_blockchain.token_transfers` WHERE DATE(block_timestamp) = '{{ds}}') > 0, 1, 
    CAST((SELECT 'There are no token transfers on {{ds}}') AS INT64))    
    '''
    add_validate_tasks('token_transfers', validate_token_transfers_sql, [enrich_token_transfers_task])
