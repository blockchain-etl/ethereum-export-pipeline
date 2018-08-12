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
    'start_date': datetime(2015, 7, 30, 15),
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

notification_emails = os.environ.get('NOTIFICATION_EMAILS')
if notification_emails and len(notification_emails) > 0:
    default_dag_args['email'] = [email.strip() for email in notification_emails.split(',')]

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'ethereumetl_export_dag',
        # Daily at 1am
        schedule_interval='30 * * * *',
        default_args=default_dag_args) as dag:
    # Will get rid of this once Google Cloud Composer supports Python 3
    install_python3_command = 'cp $DAGS_FOLDER/resources/miniconda.tar . && ' \
                              'tar xvf miniconda.tar > untar_miniconda.log && ' \
                              'PYTHON3=$PWD/miniconda/bin/python3'

    setup_command = \
        'set -o xtrace && set -o pipefail && ' + install_python3_command + \
        ' && ' \
        'PARTITION_PATH=year=$EXECUTION_DATE_YEAR/month=$EXECUTION_DATE_MONTH/day=$EXECUTION_DATE_DAY/hour=$EXECUTION_DATE_HOUR && ' \
        'git clone --branch $ETHEREUMETL_REPO_BRANCH http://github.com/medvedev1088/ethereum-etl && cd ethereum-etl && ' \
        'BLOCK_RANGE=$($PYTHON3 get_block_range_for_timestamps.py -s $EXECUTION_DATE_START_TIMESTAMP -e $EXECUTION_DATE_END_TIMESTAMP -p $WEB3_PROVIDER_URI) && ' \
        'BLOCK_RANGE_ARRAY=(${BLOCK_RANGE//,/ }) && START_BLOCK=${BLOCK_RANGE_ARRAY[0]} && END_BLOCK=${BLOCK_RANGE_ARRAY[1]} && ' \
        'EXPORT_LOCATION_URI=gs://$OUTPUT_BUCKET/export && ' \
        'export CLOUDSDK_PYTHON=/usr/local/bin/python'

    export_blocks_and_transactions_command = \
        setup_command + ' && ' + \
        'echo $BLOCK_RANGE > blocks_meta.txt && ' \
        '$PYTHON3 export_blocks_and_transactions.py -w 2 -s $START_BLOCK -e $END_BLOCK ' \
        '-p $WEB3_PROVIDER_URI --blocks-output blocks.csv --transactions-output transactions.csv && ' \
        'gsutil cp blocks.csv $EXPORT_LOCATION_URI/blocks/$PARTITION_PATH/blocks.csv && ' \
        'gsutil cp transactions.csv $EXPORT_LOCATION_URI/transactions/$PARTITION_PATH/transactions.csv && ' \
        'gsutil cp blocks_meta.txt $EXPORT_LOCATION_URI/blocks_meta/$PARTITION_PATH/blocks_meta.txt '

    export_receipts_and_logs_command = \
        setup_command + ' && ' + \
        'gsutil cp $EXPORT_LOCATION_URI/transactions/$PARTITION_PATH/transactions.csv transactions.csv && ' \
        '$PYTHON3 extract_csv_column.py -i transactions.csv -o transaction_hashes.txt -c hash && ' \
        '$PYTHON3 export_receipts_and_logs.py -w 2 --transaction-hashes transaction_hashes.txt ' \
        '-p $WEB3_PROVIDER_URI --receipts-output receipts.csv --logs-output logs.json && ' \
        'gsutil cp receipts.csv $EXPORT_LOCATION_URI/receipts/$PARTITION_PATH/receipts.csv && ' \
        'gsutil cp logs.json $EXPORT_LOCATION_URI/logs/$PARTITION_PATH/logs.json '

    export_contracts_command = \
        setup_command + ' && ' + \
        'gsutil cp $EXPORT_LOCATION_URI/receipts/$PARTITION_PATH/receipts.csv receipts.csv && ' \
        '$PYTHON3 extract_csv_column.py -i receipts.csv -o contract_addresses.txt -c contract_address && ' \
        '$PYTHON3 export_contracts.py -w 2 --contract-addresses contract_addresses.txt ' \
        '-p $WEB3_PROVIDER_URI --output contracts.json && ' \
        'gsutil cp contracts.json $EXPORT_LOCATION_URI/contracts/$PARTITION_PATH/contracts.json '

    export_tokens_command = \
        setup_command + ' && ' + \
        'gsutil cp $EXPORT_LOCATION_URI/contracts/$PARTITION_PATH/contracts.json contracts.json && ' \
        '$PYTHON3 filter_items.py -i contracts.json -p "item[\'is_erc20\'] or item[\'is_erc721\']" | ' \
        '$PYTHON3 extract_field.py -f address -o token_addresses.txt && ' \
        '$PYTHON3 export_tokens.py -w 2 --token-addresses token_addresses.txt ' \
        '-p $WEB3_PROVIDER_URI --output tokens.csv && ' \
        'gsutil cp tokens.csv $EXPORT_LOCATION_URI/tokens/$PARTITION_PATH/tokens.csv '

    extract_token_transfers_command = \
        setup_command + ' && ' + \
        'gsutil cp $EXPORT_LOCATION_URI/logs/$PARTITION_PATH/logs.json logs.json && ' \
        '$PYTHON3 extract_token_transfers.py -w 2 --logs logs.json --output token_transfers.csv && ' \
        'gsutil cp token_transfers.csv $EXPORT_LOCATION_URI/token_transfers/$PARTITION_PATH/token_transfers.csv '

    output_bucket = os.environ.get('OUTPUT_BUCKET')
    if output_bucket is None:
        raise ValueError('You must set OUTPUT_BUCKET environment variable')
    web3_provider_uri = os.environ.get('WEB3_PROVIDER_URI', 'https://mainnet.infura.io/')
    ethereumetl_repo_branch = os.environ.get('ETHEREUMETL_REPO_BRANCH', 'master')
    dags_folder = os.environ.get('DAGS_FOLDER', '/home/airflow/gcs/dags')

    # ds is 1 day behind the date on which the run is scheduled, e.g. if the dag is scheduled to run at
    # 1am on January 2, ds will be January 1.
    environment = {
        'EXECUTION_DATE': '{{ ds }}',
        'EXECUTION_DATE_YEAR': '{{ execution_date.year }}',
        'EXECUTION_DATE_MONTH': '{{ execution_date.month }}',
        'EXECUTION_DATE_DAY': '{{ execution_date.day }}',
        'EXECUTION_DATE_HOUR': '{{ execution_date.hour }}',
        'EXECUTION_DATE_START_TIMESTAMP': '{{ macros.time.mktime(execution_date.replace(minute=0).timetuple())|int }}',
        'EXECUTION_DATE_END_TIMESTAMP': '{{ macros.time.mktime(next_execution_date.replace(minute=0).timetuple())|int - 1 }}',
        'ETHEREUMETL_REPO_BRANCH': ethereumetl_repo_branch,
        'WEB3_PROVIDER_URI': web3_provider_uri,
        'OUTPUT_BUCKET': output_bucket,
        'DAGS_FOLDER': dags_folder
    }

    # TODO: Add timeouts

    export_blocks_and_transactions = get_boolean_env_variable('EXPORT_BLOCKS_AND_TRANSACTIONS', True)
    export_receipts_and_logs = get_boolean_env_variable('EXPORT_RECEIPTS_AND_LOGS', True)
    export_contracts = get_boolean_env_variable('EXPORT_CONTRACTS', True)
    export_tokens = get_boolean_env_variable('EXPORT_TOKENS', True)
    extract_token_transfers = get_boolean_env_variable('EXTRACT_TOKEN_TRANSFERS', True)

    if export_blocks_and_transactions:
        export_blocks_and_transactions_operator = bash_operator.BashOperator(
            task_id='export_blocks_and_transactions',
            bash_command=export_blocks_and_transactions_command,
            dag=dag,
            env=environment)

    if export_receipts_and_logs:
        export_receipts_and_logs_operator = bash_operator.BashOperator(
            task_id='export_receipts_and_logs',
            bash_command=export_receipts_and_logs_command,
            dag=dag,
            env=environment)
        if export_blocks_and_transactions:
            export_blocks_and_transactions_operator >> export_receipts_and_logs_operator

    if export_contracts:
        export_contracts_operator = bash_operator.BashOperator(
            task_id='export_contracts',
            bash_command=export_contracts_command,
            dag=dag,
            env=environment)
        if export_receipts_and_logs:
            export_receipts_and_logs_operator >> export_contracts_operator

    if export_tokens:
        export_tokens_operator = bash_operator.BashOperator(
            task_id='export_tokens',
            bash_command=export_tokens_command,
            dag=dag,
            env=environment)
        if export_contracts:
            export_contracts_operator >> export_tokens_operator

    if extract_token_transfers:
        extract_token_transfers_operator = bash_operator.BashOperator(
            task_id='extract_token_transfers',
            bash_command=extract_token_transfers_command,
            dag=dag,
            env=environment)
        if export_receipts_and_logs:
            export_receipts_and_logs_operator >> extract_token_transfers_operator
