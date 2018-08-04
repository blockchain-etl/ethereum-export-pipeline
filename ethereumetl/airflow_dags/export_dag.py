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
    'start_date': datetime(2015, 7, 30),
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
        'ethereumetl_export_dag',
        # Daily at 1am
        schedule_interval='0 1 * * *',
        default_args=default_dag_args) as dag:
    # Will get rid of this once Google Cloud Composer supports Python 3
    install_python3_command = 'cp $DAGS_FOLDER/resources/miniconda.tar . && ' \
                              'tar xvf miniconda.tar > untar_miniconda.log && ' \
                              'PYTHON3=$PWD/miniconda/bin/python3'

    setup_command = \
        'set -o xtrace && ' + install_python3_command + \
        ' && ' \
        'git clone --branch $ETHEREUMETL_REPO_BRANCH http://github.com/medvedev1088/ethereum-etl && cd ethereum-etl && ' \
        'BLOCK_RANGE=$($PYTHON3 get_block_range_for_date.py -d $EXECUTION_DATE -p $WEB3_PROVIDER_URI) && ' \
        'BLOCK_RANGE_ARRAY=(${BLOCK_RANGE//,/ }) && START_BLOCK=${BLOCK_RANGE_ARRAY[0]} && END_BLOCK=${BLOCK_RANGE_ARRAY[1]} && ' \
        'EXPORT_LOCATION_URI=gs://$OUTPUT_BUCKET/export && ' \
        'export CLOUDSDK_PYTHON=/usr/local/bin/python'

    export_blocks_and_transactions_command = \
        setup_command + ' && ' + \
        'echo $BLOCK_RANGE > blocks_meta.txt && ' \
        '$PYTHON3 export_blocks_and_transactions.py -s $START_BLOCK -e $END_BLOCK ' \
        '-p $WEB3_PROVIDER_URI --blocks-output blocks.csv --transactions-output transactions.csv && ' \
        'gsutil cp blocks.csv $EXPORT_LOCATION_URI/blocks/block_date=$EXECUTION_DATE/blocks.csv && ' \
        'gsutil cp transactions.csv $EXPORT_LOCATION_URI/transactions/block_date=$EXECUTION_DATE/transactions.csv && ' \
        'gsutil cp blocks_meta.txt $EXPORT_LOCATION_URI/blocks_meta/block_date=$EXECUTION_DATE/blocks_meta.txt '

    export_receipts_and_logs_command = \
        setup_command + ' && ' + \
        'gsutil cp $EXPORT_LOCATION_URI/transactions/block_date=$EXECUTION_DATE/transactions.csv transactions.csv && ' \
        '$PYTHON3 extract_csv_column.py -i transactions.csv -o transaction_hashes.csv -c transaction_hash && ' \
        '$PYTHON3 export_receipts_and_logs.py --transaction-hashes transaction_hashes.csv ' \
        '-p $WEB3_PROVIDER_URI --receipts-output receipts.csv --logs-output logs.json && ' \
        'gsutil cp receipts.csv $EXPORT_LOCATION_URI/receipts/block_date=$EXECUTION_DATE/receipts.csv && ' \
        'gsutil cp logs.json $EXPORT_LOCATION_URI/logs/block_date=$EXECUTION_DATE/logs.json '

    export_contracts_command = \
        setup_command + ' && ' + \
        'gsutil cp $EXPORT_LOCATION_URI/receipts/block_date=$EXECUTION_DATE/receipts.csv receipts.csv && ' \
        '$PYTHON3 extract_csv_column.py -i receipts.csv -o contract_addresses.csv -c receipt_contract_address && ' \
        '$PYTHON3 export_contracts.py --contract-addresses contract_addresses.csv ' \
        '-p $WEB3_PROVIDER_URI --output contracts.json && ' \
        'gsutil cp contracts.json $EXPORT_LOCATION_URI/contracts/block_date=$EXECUTION_DATE/contracts.json '

    extract_erc20_transfers_command = \
        setup_command + ' && ' + \
        'gsutil cp $EXPORT_LOCATION_URI/logs/block_date=$EXECUTION_DATE/logs.json logs.json && ' \
        '$PYTHON3 extract_erc20_transfers.py --logs logs.json --output erc20_transfers.csv && ' \
        'gsutil cp erc20_transfers.csv $EXPORT_LOCATION_URI/erc20_transfers/block_date=$EXECUTION_DATE/erc20_transfers.csv '

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
        'ETHEREUMETL_REPO_BRANCH': ethereumetl_repo_branch,
        'WEB3_PROVIDER_URI': web3_provider_uri,
        'OUTPUT_BUCKET': output_bucket,
        'DAGS_FOLDER': dags_folder
    }

    # TODO: Add timeouts

    export_blocks_and_transactions = get_boolean_env_variable('EXPORT_BLOCKS_AND_TRANSACTIONS', True)
    export_receipts_and_logs = get_boolean_env_variable('EXPORT_RECEIPTS_AND_LOGS', True)
    export_contracts = get_boolean_env_variable('EXPORT_CONTRACTS', True)
    extract_erc20_transfers = get_boolean_env_variable('EXTRACT_ERC20_TRANSFERS', True)

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

    if extract_erc20_transfers:
        extract_erc20_transfers_operator = bash_operator.BashOperator(
            task_id='extract_erc20_transfers',
            bash_command=extract_erc20_transfers_command,
            dag=dag,
            env=environment)
        if export_receipts_and_logs:
            export_receipts_and_logs_operator >> extract_erc20_transfers_operator
