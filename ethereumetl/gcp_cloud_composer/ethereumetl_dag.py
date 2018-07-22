from __future__ import print_function

import os
from datetime import datetime, timedelta

from airflow import models
from airflow.operators import bash_operator

# TODO start_date must be in UTC
default_dag_args = {
    'depends_on_past': False,
    'start_date': datetime(2015, 7, 31),
    'end_date': datetime(2016, 7, 1),
    'email': ['evge.medvedev@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'ethereumetl_export_dag6',
        # Daily at 1am
        schedule_interval='0 1 * * *',
        default_args=default_dag_args) as dag:
    setup_command = \
        'echo "WEB3_PROVIDER_URI: $WEB3_PROVIDER_URI" && echo "EXPORT_BUCKET: $EXPORT_BUCKET" && ' \
        'echo "EXPORT_DATE: $EXPORT_DATE" && echo "ETHEREUMETL_REPO_BRANCH: $ETHEREUMETL_REPO_BRANCH" && ' \
        'find ~ -maxdepth 1 -mmin +10 -type f -name ethereumetl_miniconda_install.lock -delete && ' \
        'if [ ! -e ~/ethereumetl_miniconda_install.lock ] && [ ! -e ~/miniconda/bin/python3 ]; then touch ~/ethereumetl_miniconda_install.lock && ' \
        'wget https://repo.continuum.io/miniconda/Miniconda3-4.5.4-Linux-x86_64.sh -O miniconda.sh >> miniconda_install.log && ' \
        'bash miniconda.sh -u -b -p ~/miniconda >> miniconda_install.log && ' \
        'rm -f ~/ethereumetl_miniconda_install.lock; else echo "Miniconda already installed"; fi && ' \
        'PYTHON3=~/miniconda/bin/python3 && ' \
        'git clone --branch $ETHEREUMETL_REPO_BRANCH http://github.com/medvedev1088/ethereum-etl && cd ethereum-etl && ' \
        'sudo $PYTHON3 -m pip install -r requirements.txt && ' \
        'BLOCK_RANGE=$($PYTHON3 get_block_range_for_date.py -d $EXPORT_DATE -p $WEB3_PROVIDER_URI) && ' \
        'BLOCK_RANGE_ARRAY=(${BLOCK_RANGE//,/ }) && START_BLOCK=${BLOCK_RANGE_ARRAY[0]} && END_BLOCK=${BLOCK_RANGE_ARRAY[1]} && ' \
        'export CLOUDSDK_PYTHON=/usr/local/bin/python'

    export_blocks_and_transactions_command = \
        setup_command + ' && ' + \
        'echo $BLOCK_RANGE > blocks_meta.txt && ' \
        '$PYTHON3 export_blocks_and_transactions.py -s $START_BLOCK -e $END_BLOCK ' \
        '-p $WEB3_PROVIDER_URI --blocks-output blocks.csv --transactions-output transactions.csv && ' \
        'gsutil cp blocks.csv gs://$EXPORT_BUCKET/blocks/block_date=$EXPORT_DATE/blocks.csv && ' \
        'gsutil cp transactions.csv gs://$EXPORT_BUCKET/transactions/block_date=$EXPORT_DATE/transactions.csv && ' \
        'gsutil cp blocks_meta.txt gs://$EXPORT_BUCKET/blocks_meta/block_date=$EXPORT_DATE/blocks_meta.txt '

    export_erc20_transfers_command = \
        setup_command + ' && ' + \
        '$PYTHON3 export_erc20_transfers.py -s $START_BLOCK -e $END_BLOCK ' \
        '-p $WEB3_PROVIDER_URI --output erc20_transfers.csv && ' \
        'gsutil cp erc20_transfers.csv gs://$EXPORT_BUCKET/erc20_transfers/block_date=$EXPORT_DATE/erc20_transfers.csv '

    output_bucket = os.environ.get('EXPORT_BUCKET')
    if output_bucket is None:
        raise ValueError('You must set EXPORT_BUCKET environment variable')
    web3_provider_uri = os.environ.get('WEB3_PROVIDER_URI', 'https://mainnet.infura.io/')
    ethereumetl_repo_branch = os.environ.get('ETHEREUMETL_REPO_BRANCH', 'master')

    environment = {
        'EXECUTION_DATE': '{{ ds }}',
        'EXPORT_DATE': '{{ macros.ds_add(ds, -1) }}',
        'ETHEREUMETL_REPO_BRANCH': ethereumetl_repo_branch,
        'WEB3_PROVIDER_URI': web3_provider_uri,
        'EXPORT_BUCKET': output_bucket
    }

    export_blocks_and_transactions_operator = bash_operator.BashOperator(
        task_id='export_blocks_and_transactions',
        bash_command=export_blocks_and_transactions_command,
        dag=dag,
        env=environment)

    export_erc20_transfers_operator = bash_operator.BashOperator(
        task_id='export_erc20_transfers',
        bash_command=export_erc20_transfers_command,
        dag=dag,
        env=environment)
