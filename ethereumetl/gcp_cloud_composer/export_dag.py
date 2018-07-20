from __future__ import print_function

import datetime

from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator

default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2018, 7, 18),
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'ethereumetl_export_dag',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
    # command = 'wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && ' \
    #           'bash ~/miniconda.sh -b -p ~/miniconda >>miniconda_install.log 2>&1 && ' \
    #           'rm ~/miniconda.sh && ls ~/miniconda'
    # 'sudo apt-get -y update && sudo apt-get -y install --reinstall build-essential && ' \

    command = 'echo test > test.txt && which python && export CLOUDSDK_PYTHON=/usr/bin/python && gsutil cp test.txt gs://export-ethereumetl-io/test.txt && which python && python --version '

    # command = 'echo test > test.txt && gsutil cp test.txt gs://export-ethereumetl-io/test.txt && ' \
    #           '[[ ! -d ~/miniconda ]] && wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh >>miniconda_install.log 2>&1 && ' \
    #           '[[ ! -d ~/miniconda ]] && bash ~/miniconda.sh -b -p ~/miniconda >>miniconda_install.log 2>&1 && ' \
    #           'rm -f ~/miniconda.sh && ' \
    #           'git clone http://github.com/medvedev1088/ethereum-etl && cd ethereum-etl && ' \
    #           '~/miniconda/bin/python3 -m pip install -r requirements.txt && ' \
    #           'BLOCK_RANGE=$(~/miniconda/bin/python3 get_block_range_for_date.py -d $EXPORT_DATE -p https://mainnet.infura.io/) && ' \
    #           'BLOCK_RANGE_ARRAY=(${BLOCK_RANGE//,/ }) && ' \
    #           'echo $BLOCK_RANGE > blocks_meta.txt && ' \
    #           '~/miniconda/bin/python3 export_blocks_and_transactions.py -s ${BLOCK_RANGE_ARRAY[0]} -e ${BLOCK_RANGE_ARRAY[1]} ' \
    #           '-p https://mainnet.infura.io/ --blocks-output blocks.csv && ' \
    #           'gsutil cp blocks.csv gs://export-ethereumetl-io/blocks/block_date=$EXPORT_DATE/blocks.csv && ' \
    #           'gsutil cp blocks_meta.txt gs://export-ethereumetl-io/blocks/block_date=$EXPORT_DATE/blocks_meta.txt '

    bash_operator = bash_operator.BashOperator(
        task_id='export_blocks_and_transactions_bash_operator',
        bash_command=command,
        dag=dag,
        env={'EXECUTION_DATE': '{{ ds }}',
             'EXPORT_DATE': '{{ macros.ds_add(ds, -1) }}'})
# bash_command='sudo apt-get update && sudo apt-get -y install python3=3.5.1* python3-pip && python3 --version')
