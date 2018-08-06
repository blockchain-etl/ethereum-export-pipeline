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
        'performance_test_dag',
        # Daily at 1am
        schedule_interval='0 1 * * *',
        default_args=default_dag_args) as dag:

    test_command = 'cp /home/airflow/gcs/dags/resources/miniconda.tar . && ' \
                              'tar xvf miniconda.tar > untar_miniconda.log'
    test_operator = bash_operator.BashOperator(
        task_id='sleep',
        bash_command=test_command)
