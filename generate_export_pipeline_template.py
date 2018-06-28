import argparse

from config import EXPORT_PARTITIONS, DEFAULT_BUCKET, DEFAULT_COMMAND, ADD_TRANSACTIONS_INPUT
from ethereumetl.templates.export_pipeline_template import generate_export_pipeline_template

parser = argparse.ArgumentParser(description='Generate export pipeline template.')
parser.add_argument('--output', default='export_pipeline.template', type=str,
                    help='The output file for the template.')

args = parser.parse_args()

generate_export_pipeline_template(
    export_partitions=EXPORT_PARTITIONS,
    default_bucket=DEFAULT_BUCKET,
    default_command=DEFAULT_COMMAND,
    output=args.output,
    add_transactions_input=ADD_TRANSACTIONS_INPUT)
