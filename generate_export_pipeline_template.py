import argparse

from config import EXPORT_PARTITIONS, DEFAULT_BUCKET, EXPORT_BLOCKS_AND_TRANSACTIONS, EXPORT_RECEIPTS_AND_LOGS, \
    EXPORT_CONTRACTS, EXPORT_ERC20_TRANSFERS
from ethereumetl.templates.export_pipeline_template import generate_export_pipeline_template

parser = argparse.ArgumentParser(description='Generate export pipeline template.')
parser.add_argument('--output', default='export_pipeline.template', type=str,
                    help='The output file for the template.')

args = parser.parse_args()

generate_export_pipeline_template(
    export_partitions=EXPORT_PARTITIONS, default_bucket=DEFAULT_BUCKET, output=args.output, minimize_output=True,
    export_blocks_and_transactions=EXPORT_BLOCKS_AND_TRANSACTIONS,
    export_receipts_and_logs=EXPORT_RECEIPTS_AND_LOGS,
    export_contracts=EXPORT_CONTRACTS,
    export_erc20_transfers=EXPORT_ERC20_TRANSFERS)
