from ethereumetl.utils import split_to_batches

# The below partitioning tries to make each partition of equal size.
# The first million blocks are in a single partition.
# The next 3 million blocks are in 100k partitions.
# The next 1 million blocks are in 10k partitions.
# Note that there is a limit in Data Pipeline on the number of objects, which can be
# increased in the Support Center
# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-limits.html
EXPORT_PARTITIONS = [(0, 999999)] + \
                    [(start, end) for start, end in split_to_batches(1000000, 1999999, 100000)] + \
                    [(start, end) for start, end in split_to_batches(2000000, 2999999, 100000)] + \
                    [(start, end) for start, end in split_to_batches(3000000, 3999999, 100000)] + \
                    [(start, end) for start, end in split_to_batches(4000000, 4999999, 10000)]

EXPORT_BLOCKS_AND_TRANSACTIONS = True
EXPORT_ERC20_TRANSFERS = False
EXPORT_RECEIPTS_AND_LOGS = False

SETUP_COMMAND = \
    "cd /home/ec2-user/ethereum-etl && IPC_PATH=/home/ec2-user/.ethereum/geth.ipc"

EXPORT_BLOCKS_AND_TRANSACTIONS_COMMAND = \
    "python3 export_blocks_and_transactions.py -s $1 -e $2 --ipc-path $IPC_PATH -w 1 " + \
    "--blocks-output ${OUTPUT1_STAGING_DIR}${3} --transactions-output ${OUTPUT1_STAGING_DIR}${4}"

EXPORT_ERC20_TRANSFERS_COMMAND = \
    "python3 export_erc20_transfers.py -s $1 -e $2 --ipc-path $IPC_PATH -w 1 " + \
    "--output ${OUTPUT1_STAGING_DIR}${5}"

if EXPORT_RECEIPTS_AND_LOGS and not EXPORT_BLOCKS_AND_TRANSACTIONS:
    EXPORT_RECEIPTS_AND_LOGS_COMMAND = \
        "PADDED_START=`printf \"%08d\" $1` && " + \
        "PADDED_END=`printf \"%08d\" $2` && " + \
        "python3 extract_csv_column.py -i ${INPUT1_STAGING_DIR}/transactions_${PADDED_START}_${PADDED_END}.csv -o ${OUTPUT1_STAGING_DIR}/tx_hashes.csv.temp -c tx_hash && " + \
        "python3 export_receipts_and_logs.py --tx-hashes ${OUTPUT1_STAGING_DIR}/tx_hashes.csv.temp --ipc-path $IPC_PATH -w 1 " + \
        "--receipts-output ${OUTPUT1_STAGING_DIR}${6} --logs-output ${OUTPUT1_STAGING_DIR}${7} && " + \
        "rm -f ${OUTPUT1_STAGING_DIR}/tx_hashes.csv.temp"
else:
    EXPORT_RECEIPTS_AND_LOGS_COMMAND = \
        "python3 extract_csv_column.py -i ${OUTPUT1_STAGING_DIR}${4} -o ${OUTPUT1_STAGING_DIR}/tx_hashes.csv.temp -c tx_hash && " + \
        "python3 export_receipts_and_logs.py --tx-hashes ${OUTPUT1_STAGING_DIR}/tx_hashes.csv.temp --ipc-path $IPC_PATH -w 1 " + \
        "--receipts-output ${OUTPUT1_STAGING_DIR}${6} --logs-output ${OUTPUT1_STAGING_DIR}${7} && " + \
        "rm -f ${OUTPUT1_STAGING_DIR}/tx_hashes.csv.temp"

COMMANDS = [SETUP_COMMAND] + \
           ([EXPORT_BLOCKS_AND_TRANSACTIONS_COMMAND] if EXPORT_BLOCKS_AND_TRANSACTIONS else []) + \
           ([EXPORT_ERC20_TRANSFERS_COMMAND] if EXPORT_ERC20_TRANSFERS else []) + \
           ([EXPORT_RECEIPTS_AND_LOGS_COMMAND] if EXPORT_RECEIPTS_AND_LOGS else [])

DEFAULT_COMMAND = ' && '.join(COMMANDS)

if EXPORT_RECEIPTS_AND_LOGS and not EXPORT_BLOCKS_AND_TRANSACTIONS:
    ADD_TRANSACTIONS_INPUT = True
else:
    ADD_TRANSACTIONS_INPUT = False

DEFAULT_BUCKET = "example.com"
