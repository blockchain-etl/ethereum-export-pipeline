import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "ethereumetl", table_name = "blocks", transformation_ctx = "data_source"]
## @return: data_source
## @inputs: []
data_source = glueContext.create_dynamic_frame.from_catalog(database="ethereumetl", table_name="blocks",
                                                            transformation_ctx="data_source")
## @type: ApplyMapping
## @args: [mapping = [("number", "long", "number", "long"), ("hash", "string", "hash", "string"), ("parent_hash", "string", "parent_hash", "string"), ("nonce", "string", "nonce", "string"), ("sha3_uncles", "string", "sha3_uncles", "string"), ("logs_bloom", "string", "logs_bloom", "string"), ("transactions_root", "string", "transactions_root", "string"), ("state_root", "string", "state_root", "string"), ("miner", "string", "miner", "string"), ("difficulty", "long", "difficulty", "long"), ("total_difficulty", "long", "total_difficulty", "long"), ("size", "long", "size", "long"), ("extra_data", "string", "extra_data", "string"), ("gas_limit", "long", "gas_limit", "long"), ("gas_used", "long", "gas_used", "long"), ("timestamp", "long", "timestamp", "long"), ("transaction_count", "long", "transaction_count", "long")], transformation_ctx = "applymapping1"]
## @return: mapped_frame
## @inputs: [frame = data_source]
mapped_frame = ApplyMapping.apply(frame=data_source, mappings=[
    ("start_block", "string", "start_block", "string"),
    ("end_block", "string", "end_block", "string"),
    ("number", "long", "number", "long"),
    ("hash", "string", "hash", "string"),
    ("parent_hash", "string", "parent_hash", "string"),
    ("nonce", "string", "nonce", "string"),
    ("sha3_uncles", "string", "sha3_uncles", "string"),
    ("logs_bloom", "string", "logs_bloom", "string"),
    ("transactions_root", "string", "transactions_root", "string"),
    ("state_root", "string", "state_root", "string"),
    ("miner", "string", "miner", "string"),
    ("difficulty", "string", "difficulty", "decimal(38,0)"),
    ("total_difficulty", "string", "total_difficulty", "decimal(38,0)"),
    ("size", "long", "size", "long"),
    ("extra_data", "string", "extra_data", "string"),
    ("gas_limit", "long", "gas_limit", "long"),
    ("gas_used", "long", "gas_used", "long"),
    ("timestamp", "long", "timestamp", "long"),
    ("transaction_count", "long", "transaction_count", "long")],
                                  transformation_ctx="mapped_frame")

## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolve_choice_frame"]
## @return: resolve_choice_frame
## @inputs: [frame = mapped_frame]
resolve_choice_frame = ResolveChoice.apply(frame=mapped_frame, choice="make_struct",
                                           transformation_ctx="resolve_choice_frame")
## @type: DropNullFields
## @args: [transformation_ctx = "drop_null_fields_frame"]
## @return: drop_null_fields_frame
## @inputs: [frame = resolve_choice_frame]
drop_null_fields_frame = DropNullFields.apply(frame=resolve_choice_frame, transformation_ctx="drop_null_fields_frame")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://<your_bucket>/ethereumetl/parquet/blocks"}, format = "parquet", transformation_ctx = "data_sink"]
## @return: data_sink
## @inputs: [frame = drop_null_fields_frame]
data_sink = glueContext.write_dynamic_frame.from_options(frame=drop_null_fields_frame, connection_type="s3",
                                                         connection_options={
                                                             "path": "s3://<your_bucket>/ethereumetl/parquet/blocks",
                                                             "partitionKeys": ["start_block", "end_block"]},
                                                         format="parquet", transformation_ctx="data_sink")
job.commit()
