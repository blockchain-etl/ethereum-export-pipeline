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
## @args: [database = "ethereumetl", table_name = "erc20_transfers", transformation_ctx = "data_source"]
## @return: data_source
## @inputs: []
data_source = glueContext.create_dynamic_frame.from_catalog(database="ethereumetl", table_name="erc20_transfers",
                                                            transformation_ctx="data_source")
## @type: ApplyMapping
## @args: [mapping = [("erc20_token", "string", "erc20_token", "string"), ("erc20_from", "string", "erc20_from", "string"), ("erc20_to", "string", "erc20_to", "string"), ("erc20_value", "long", "erc20_value", "long"), ("erc20_tx_hash", "string", "erc20_tx_hash", "string"), ("erc20_log_index", "long", "erc20_log_index", "long"), ("erc20_block_number", "long", "erc20_block_number", "long")], transformation_ctx = "mapped_frame"]
## @return: mapped_frame
## @inputs: [frame = data_source]
mapped_frame = ApplyMapping.apply(frame=data_source, mappings=[
    ("start_block", "string", "start_block", "string"),
    ("end_block", "string", "end_block", "string"),
    ("erc20_token", "string", "erc20_token", "string"),
    ("erc20_from", "string", "erc20_from", "string"),
    ("erc20_to", "string", "erc20_to", "string"),
    ("erc20_value", "string", "erc20_value", "decimal(38,0)"),
    ("erc20_tx_hash", "string", "erc20_tx_hash", "string"),
    ("erc20_log_index", "long", "erc20_log_index", "long"),
    ("erc20_block_number", "long", "erc20_block_number", "long")],
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
## @args: [connection_type = "s3", connection_options = {"path": "s3://<your_bucket>/ethereumetl/parquet/erc20_transfers"}, format = "parquet", transformation_ctx = "data_sink"]
## @return: data_sink
## @inputs: [frame = drop_null_fields_frame]
data_sink = glueContext.write_dynamic_frame.from_options(frame=drop_null_fields_frame, connection_type="s3",
                                                         connection_options={
                                                             "path": "s3://<your_bucket>/ethereumetl/parquet/erc20_transfers",
                                                             "partitionKeys": ["start_block", "end_block"]},
                                                         format="parquet", transformation_ctx="data_sink")
job.commit()
