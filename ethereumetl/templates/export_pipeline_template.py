from troposphere import Template, Parameter, Ref
from troposphere.datapipeline import Pipeline, PipelineTag, PipelineObject, ObjectField, ParameterObject, \
    ParameterObjectAttribute

from config import SETUP_COMMAND, EXPORT_BLOCKS_AND_TRANSACTIONS_COMMAND, EXPORT_RECEIPTS_AND_LOGS_COMMAND, \
    EXPORT_CONTRACTS_COMMAND, EXPORT_ERC20_TRANSFERS_COMMAND


def build_command_parameter_object(activity_name, description, default):
    return ParameterObject(Id='myCmd_{}'.format(activity_name), Attributes=[
        ParameterObjectAttribute(Key='type', StringValue='String'),
        ParameterObjectAttribute(Key='description', StringValue=description),
        ParameterObjectAttribute(Key='default', StringValue=default),
    ])


def build_s3_location(base_file_name, start, end):
    padded_start = str(start).rjust(8, '0')
    padded_end = str(end).rjust(8, '0')
    directory_path = \
        's3://#{myS3Bucket}/ethereumetl/export' + \
        '/{}/start_block={}/end_block={}'.format(
            base_file_name, padded_start, padded_end
        )
    return PipelineObject(
        Id='S3Location_{}_{}_{}'.format(base_file_name, start, end),
        Name='S3Location_{}_{}_{}'.format(base_file_name, start, end),
        Fields=[
            ObjectField(Key='type', StringValue='S3DataNode'),
            ObjectField(Key='directoryPath', StringValue=directory_path)

        ]
    )


def build_shell_command_activity(activity_name, start, end, outputs=None, inputs=None):
    outputs = outputs if outputs is not None else []
    inputs = inputs if inputs is not None else []
    command_variable_name = 'myCmd_{}'.format(activity_name)
    return PipelineObject(
        Id='ExportActivity_{}_{}_{}'.format(activity_name, start, end),
        Name='ExportActivity_{}_{}_{}'.format(activity_name, start, end),
        Fields=[
                   ObjectField(Key='type', StringValue='ShellCommandActivity'),
                   ObjectField(Key='command', StringValue='#{' + command_variable_name + '}'),
                   ObjectField(Key='scriptArgument', StringValue=str(start)),
                   ObjectField(Key='scriptArgument', StringValue=str(end)),
                   ObjectField(Key='workerGroup', StringValue='ethereum-etl'),
                   ObjectField(Key='maximumRetries', StringValue='5'),
                   ObjectField(Key='stage', StringValue='true')
               ] + [
                   ObjectField(Key='output', RefValue='S3Location_{}_{}_{}'.format(output, start, end))
                   for output in outputs
               ] + [
                   ObjectField(Key='input', RefValue='S3Location_{}_{}_{}'.format(inp, start, end))
                   for inp in inputs
               ]
    )


# https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-shellcommandactivity.html
def generate_export_pipeline_template(
        export_partitions, default_bucket, output,
        export_blocks_and_transactions=True,
        export_receipts_and_logs=False,
        export_erc20_transfers=False,
        export_contracts=False):
    """export_partitions is a list of tuples for start and end blocks"""
    template = Template()

    # CloudFormation version
    template.add_version('2010-09-09')

    template.add_description('Ethereum ETL Export CloudFormation Stack')

    # Parameters

    S3Bucket = template.add_parameter(Parameter(
        "S3Bucket",
        Description="S3 bucket where CSV files will be uploaded",
        Type="String",
        Default=default_bucket
    ))

    # Parameter Objects

    parameter_objects = [ParameterObject(Id='myS3Bucket', Attributes=[
        ParameterObjectAttribute(Key='type', StringValue='String'),
        ParameterObjectAttribute(Key='description', StringValue='S3 bucket'),
        ParameterObjectAttribute(Key='default', StringValue=Ref(S3Bucket)),
    ])]

    if export_blocks_and_transactions:
        parameter_objects.append(build_command_parameter_object(
            activity_name='blocks_and_transactions',
            description='Shell command for exporting blocks and transactions',
            default=' && '.join([SETUP_COMMAND, EXPORT_BLOCKS_AND_TRANSACTIONS_COMMAND])
        ))

    if export_receipts_and_logs:
        parameter_objects.append(build_command_parameter_object(
            activity_name='receipts_and_logs',
            description='Shell command for exporting receipts and logs',
            default=' && '.join([SETUP_COMMAND, EXPORT_RECEIPTS_AND_LOGS_COMMAND])
        ))

    if export_contracts:
        parameter_objects.append(build_command_parameter_object(
            activity_name='contracts',
            description='Shell command for exporting contracts',
            default=' && '.join([SETUP_COMMAND, EXPORT_CONTRACTS_COMMAND])
        ))

    if export_erc20_transfers:
        parameter_objects.append(build_command_parameter_object(
            activity_name='erc20_transfers',
            description='Shell command for exporting ERC20 transfers',
            default=' && '.join([SETUP_COMMAND, EXPORT_ERC20_TRANSFERS_COMMAND])
        ))

    # Pipeline Objects

    pipeline_objects = [PipelineObject(
        Id='Default',
        Name='Default',
        Fields=[
            ObjectField(Key='type', StringValue='Default'),
            ObjectField(Key='failureAndRerunMode', StringValue='cascade'),
            ObjectField(Key='scheduleType', StringValue='ondemand'),
            ObjectField(Key='role', StringValue='DataPipelineDefaultRole'),
            ObjectField(Key='pipelineLogUri', StringValue='s3://#{myS3Bucket}/data-pipeline-logs/'),

        ]
    )]

    for start, end in export_partitions:
        if export_blocks_and_transactions:
            pipeline_objects.append(build_shell_command_activity(
                'blocks_and_transactions', start, end, outputs=['blocks', 'transactions']))

        if export_blocks_and_transactions:
            pipeline_objects.append(build_s3_location('blocks', start, end))

        if export_blocks_and_transactions or export_receipts_and_logs:
            pipeline_objects.append(build_s3_location('transactions', start, end))

        if export_receipts_and_logs:
            pipeline_objects.append(build_shell_command_activity(
                'receipts_and_logs', start, end, inputs=['transactions'], outputs=['receipts', 'logs']))

        if export_receipts_and_logs:
            pipeline_objects.append(build_s3_location('logs', start, end))

        if export_receipts_and_logs or export_contracts:
            pipeline_objects.append(build_s3_location('receipts', start, end))

        if export_contracts:
            pipeline_objects.append(build_shell_command_activity(
                'contracts', start, end, inputs=['receipts'], outputs=['contracts']))
            pipeline_objects.append(build_s3_location('contracts', start, end))

        # ERC20 transfer pipe
        if export_erc20_transfers:
            pipeline_objects.append(build_shell_command_activity(
                'erc20_transfers', start, end, outputs=['erc20_transfers']))
            pipeline_objects.append(build_s3_location('erc20_transfers', start, end))

    template.add_resource(Pipeline(
        "EthereumETLPipeline",
        Name="EthereumETLPipeline",
        Description="Ethereum ETL Export Pipeline",
        PipelineTags=[PipelineTag(Key='Name', Value='ethereum-etl-pipeline')],
        ParameterObjects=parameter_objects,
        PipelineObjects=pipeline_objects
    ))

    # Write json template to file

    with open(output, 'w+') as output_file:
        output_file.write(template.to_json(indent=0, separators=(',', ":")).replace("\n", ""))
