from troposphere import Template, Parameter, Ref
from troposphere.datapipeline import Pipeline, PipelineTag, PipelineObject, ObjectField, ParameterObject, \
    ParameterObjectAttribute


def build_output_file_path(base_file_name, start_block, end_block):
    padded_start_block = str(start_block).rjust(8, '0')
    padded_end_block = str(end_block).rjust(8, '0')
    return '/{}/start_block={}/end_block={}/{}_{}_{}.csv'.format(
        base_file_name, padded_start_block, padded_end_block,
        base_file_name, padded_start_block, padded_end_block
    )


def generate_export_pipeline_template(export_partitions, default_bucket, default_command, output, add_input=False):
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

    # https://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-shellcommandactivity.html
    Command = template.add_parameter(Parameter(
        "Command",
        Description="Shell command that will be executed on workers",
        Type="String",
        Default=default_command
    ))

    template.add_resource(Pipeline(
        "EthereumETLPipeline",
        Name="EthereumETLPipeline",
        Description="Ethereum ETL Export Pipeline",
        PipelineTags=[PipelineTag(Key='Name', Value='ethereum-etl-pipeline')],
        ParameterObjects=[
            ParameterObject(Id='myS3Bucket', Attributes=[
                ParameterObjectAttribute(Key='type', StringValue='String'),
                ParameterObjectAttribute(Key='description', StringValue='S3 bucket'),
                ParameterObjectAttribute(Key='default', StringValue=Ref(S3Bucket)),
            ]),
            ParameterObject(Id='myShellCmd', Attributes=[
                ParameterObjectAttribute(Key='type', StringValue='String'),
                ParameterObjectAttribute(Key='description', StringValue='Shell command that will be run on workers'),
                ParameterObjectAttribute(Key='default', StringValue=Ref(Command)),
            ])
        ],
        PipelineObjects=
        [PipelineObject(
            Id='Default',
            Name='Default',
            Fields=[
                ObjectField(Key='type', StringValue='Default'),
                ObjectField(Key='failureAndRerunMode', StringValue='cascade'),
                ObjectField(Key='scheduleType', StringValue='ondemand'),
                ObjectField(Key='role', StringValue='DataPipelineDefaultRole'),
                ObjectField(Key='pipelineLogUri', StringValue='s3://#{myS3Bucket}/data-pipeline-logs/'),

            ]
        )] +
        [PipelineObject(
            Id='ExportActivity_{}_{}'.format(start, end),
            Name='ExportActivity_{}_{}'.format(start, end),
            Fields=[
                       ObjectField(Key='type', StringValue='ShellCommandActivity'),
                       ObjectField(Key='command', StringValue='#{myShellCmd}'),
                       ObjectField(Key='scriptArgument', StringValue=str(start)),
                       ObjectField(Key='scriptArgument', StringValue=str(end)),
                       ObjectField(Key='scriptArgument', StringValue=build_output_file_path('blocks', start, end)),
                       ObjectField(Key='scriptArgument',
                                   StringValue=build_output_file_path('transactions', start, end)),
                       ObjectField(Key='scriptArgument',
                                   StringValue=build_output_file_path('erc20_transfers', start, end)),
                       ObjectField(Key='scriptArgument',
                                   StringValue=build_output_file_path('receipts', start, end)),
                       ObjectField(Key='scriptArgument',
                                   StringValue=build_output_file_path('logs', start, end)),
                       ObjectField(Key='workerGroup', StringValue='ethereum-etl'),
                       ObjectField(Key='maximumRetries', StringValue='5'),
                       ObjectField(Key='output', RefValue='S3OutputLocation'),
                       ObjectField(Key='stage', StringValue='true')
                   ] + ([
                            ObjectField(Key='input', RefValue='S3InputLocation_{}_{}'.format(start, end))
                        ] if add_input else [])
        ) for start, end in export_partitions] + ([PipelineObject(
            Id='S3InputLocation_{}_{}'.format(start, end),
            Name='S3InputLocation_{}_{}'.format(start, end),
            Fields=[
                ObjectField(Key='type', StringValue='S3DataNode'),
                ObjectField(Key='directoryPath',
                            StringValue='s3://#{myS3Bucket}/ethereumetl/export/transactions/' +
                                        'start_block={}/end_block={}'.format(str(start).rjust(8, '0'), str(end).rjust(8, '0')))

            ]
        ) for start, end in export_partitions] if add_input else []) +
        [PipelineObject(
            Id='S3OutputLocation',
            Name='S3OutputLocation',
            Fields=[
                ObjectField(Key='type', StringValue='S3DataNode'),
                ObjectField(Key='directoryPath', StringValue='s3://#{myS3Bucket}/ethereumetl/export')

            ]
        )]
    ))

    # Write json template to file

    with open(output, 'w+') as output_file:
        output_file.write(template.to_json())
