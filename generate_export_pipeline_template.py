import argparse

from ethereumetl.templates.export_pipeline_template import generate_export_pipeline_template

parser = argparse.ArgumentParser(description='Generate export pipeline template.')
parser.add_argument('--output', default='export_pipeline.template', type=str,
                    help='The output file for the template.')

args = parser.parse_args()

generate_export_pipeline_template(args.output)
