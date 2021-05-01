"""Pratice01."""
import re
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None, save_main_session=True):

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        default='source/kinglear.txt',
    )
    parser.add_argument(
        '--output',
        default='result/count.txt',
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DirectRunner',
    ])
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | '讀取文件' >> ReadFromText(known_args.input)

        counts = (
            lines
            | 'Split' >> (
                beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x)).with_output_types(str)
            )
            | beam.combiners.Count.PerElement()
            # | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
            # | 'GroupAndSum' >> beam.CombinePerKey(sum)
        )

        def format_result(word_count):
            word, count = word_count
            return f'{word}: {count}'
        output = counts | 'Format' >> beam.Map(format_result)
        output | 'write to text' >> WriteToText(known_args.output)


if __name__ == '__main__':
    run()
