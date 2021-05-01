"""Pratice for debug."""
import re
import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class FilterTextFn(beam.DoFn):
    """A DoFn that filters for a specific key based on a regular expression."""

    def __init__(self, pattern):
        beam.DoFn.__init__(self)
        self.pattern = pattern
        self.matched_words = Metrics.counter(self.__class__, 'matched_words')
        self.umatched_words = Metrics.counter(self.__class__, 'umatched_words')

    def process(self, element):
        word, _ = element
        if re.match(self.pattern, word):
            # Log at INFO level each element we match. When executing this pipeline
            # using the Dataflow service, these log lines will appear in the Cloud
            # Logging UI.
            logging.info('Matched %s', word)
            self.matched_words.inc()
            yield element
        else:
            logging.debug('Did not match %s', word)
            self.umatched_words.inc()


class WordExtractingDoFn(beam.DoFn):

    def process(self, element):
        return re.findall(r'[\w\']+', element, re.UNICODE)


class FormatResultDoFn(beam.DoFn):

    def process(self, element):
        word, count = element
        yield f'{word}: {count}'


class CountWords(beam.PTransform):
    def expand(self, lines):
        counts = (
            lines
            | 'Split' >> (
                beam.ParDo(WordExtractingDoFn()).with_output_types(str)
            )
            | beam.combiners.Count.PerElement()
        )
        return counts


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

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:
        lines = p | '讀取文件' >> ReadFromText(known_args.input)
        counts = lines | 'count words' >> CountWords()
        filtered_words = counts | 'filter text' >> beam.ParDo(FilterTextFn('Flourish|stomach'))
        assert_that(filtered_words, equal_to([('Flourish', 3), ('stomach', 1)]))
        output = filtered_words | 'Format' >> beam.ParDo(FormatResultDoFn())
        output | 'write to text' >> WriteToText(known_args.output)


if __name__ == '__main__':
    """To execute this pipeline using the Google Cloud Dataflow service, specify
    pipeline configuration::
    python wordcount_debug.py \
        --input=gs://dataflow-samples/shakespeare/kinglear.txt \
        --project=${PROJECT_ID} \
        --staging_location=gs://${BUCKET_NAME}/staging \
        --temp_location=gs://${BUCKET_NAME}/temp \
        --region=${REGION} \
        --job_name=debugwordcount01 \
        --runner=DataflowRunner \
        --output=gs://${BUCKET_NAME}/dataflow_result
    """
    logging.getLogger().setLevel(logging.INFO)
    run()
