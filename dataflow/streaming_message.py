"""Pratice04."""
import argparse
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


def run(argv=None, save_main_session=True):

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_topic',
        default="projects/tw-rd-de-mark-chang/topics/fake_data_topic",
        help="The Cloud Pub/Sub topic to read from."
        '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
    )
    parser.add_argument(
        '--input_subscription',
        default="projects/tw-rd-de-mark-chang/subscriptions/fake_social_media_sub",
        help='Input PubSub subscription of the form '
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    parser.add_argument(
        '--output',
        default='result/count.txt',
    )
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DirectRunner',
    ])
    pipeline_options = PipelineOptions(pipeline_args, streaming=True)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        # Read from PubSub into a PCollection.
        if known_args.input_subscription:
            messages = (
                p
                | beam.io.ReadFromPubSub(subscription=known_args.input_subscription).
                with_output_types(bytes)
            )
        else:
            messages = (
                p
                | beam.io.ReadFromPubSub(topic=known_args.input_topic).
                with_output_types(bytes)
            )
        messages = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
        messages | 'write to text' >> WriteToText(known_args.output)


if __name__ == '__main__':
    run()
