"""Execute fake data generator."""
import json
import os
import sys
from google.cloud import storage
from data_schema.social_media import Message
from gcp.producer import GooglePubSubProducer
from gcp.consumer import GooglePubSubConsumer
from gcp.utils.gcs_stream_upload import GCSObjectStreamUpload


def produce_fake_data_to_pubsub(message_num: int):
    """Fake data to pubsub."""
    producer = GooglePubSubProducer(
        project_id=os.getenv("PROJECT_ID"),
        topic_id="fake_data_topic"
    )
    for _ in range(message_num):
        message = Message().serialize()
        producer.produce(message)

def consume_data_from_pubsub():
    """Consume data from pubsub."""
    def upload_fake_data_to_gcs(data: bytes):
        doc = Message().deserialize(data)
        client = storage.Client()
        md5_id = doc['md5_id']
        with GCSObjectStreamUpload(
            client=client, bucket_name='mark-etl',
            blob_name=f'test-stream/{md5_id}',
            content_type='avro/binary',
        ) as f:
            f.write(data)
    
    consumer = GooglePubSubConsumer(
        project_id=os.getenv('PROJECT_ID'),
        topic_id='fake_data_topic',
        subscription_id='fake_data_topic_sub',
        func=upload_fake_data_to_gcs
    )
    consumer.consume()

def main():
    """Main."""
    produce_fake_data_to_pubsub(int(sys.argv[1]))
    consume_data_from_pubsub()


if __name__ == "__main__":
    main()
