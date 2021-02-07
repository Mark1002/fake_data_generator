"""Show gcp data service etl processing."""
import json
import os
from google.cloud import pubsub_v1
from google.cloud import storage
from data_generator.data_schema import Message
from gcp.producer import GooglePubSubProducer
from gcp.utils.gcs_stream_upload import GCSObjectStreamUpload


def produce_fake_data_to_pubsub():
    """Fake data to pubsub."""
    producer = GooglePubSubProducer(
        project_id=os.getenv("PROJECT_ID"),
        topic_id="fake_data_topic"
    )
    for _ in range(10):
        message = Message().serialize()
        producer.produce(message)

def consume_data_from_pubsub():
    """Consume data from pubsub."""
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        os.getenv("PROJECT_ID"), "fake_data_topic-sub"
    )
    pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")
    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            pull_future.result()
        except KeyboardInterrupt:
            print('exit')
            pull_future.cancel()

def callback(message: pubsub_v1.subscriber.message.Message):
    """Callback for consumer."""
    data = message.data
    print(data)
    upload_fake_data_to_gcs(data)
    message.ack()

def upload_fake_data_to_gcs(data: bytes):
    client = storage.Client()
    md5_id = json.loads(data.decode())['md5_id']
    with GCSObjectStreamUpload(
        client=client, bucket_name='mark-etl', blob_name=f'test-stream/{md5_id}'
    ) as f:
        f.write(data)


def main():
    """Main."""
    produce_fake_data_to_pubsub()
    consume_data_from_pubsub()


if __name__ == "__main__":
    main()
