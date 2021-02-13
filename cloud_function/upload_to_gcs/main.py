"""Cloud function for upload_fake_data_to_gcs."""
import base64
import traceback
from google.cloud import storage


def upload_fake_data_to_gcs(event: dict, context):
    """Cloud function."""
    try:
        client = storage.Client()
        bucket_name = 'mark-etl'
        bucket = client.get_bucket(bucket_name)
        doc = base64.b64decode(event['data'])
        print(doc)
        blob_name = f'test-stream/{context.event_id}'
        blob = bucket.blob(blob_name)
        blob.upload_from_string(doc, content_type='avro/binary')
    except Exception:
        print(traceback.format_exc())
