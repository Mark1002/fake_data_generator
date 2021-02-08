"""Cloud function for upload_fake_data_to_gcs."""
import base64
import json
import traceback
from google.cloud import storage


def upload_fake_data_to_gcs(event: dict, context):
    """Cloud function."""
    try:
        client = storage.Client()
        bucket_name = 'mark-etl'
        bucket = client.get_bucket(bucket_name)
        doc = base64.b64decode(event['data']).decode('utf-8')
        print(doc)
        md5_id = json.loads(doc)['md5_id']
        blob_name = f'test-stream/{md5_id}'
        blob = bucket.blob(blob_name)
        blob.upload_from_string(doc, content_type='application/json')
    except Exception:
        print(traceback.format_exc())
