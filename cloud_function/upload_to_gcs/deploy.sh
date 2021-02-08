#!/bin/bash
TOPIC_NAME=fake_data_topic
# create cloud function
gcloud functions deploy upload_fake_data_to_gcs \
    --runtime python38 \
    --trigger-topic "${TOPIC_NAME}" \
    --timeout 540
