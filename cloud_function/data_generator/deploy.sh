#!/bin/bash
TOPIC_NAME=cron_topic
# create cloud function
gcloud functions deploy produce_message \
    --runtime python38 \
    --trigger-topic "${TOPIC_NAME}" \
    --timeout 540 \
    --set-env-vars PROJECT_ID="${PROJECT_ID}"
