#!/bin/bash

gsutil mb gs://$BUCKET_NAME
gcloud pubsub topics create $TOPIC_ID
gcloud scheduler jobs create pubsub publisher-job --schedule="* * * * *" \
    --topic=$TOPIC_ID --message-body="Hello!"
gcloud scheduler jobs run publisher-job
