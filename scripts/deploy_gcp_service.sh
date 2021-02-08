#!/bin/bash
echo 'deploy gcp service'

# create pubsub topic
TOPIC_NAME=fake_data_topic
gcloud pubsub topics create "${TOPIC_NAME}"
