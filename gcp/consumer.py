"""Consumer message."""
import os
from typing import Callable 
from google.cloud import pubsub_v1

class GooglePubSubConsumer:
    """Pubsub consumer."""

    def __init__(self, func: Callable) -> None:
        """Init."""
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(
            os.getenv("PROJECT_ID"), "fake_data_topic-sub"
        )
        self.func = func

    def consume(self):
        """Consume data from pubsub."""
        pull_future = self.subscriber.subscribe(
            self.subscription_path, callback=self._callback)
        print(f"Listening for messages on {self.subscription_path}..\n")
        # Wrap subscriber in a 'with' block to automatically call close() when done.
        with self.subscriber:
            try:
                # When `timeout` is not set, result() will block indefinitely,
                # unless an exception is encountered first.
                pull_future.result()
            except KeyboardInterrupt:
                print('exit')
                pull_future.cancel()

    def _callback(self, message: pubsub_v1.subscriber.message.Message):
        """Callback for consumer."""
        data = message.data
        print(data)
        self.func(data)
        message.ack()
