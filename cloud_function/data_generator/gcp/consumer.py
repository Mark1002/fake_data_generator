"""Consumer message."""
from typing import Callable
from google.cloud import pubsub_v1


class GooglePubSubConsumer:
    """Pubsub consumer."""

    def __init__(
        self, project_id: str, topic_id: str,
        subscription_id: str, func: Callable,
    ) -> None:
        """Init."""
        self.subscriber = pubsub_v1.SubscriberClient()
        self.topic_path = pubsub_v1.PublisherClient().topic_path(project_id, topic_id) # noqa
        self.subscription_path = self.subscriber.subscription_path(
            project_id, subscription_id
        )
        self.project_path = f"projects/{project_id}"
        self.func = func
        self._create_subscription()

    def _create_subscription(self):
        subscriptions = [subscription.name for subscription in self.subscriber.list_subscriptions( # noqa
            request={"project": self.project_path}
        )]
        if self.subscription_path in subscriptions:
            print(f'{self.subscription_path} is exist!')
            return
        subscription = self.subscriber.create_subscription(
            request={"name": self.subscription_path, "topic": self.topic_path}
        )
        print(f"Subscription created: {subscription}")

    def consume(self):
        """Consume data from pubsub."""
        pull_future = self.subscriber.subscribe(
            self.subscription_path, callback=self._callback)
        print(f"Listening for messages on {self.subscription_path}..\n")
        # Wrap subscriber in a 'with' block to automatically call close() when done. # noqa
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
