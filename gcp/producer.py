"""produce message."""
from google.cloud import pubsub_v1

class GooglePubSubProducer:
    """Pubsub producer."""
    
    def __init__(self, project_id: str, topic_id: str) -> None:
        """Init."""
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)
    
    def produce(self, message: str):
        """Produce."""
        data = message.encode("utf-8")
        future = self.publisher.publish(self.topic_path, data)
        print(future.result())
