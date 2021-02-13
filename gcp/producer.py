"""produce message."""
from google.cloud import pubsub_v1

class GooglePubSubProducer:
    """Pubsub producer."""
    
    def __init__(self, project_id: str, topic_id: str) -> None:
        """Init."""
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)
    
    def produce(self, message: bytes):
        """Produce."""
        future = self.publisher.publish(self.topic_path, message)
        print(future.result())
