"""Execute fake data generator."""
import base64
import traceback
from pubsub import produce_fake_data_to_pubsub


def produce_message(event: dict, context):
    """trigger by cloud scheduler."""
    try:
        cron_message = base64.b64decode(event['data']).decode('utf-8')
        print(cron_message)
        produce_fake_data_to_pubsub(100)
    except Exception:
        print(traceback.format_exc())


if __name__ == "__main__":
    from dataclasses import dataclass

    @dataclass
    class ConText:
        event_id = 'event_id'

    produce_message(
        {'data': base64.b64encode('publish message!'.encode('utf8'))},
        ConText()
    )
