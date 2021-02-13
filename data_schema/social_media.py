"""Fake crawler data schema."""
import fastavro
import datetime
import json
import io
import random
from dataclasses import dataclass, field, asdict
from faker import Faker

faker = Faker(['en_US', 'zh_TW'])


@dataclass
class Message:
    """Message."""
    article_layer: int = field(default_factory=lambda: random.randint(0, 1))
    author: str = field(default_factory=faker.name)
    author_id: str = field(default_factory=faker.user_name)
    category: str = field(default_factory=faker.user_name)
    comment_count: int = field(default_factory=lambda: random.randint(0, 999))
    content: str = field(default_factory=lambda:  faker.text(max_nb_chars=2000))  # noqa
    created_time: datetime.datetime = field(default_factory=faker.date_time_this_year) # noqa
    dislike_count: int = field(default_factory=lambda: random.randint(0, 999))
    doc_id: str = field(default_factory=faker.md5)
    fetched_time: datetime.datetime = field(default_factory=faker.date_time_this_year) # noqa
    image_url: str = field(default_factory=faker.image_url)
    like_count: int = field(default_factory=lambda: random.randint(0, 999))
    link: str = field(default_factory=faker.uri)
    md5_id: str = field(default_factory=faker.md5)
    poster: str = field(default_factory=faker.name)
    share_count: int = field(default_factory=lambda: random.randint(0, 999))
    source: str = field(default_factory=lambda: random.choice(['NEWS', 'FORUM', 'BLOG'])) # noqa
    tag: str = field(default_factory=lambda: ','.join(faker.texts(nb_texts=4, max_nb_chars=5)).replace('.', '')) # noqa
    title: str = field(default_factory=faker.sentence)
    uid: str = field(default_factory=faker.user_name)
    view_count: int = field(default_factory=lambda: random.randint(0, 999))

    schema = fastavro.parse_schema({
        "type": "record",
        "name": "social_media",
        "namespace": "fakedata.generator",
        "fields": [
            {"name": "article_layer", "type": "int"},
            {"name": "author", "type": "string"},
            {"name": "author_id", "type": "string"},
            {"name": "category", "type": "string"},
            {"name": "comment_count", "type": ["int", "null"]},
            {"name": "content", "type": "string"},
            {"name": "created_time", "type": "long"},
            {"name": "dislike_count", "type": ["int", "null"]},
            {"name": "doc_id", "type": "string"},
            {"name": "fetched_time", "type": "long"},
            {"name": "image_url", "type": "string"},
            {"name": "like_count", "type": ["int", "null"]},
            {"name": "link", "type": "string"},
            {"name": "md5_id", "type": "string"},
            {"name": "share_count", "type": ["int", "null"]},
            {"name": "source", "type": "string"},
            {"name": "tag", "type": "string"},
            {"name": "title", "type": "string"},
            {"name": "uid", "type": "string"},
            {"name": "view_count", "type": ["int", "null"]},
        ]
    })

    def to_dict(self):
        """Convert to dict."""
        return asdict(self)

    def serialize(self) -> bytes:
        """Serialize data to avro."""
        data = self.to_dict()
        data['created_time'] = data['created_time'].timestamp()
        data['fetched_time'] = data['fetched_time'].timestamp()
        out = io.BytesIO()
        fastavro.writer(out, Message.schema, [data])
        return out.getvalue()

    def deserialize(self, avro: bytes) -> dict:
        """Deserialize data."""
        fo = io.BytesIO(avro)
        data = list(fastavro.reader(fo))[0]
        return data
