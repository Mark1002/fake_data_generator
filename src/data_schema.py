"""Data generator."""
import datetime
import json
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

    def to_dict(self):
        """Convert to dict."""
        return asdict(self)

    def serialize(self):
        """Serialize data."""
        return json.dumps(
            self.to_dict(), default=lambda x: x.isoformat() if isinstance(x, datetime.datetime) else None # noqa
        )

    def deserialize(json_data):
        """Deserialize data."""
        pass