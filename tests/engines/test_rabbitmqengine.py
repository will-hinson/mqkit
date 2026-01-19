from mqkit.engines import RabbitMqEngine

import pytest
from yarl import URL


def test_rabbitmq_engine_required_attrs() -> None:
    for bad_url in [
        "amqp:///",  # missing host
        "amqp://:pass@localhost/",  # missing username
    ]:
        with pytest.raises(AttributeError):
            RabbitMqEngine.from_url(URL(bad_url))
