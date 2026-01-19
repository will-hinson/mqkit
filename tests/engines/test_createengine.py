from mqkit import create_engine
from mqkit.engines.rabbitmq import RabbitMqEngine

import pytest


def test_create_engine_amqp() -> None:
    engine = create_engine("amqp://admin:123456@localhost/")
    assert isinstance(engine, RabbitMqEngine)


def test_create_engine_amqps() -> None:
    engine = create_engine("amqps://admin:123456@localhost/")
    assert isinstance(engine, RabbitMqEngine)


def test_create_engine_unknown() -> None:
    with pytest.raises(ValueError):
        create_engine("unknown://admin:123456@localhost/")
