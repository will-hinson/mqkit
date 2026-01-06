__all__ = ["create_engine", "Engine", "RabbitMQEngine"]

from typing import Dict, Type

from .engine import Engine
from .rabbitmqengine import RabbitMqEngine

from yarl import URL

_scheme_mapping: Dict[str, Type[Engine]] = {
    "amqp": RabbitMqEngine,
    "amqps": RabbitMqEngine,
}


def create_engine(url: str) -> Engine:
    """
    Factory method to create an Engine instance based on the provided URL.

    Args:
        url (str): The connection URL for the messaging engine.

    Returns:
        Engine: An instance of the appropriate Engine subclass.
    """

    connect_url: URL = URL(url)
    if connect_url.scheme not in _scheme_mapping:
        raise NotImplementedError(
            f"Engine creation for URL '{url}' is not yet implemented"
        )

    return _scheme_mapping[connect_url.scheme].from_url(connect_url)
