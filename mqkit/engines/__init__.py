"""
module mqkit.engines

Defines the Engine base class and factory method to create specific engine instances
based on connection URLs.
"""

__all__ = ["create_engine", "Engine", "RabbitMqEngine"]

from typing import Dict, Type

from yarl import URL

from .engine import Engine
from .rabbitmq import RabbitMqEngine


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
