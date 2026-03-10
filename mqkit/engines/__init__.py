"""
module mqkit.engines

Defines the Engine base class and factory method to create specific engine instances
based on connection URLs.
"""

__all__ = ["create_engine", "Engine", "RabbitMqEngine"]

import os
from typing import Dict, Optional, Type

from yarl import URL

from .engine import Engine
from .rabbitmq import RabbitMqEngine

from ..errors import ConfigurationError


_scheme_mapping: Dict[str, Type[Engine]] = {
    "amqp": RabbitMqEngine,
    "amqps": RabbitMqEngine,
}


def create_engine(url: Optional[str] = None) -> Engine:
    """
    Factory method to create an Engine instance based on the provided URL.

    Args:
        url (str): The connection URL for the messaging engine.

    Returns:
        Engine: An instance of the appropriate Engine subclass.
    """

    # try to infer the engine if no URL is provided
    if url is None:
        if "MQKIT_ENGINE_URL" not in os.environ:
            raise ConfigurationError(
                "No engine URL provided and MQKIT_ENGINE_URL environment variable not set"
            )

        url = os.environ["MQKIT_ENGINE_URL"]

    connect_url: URL = URL(url)
    if connect_url.scheme not in _scheme_mapping:
        raise ValueError(
            f"Unknown URL scheme '{connect_url.scheme}' for engine creation"
        )

    return _scheme_mapping[connect_url.scheme].from_url(connect_url)
