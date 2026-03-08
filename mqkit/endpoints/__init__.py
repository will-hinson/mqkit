"""
module mqkit.endpoints

Defines the Endpoint and QueueEndpoint classes for message queue endpoints.
"""

__all__ = ["config", "Endpoint", "EndpointFactory", "QueueEndpoint"]


from . import config

from .endpoint import Endpoint
from .endpointfactory import EndpointFactory
from .queueendpoint import QueueEndpoint
