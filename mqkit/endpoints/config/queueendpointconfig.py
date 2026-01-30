"""
module mqkit.endpoints.config.queueendpointconfig

Defines the QueueEndpointConfig model for configuring queue endpoints.
"""

from typing import Optional, Union

from pydantic import BaseModel

from ..endpoint import EndpointCallback
from ...marshal.codecs import CodecType
from ...messaging import Destination, Exchange, Queue


class QueueEndpointConfig(BaseModel):
    """
    class QueueEndpointConfig

    Model for configuring a queue endpoint.
    """

    queue: Queue
    target: EndpointCallback
    codec_type: CodecType
    forward_to: Optional[Destination] = None

    def __init__(
        self: "QueueEndpointConfig",
        codec_type: Union[CodecType, str],
        forward_to: Optional[Union[str, Queue, Exchange, Destination]] = None,
        **data,
    ) -> None:  # pragma: no cover
        if isinstance(codec_type, str):
            data["codec_type"] = CodecType(codec_type)
        if forward_to is not None:
            if isinstance(forward_to, str):
                data["forward_to"] = Queue(name=forward_to)
            else:
                data["forward_to"] = forward_to

            if isinstance(data["forward_to"], (Queue, Exchange)):
                data["forward_to"] = Destination(resource=data["forward_to"])

        super().__init__(**data)
