"""
module mqkit.endpoints.config.queueendpointconfig

Defines the QueueEndpointConfig model for configuring queue endpoints.
"""

from typing import Callable, Optional, Union

from pydantic import BaseModel

from ...marshal.codecs import CodecType
from ...marshal import Queue


class QueueEndpointConfig(BaseModel):
    """
    class QueueEndpointConfig

    Model for configuring a queue endpoint.
    """

    queue_name: str
    target: Callable
    codec_type: CodecType
    forward_to: Optional[Queue] = None
    persistent: bool = True
    auto_delete: bool = False

    def __init__(
        self: "QueueEndpointConfig",
        codec_type: Union[CodecType, str],
        forward_to: Optional[Union[str, Queue]] = None,
        **data,
    ) -> None:  # pragma: no cover
        if isinstance(codec_type, str):
            data["codec_type"] = CodecType(codec_type)
        if forward_to is not None:
            if isinstance(forward_to, str):
                data["forward_to"] = Queue(name=forward_to)

        super().__init__(**data)
