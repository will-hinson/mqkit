"""
module mqkit.endpoints.config.queueendpointconfig

Defines the QueueEndpointConfig model for configuring queue endpoints.
"""

from typing import Callable, Optional, Union

from pydantic import BaseModel

from ...marshal.codecs import CodecType


class QueueEndpointConfig(BaseModel):
    """
    class QueueEndpointConfig

    Model for configuring a queue endpoint.
    """

    queue_name: str
    target: Callable
    codec_type: CodecType
    forward_to: Optional[str] = None
    persistent: bool = True
    auto_delete: bool = False

    def __init__(
        self: "QueueEndpointConfig",
        *codec_type: Union[CodecType, str],
        **data,
    ) -> None:  # pragma: no cover
        if isinstance(codec_type, str):
            data["codec_type"] = CodecType(codec_type)

        super().__init__(**data)
