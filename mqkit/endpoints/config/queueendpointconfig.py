"""
module mqkit.endpoints.config.queueendpointconfig

Defines the QueueEndpointConfig model for configuring queue endpoints.
"""

from typing import ClassVar, Dict, Optional, Type, Union

from pydantic import BaseModel, ConfigDict, ValidationError

from ..endpoint import (
    EndpointCallback,
    EndpointDecodeException,
    EndpointExceptionHandler,
)
from ..endpointfactory import EndpointFactory
from ...errors import DecodeError
from ...marshal.codecs import CodecType
from ...messaging import Destination, ForwardTarget, Queue
from ...messaging.retry import RetryStrategy


class QueueEndpointConfig(BaseModel):
    """
    class QueueEndpointConfig

    Model for configuring a queue endpoint.
    """

    queue: Queue
    target: EndpointCallback
    codec_type: CodecType
    forward_to: Optional[Destination] = None
    dead_letter: Optional[Destination] = None
    retry_strategy: RetryStrategy
    error_handlers: Dict[Type[EndpointDecodeException], EndpointExceptionHandler] = {}

    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True,
    )

    def __init__(
        self: "QueueEndpointConfig",
        codec_type: Union[CodecType, str],
        forward_to: Optional[ForwardTarget] = None,
        dead_letter: Optional[ForwardTarget] = None,
        **data,
    ) -> None:  # pragma: no cover
        if isinstance(codec_type, str):
            data["codec_type"] = CodecType(codec_type)

        data["forward_to"] = EndpointFactory.convert_forward_target_to_destination(
            forward_to
        )
        data["dead_letter"] = EndpointFactory.convert_forward_target_to_destination(
            dead_letter
        )

        super().__init__(**data)

    @staticmethod
    def make_error_handlers_dict(
        on_decode_error: Optional[EndpointExceptionHandler] = None,
        on_validation_error: Optional[EndpointExceptionHandler] = None,
    ) -> Dict[Type[EndpointDecodeException], EndpointExceptionHandler]:
        error_handlers: Dict[
            Type[EndpointDecodeException], EndpointExceptionHandler
        ] = {}

        if on_decode_error is not None:
            error_handlers[DecodeError] = on_decode_error
        if on_validation_error is not None:
            error_handlers[ValidationError] = on_validation_error

        return error_handlers
