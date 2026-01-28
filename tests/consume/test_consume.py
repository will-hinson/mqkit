import logging
import os
import uuid

import pytest
from unittest.mock import Mock, patch

from mqkit import consume
from mqkit.consume.decorator import _consume_threaded, _infer_engine, _infer_logger
from mqkit.endpoints.config import QueueEndpointConfig
from mqkit.marshal.codecs import CodecType


def test_consume_threaded_success(mocker) -> None:
    mock_worker = Mock()
    mock_worker.error = None

    mocker.patch(
        "mqkit.consume.decorator.ThreadWorker",
        return_value=mock_worker,
    )

    mocker.patch(
        "mqkit.consume.decorator.EndpointFactory.create_queue_endpoint",
        return_value=Mock(),
    )

    _consume_threaded(
        config=Mock(queue_name="test-queue"),
        engine=Mock(),
        logger=Mock(),
    )

    mock_worker.start.assert_called_once()
    mock_worker.join.assert_called_once()


def test_consume_threaded_reraises_worker_error(mocker) -> None:
    uuid_str: str = str(uuid.uuid4())
    err = RuntimeError(uuid_str)

    mock_worker = Mock()
    mock_worker.error = err

    mocker.patch(
        "mqkit.consume.decorator.ThreadWorker",
        return_value=mock_worker,
    )

    mocker.patch(
        "mqkit.consume.decorator.EndpointFactory.create_queue_endpoint",
        return_value=Mock(),
    )

    with pytest.raises(RuntimeError, match=uuid_str):
        _consume_threaded(
            config=Mock(queue_name="test-queue"),
            engine=Mock(),
            logger=Mock(),
        )


def test_infer_engine_env_not_set() -> None:
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(RuntimeError, match="MQKIT_ENGINE_URL"):
            _infer_engine()


def test_infer_engine_env_set(mocker) -> None:
    mock_create_engine = mocker.patch(
        "mqkit.consume.decorator.create_engine", return_value="engine"
    )
    with patch.dict(os.environ, {"MQKIT_ENGINE_URL": "amqp://test"}):
        result = _infer_engine()

    assert result == "engine"
    mock_create_engine.assert_called_once_with("amqp://test")


def test_infer_logger_default_name():
    def fake_func(): ...

    # no env var
    with patch.dict(os.environ, {}, clear=True):
        logger = _infer_logger(fake_func)
    assert isinstance(logger, logging.Logger)
    assert logger.name == "fake_func"


def test_infer_logger_env_name():
    def fake_func(): ...

    with patch.dict(os.environ, {"MQKIT_CONSUMER_LOGGER_NAME": "my_logger"}):
        logger = _infer_logger(fake_func)
    assert isinstance(logger, logging.Logger)
    assert logger.name == "my_logger"


def test_consume_decorator_async_disallowed() -> None:
    async def async_handler(msg):
        return msg

    with pytest.raises(NotImplementedError):
        consume(name="test-queue")(async_handler)


def test_consume_decorator_blocks_safely(mocker) -> None:
    # patch the blocking pieces BEFORE decorating
    mock_threaded = mocker.patch("mqkit.consume.decorator._consume_threaded")
    mock_exit = mocker.patch("sys.exit")
    mock_infer_engine = mocker.patch(
        "mqkit.consume.decorator._infer_engine", return_value="engine"
    )
    mock_infer_logger = mocker.patch(
        "mqkit.consume.decorator._infer_logger", return_value=Mock()
    )

    # dummy handler
    def handler(msg):
        return msg

    # decorate (this would normally block, but is safe due to mocks)
    decorated = consume(name="test-queue")(handler)

    # call the decorated function
    # decorated()

    # assertions
    mock_infer_engine.assert_called_once()
    mock_infer_logger.assert_called_once_with(handler)
    mock_threaded.assert_called_once()

    # inspect the QueueEndpointConfig passed
    config_arg = mock_threaded.call_args[1]["config"]
    assert isinstance(config_arg, QueueEndpointConfig)
    assert config_arg.queue.name == "test-queue"
    assert config_arg.target == handler
    assert config_arg.codec_type == CodecType.JSON

    # ensure sys.exit was called
    mock_exit.assert_called_once_with(0)
