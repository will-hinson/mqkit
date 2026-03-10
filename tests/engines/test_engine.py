import inspect
import os

from mqkit import create_engine, Engine
from mqkit.engines import RabbitMqEngine
from mqkit.errors import ConfigurationError

import pytest
from unittest.mock import patch


def test_connection_is_abstract_base_class() -> None:
    for function in [
        "connect",
        "declare_resources",
        "from_url",
    ]:
        assert hasattr(Engine, function)
        assert getattr(Engine, function).__isabstractmethod__ is True

        with pytest.raises(NotImplementedError):
            getattr(Engine, function)(
                *([None] * len(inspect.signature(getattr(Engine, function)).parameters))
            )


def test_engine_infer_env_not_set() -> None:
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ConfigurationError, match="MQKIT_ENGINE_URL"):
            create_engine()


def test_engine_infer_env_set() -> None:
    with patch.dict(
        os.environ, {"MQKIT_ENGINE_URL": "amqp://user:password@test:1234/vhost"}
    ):
        result = create_engine()

    assert isinstance(result, RabbitMqEngine)
    assert result.host == "test"
    assert result.port == 1234
    assert result.vhost == "vhost"
    assert result.credentials.username == "user"
    assert result.credentials.password == "password"
