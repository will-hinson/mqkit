import inspect

from mqkit.engines import Engine

import pytest


def test_connection_is_abstract_base_class() -> None:
    for function in [
        "connect",
        "from_url",
    ]:
        assert hasattr(Engine, function)
        assert getattr(Engine, function).__isabstractmethod__ is True

        with pytest.raises(NotImplementedError):
            getattr(Engine, function)(
                *([None] * len(inspect.signature(getattr(Engine, function)).parameters))
            )
