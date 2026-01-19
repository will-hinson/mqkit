import inspect

from mqkit.workers import Coordinator

import pytest


def test_codec_is_abstract_base_class() -> None:
    for function in [
        "run",
        "stop",
    ]:
        assert hasattr(Coordinator, function)
        assert getattr(Coordinator, function).__isabstractmethod__ is True

        with pytest.raises(NotImplementedError):
            getattr(Coordinator, function)(
                *(
                    [None]
                    * len(inspect.signature(getattr(Coordinator, function)).parameters)
                )
            )
