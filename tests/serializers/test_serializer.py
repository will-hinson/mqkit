import inspect

from mqkit.marshal import Serializer
from mqkit.marshal.codecs import JsonCodec

import pytest


def test_serializer_is_abstract_base_class() -> None:
    with pytest.raises(TypeError):
        Serializer(codec=JsonCodec())  # pyright: ignore[reportAbstractUsage]

    for function in [
        "serialize",
        "deserialize",
    ]:
        assert hasattr(Serializer, function)
        assert getattr(Serializer, function).__isabstractmethod__ is True

        with pytest.raises(NotImplementedError):
            getattr(Serializer, function)(
                *(
                    [None]
                    * len(inspect.signature(getattr(Serializer, function)).parameters)
                )
            )
