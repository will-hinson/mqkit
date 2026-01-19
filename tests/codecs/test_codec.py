import inspect

from mqkit.marshal.codecs import Codec

import pytest


def test_codec_is_abstract_base_class() -> None:
    for function in [
        "decode",
        "encode",
    ]:
        assert hasattr(Codec, function)
        assert getattr(Codec, function).__isabstractmethod__ is True

        with pytest.raises(NotImplementedError):
            getattr(Codec, function)(
                *([None] * len(inspect.signature(getattr(Codec, function)).parameters))
            )
