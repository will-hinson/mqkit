import inspect

from mqkit.connections import Connection

import pytest


def test_connection_is_abstract_base_class() -> None:
    for function in [
        "acknowledge_failure",
        "acknowledge_success",
        "declare_resources",
        "__enter__",
        "__exit__",
        "forward_message",
        "get_message",
        "unblock",
    ]:
        assert hasattr(Connection, function)
        assert getattr(Connection, function).__isabstractmethod__ is True

        with pytest.raises(NotImplementedError):
            getattr(Connection, function)(
                *(
                    [None]
                    * len(inspect.signature(getattr(Connection, function)).parameters)
                )
            )
