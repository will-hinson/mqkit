from mqkit.messaging import Exchange, ExchangeType


def test_exchange_init_with_str() -> None:
    exchange = Exchange(
        name="test_exchange",
        type="direct",
        persistent=True,
    )

    assert exchange.name == "test_exchange"
    assert isinstance(exchange.type, ExchangeType)
    assert exchange.type == ExchangeType.DIRECT
    assert exchange.persistent is True
    assert exchange.auto_delete is False  # default value


def test_exchange_init_with_enum() -> None:
    exchange = Exchange(
        name="test_exchange",
        type=ExchangeType.FANOUT,
        persistent=False,
    )

    assert exchange.name == "test_exchange"
    assert exchange.type == ExchangeType.FANOUT
    assert exchange.persistent is False
    assert exchange.auto_delete is False  # default value
