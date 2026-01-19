from mqkit.credentials import Credentials, PlainCredentials


def test_credentials() -> None:
    credentials: Credentials = PlainCredentials(username="user", password="pass")
    assert credentials.username == "user"
    assert credentials.password == "pass"

    # ensure that str and repr do not expose sensitive information
    assert str(credentials) == "PlainCredentials(...)"
    assert repr(credentials) == "PlainCredentials(...)"
