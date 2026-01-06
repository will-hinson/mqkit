from .credentials import Credentials


class PlainCredentials(Credentials):
    username: str
    password: str
