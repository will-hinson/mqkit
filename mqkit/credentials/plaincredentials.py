"""
module mqkit.credentials.plaincredentials

Defines the PlainCredentials class for storing plain text username and password.
"""

from .credentials import Credentials


class PlainCredentials(Credentials):
    """
    class PlainCredentials

    Stores plain text username and password for authentication.
    """

    username: str
    password: str
