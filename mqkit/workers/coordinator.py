from abc import ABCMeta
from typing import List

from ..endpoints import Endpoint
from ..engines import Engine


class Coordinator(metaclass=ABCMeta):
    _endpoints: List[Endpoint]
    _engine: Engine

    def __init__(
        self: "Coordinator", endpoints: List[Endpoint], engine: Engine
    ) -> None:
        self._endpoints = endpoints
        self._engine = engine
