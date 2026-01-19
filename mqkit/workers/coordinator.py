"""
module mqkit.workers.coordinator

Defines the Coordinator abstract base class for automatically managing
endpoints and engines.
"""

from abc import ABCMeta, abstractmethod
from typing import List

from ..endpoints import Endpoint
from ..engines import Engine


class Coordinator(metaclass=ABCMeta):
    """
    class Coordinator

    An abstract base class for coordinators that manage endpoints and engines.
    """

    # pylint: disable=too-few-public-methods

    _endpoints: List[Endpoint]
    _engine: Engine

    def __init__(
        self: "Coordinator", endpoints: List[Endpoint], engine: Engine
    ) -> None:  # pragma: no cover
        self._endpoints = endpoints
        self._engine = engine

    @abstractmethod
    def run(self: "Coordinator") -> None:
        """
        Abstract method to run the coordinator.
        """

        raise NotImplementedError()  # pragma: no cover
