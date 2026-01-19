"""
module mqkit.workers.worker

Contains the definition of the Worker abstract base class.
"""

from abc import ABCMeta


# pylint: disable=too-few-public-methods
class Worker(metaclass=ABCMeta):
    """
    class Worker

    Abstract base class for all worker implementations.
    """
