"""
module mqkit.workers.threaded.monotoniccounter

Defines the MonotonicCounter class for generating thread-safe sequential integer values.
"""

import itertools
from threading import Lock


class MonotonicCounter:
    """
    class MonotonicCounter

    A thread-safe monotonic counter that generates sequential integer values.
    Each call to the `next` method returns the next integer in the sequence,
    starting from the specified initial value.
    """

    # pylint: disable=too-few-public-methods

    _counter: itertools.count
    _lock: Lock

    def __init__(self: "MonotonicCounter", start: int = 0) -> None:
        self._counter = itertools.count(start)
        self._lock = Lock()

    def next(self: "MonotonicCounter") -> int:
        """
        Returns the next value in the monotonic counter sequence.

        Returns:
            int: The next value in the sequence.
        """

        with self._lock:
            return next(self._counter)
