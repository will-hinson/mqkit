import itertools
from threading import Lock


class MonotonicCounter:
    _counter: itertools.count
    _lock: Lock

    def __init__(self: "MonotonicCounter", start: int = 0) -> None:
        self._counter = itertools.count(start)
        self._lock = Lock()

    def next(self: "MonotonicCounter") -> int:
        with self._lock:
            return next(self._counter)
