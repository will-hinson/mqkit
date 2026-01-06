from enum import Enum


class ConcurrencyMode(str, Enum):
    THREAD = "thread"
    PROCESS = "process"
    ASYNC = "async"
