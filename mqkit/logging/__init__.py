"""
module mqkit.logging

Defines logging configuration for the mqkit package.
"""

import os

root_logger_name: str = os.environ.get(
    "MQKIT_ROOT_LOGGER_NAME",
    "mqkit",
)
