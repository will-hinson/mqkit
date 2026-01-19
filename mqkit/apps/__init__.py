"""
module mqkit.apps

A module providing application framework for message queue applications.
Includes the App class API intended for use by users.
"""

__all__ = ["App", "ConcurrencyMode"]

from .app import App
from .concurrencymode import ConcurrencyMode
