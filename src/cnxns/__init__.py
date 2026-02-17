"""
Cnxns: A lightweight library for interacting with data systems.

Public API:
    - cnxn: Create a connection to a data system
    - read: Read data from a connection
    - write: Write data to a connection
    - register_backend: Register custom backends
"""
from .api import cnxn, read, write, register_backend

__version__ = "0.2.0"
__all__ = ["cnxn", "read", "write", "register_backend"]
