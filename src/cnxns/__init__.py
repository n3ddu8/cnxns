"""
Cnxns: A lightweight library for interacting with data systems.

Public API:
    - cnxn: Create a connection to a data system
    - read: Read data from a connection
    - write: Write data to a connection
"""
from .api import cnxn, read, write

__version__ = "0.1.0"
__all__ = ["cnxn", "read", "write"]
