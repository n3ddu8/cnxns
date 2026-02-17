"""Core type definitions and protocols."""
from enum import Enum, auto
from typing import Any, Iterator, Mapping


class Capability(Enum):
    """Capabilities that backends may support."""
    
    STREAMING = auto()
    CHUNKING = auto()
    SCHEMA_SUPPORT = auto()
    BATCH_WRITE = auto()
    TRANSACTIONAL = auto()
    PREDICATE_PUSHDOWN = auto()
    PROJECTION = auto()


Row = Mapping[str, Any]
RowIterator = Iterator[Row]
