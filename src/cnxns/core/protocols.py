"""Protocol definitions for backends and adapters."""
from typing import Any, Optional, Protocol, Set

from .types import Capability, RowIterator


class Backend(Protocol):
    """Protocol for data backend implementations."""
    
    def capabilities(self) -> Set[Capability]:
        """Return the set of capabilities this backend supports."""
        ...
    
    def read(
        self,
        query: Optional[str] = None,
        table: Optional[str] = None,
        schema: Optional[str] = None,
        columns: Optional[list[str]] = None,
        chunk_size: Optional[int] = None,
    ) -> RowIterator:
        """
        Read data from the backend.
        
        Args:
            query: SQL query or backend-specific query string
            table: Table name (alternative to query)
            schema: Schema name (if backend supports schemas)
            columns: List of columns to retrieve (if supported)
            chunk_size: Number of rows per chunk for streaming
            
        Returns:
            Iterator of row mappings
        """
        ...
    
    def write(
        self,
        data: RowIterator,
        table: str,
        schema: Optional[str] = None,
        if_exists: str = "replace",
    ) -> None:
        """
        Write data to the backend.
        
        Args:
            data: Iterator of row mappings
            table: Target table name
            schema: Schema name (if backend supports schemas)
            if_exists: Behavior if table exists ('replace', 'append', 'fail')
        """
        ...
    
    def close(self) -> None:
        """Close the backend connection."""
        ...


class Adapter(Protocol):
    """Protocol for dataframe adapters."""
    
    def from_rows(self, rows: RowIterator) -> Any:
        """Convert row iterator to framework-specific format."""
        ...
    
    def to_rows(self, data: Any) -> RowIterator:
        """Convert framework-specific format to row iterator."""
        ...
