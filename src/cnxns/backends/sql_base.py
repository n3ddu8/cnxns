"""Base implementation for SQL backends."""
from abc import ABC, abstractmethod
from typing import Any, Iterator, Optional, Set

from ..core.types import Capability, Row, RowIterator


class SQLBackend(ABC):
    """Base class for SQL database backends."""
    
    def __init__(self, connection: Any):
        """
        Initialize SQL backend.
        
        Args:
            connection: Database-specific connection object
        """
        self._connection = connection
        self._closed = False
    
    def capabilities(self) -> Set[Capability]:
        """Return standard SQL capabilities."""
        return {
            Capability.STREAMING,
            Capability.CHUNKING,
            Capability.SCHEMA_SUPPORT,
            Capability.BATCH_WRITE,
            Capability.TRANSACTIONAL,
            Capability.PREDICATE_PUSHDOWN,
            Capability.PROJECTION,
        }
    
    def read(
        self,
        query: Optional[str] = None,
        table: Optional[str] = None,
        schema: Optional[str] = None,
        columns: Optional[list[str]] = None,
        chunk_size: Optional[int] = None,
    ) -> RowIterator:
        """
        Read data from SQL database.
        
        Args:
            query: SQL query string
            table: Table name (alternative to query)
            schema: Schema name
            columns: List of columns to retrieve
            chunk_size: Number of rows per chunk
            
        Returns:
            Iterator of row dictionaries
        """
        if self._closed:
            raise RuntimeError("Backend connection is closed")
        
        if query is None:
            if table is None:
                raise ValueError("Must provide either 'query' or 'table'")
            query = self._build_select_query(table, schema, columns)
        
        return self._execute_read(query, chunk_size)
    
    def write(
        self,
        data: RowIterator,
        table: str,
        schema: Optional[str] = None,
        if_exists: str = "replace",
    ) -> None:
        """
        Write data to SQL database.
        
        Args:
            data: Iterator of row dictionaries
            table: Target table name
            schema: Schema name
            if_exists: 'replace', 'append', or 'fail'
        """
        if self._closed:
            raise RuntimeError("Backend connection is closed")
        
        if if_exists not in ("replace", "append", "fail"):
            raise ValueError(f"Invalid if_exists value: {if_exists}")
        
        self._execute_write(data, table, schema, if_exists)
    
    def close(self) -> None:
        """Close the database connection."""
        if not self._closed:
            self._close_connection()
            self._closed = True
    
    def _build_select_query(
        self,
        table: str,
        schema: Optional[str],
        columns: Optional[list[str]],
    ) -> str:
        """Build a SELECT query from table and columns."""
        col_list = ", ".join(columns) if columns else "*"
        
        if schema:
            full_table = f"{schema}.{table}"
        else:
            full_table = table
        
        return f"SELECT {col_list} FROM {full_table}"
    
    @abstractmethod
    def _execute_read(
        self,
        query: str,
        chunk_size: Optional[int],
    ) -> RowIterator:
        """
        Execute read query and return row iterator.
        
        Must be implemented by subclasses.
        """
        ...
    
    @abstractmethod
    def _execute_write(
        self,
        data: RowIterator,
        table: str,
        schema: Optional[str],
        if_exists: str,
    ) -> None:
        """
        Execute write operation.
        
        Must be implemented by subclasses.
        """
        ...
    
    @abstractmethod
    def _close_connection(self) -> None:
        """
        Close the underlying connection.
        
        Must be implemented by subclasses.
        """
        ...
