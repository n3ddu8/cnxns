"""Base implementation for SQL backends."""
import re
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
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
    
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
        
        if chunk_size is not None:
            if Capability.CHUNKING not in self.capabilities():
                raise NotImplementedError(
                    f"{self.__class__.__name__} does not support chunking"
                )
        
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
            raise ValueError(
                f"Invalid if_exists value: '{if_exists}'. "
                f"Must be 'replace', 'append', or 'fail'"
            )
        
        self._execute_write(data, table, schema, if_exists)
    
    def close(self) -> None:
        """Close the database connection."""
        if not self._closed:
            self._close_connection()
            self._closed = True
    
    def _validate_identifier(self, identifier: str, name: str = "identifier") -> None:
        """
        Validate SQL identifier to prevent injection.
        
        Args:
            identifier: The identifier to validate
            name: Name for error messages
            
        Raises:
            ValueError: If identifier is invalid
        """
        if not identifier:
            raise ValueError(f"{name} cannot be empty")
        
        # Allow alphanumeric, underscore, must start with letter or underscore
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
            raise ValueError(
                f"Invalid {name}: '{identifier}'. "
                f"Must start with letter/underscore and contain only "
                f"alphanumeric characters and underscores."
            )
        
        # Check for common SQL keywords that shouldn't be bare identifiers
        sql_keywords = {
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE',
            'ALTER', 'TABLE', 'FROM', 'WHERE', 'JOIN', 'UNION'
        }
        if identifier.upper() in sql_keywords:
            raise ValueError(
                f"Invalid {name}: '{identifier}' is a SQL keyword. "
                f"Use a different name or quote it explicitly."
            )
    
    def _build_select_query(
        self,
        table: str,
        schema: Optional[str],
        columns: Optional[list[str]],
    ) -> str:
        """Build a SELECT query from table and columns."""
        # Validate identifiers
        self._validate_identifier(table, "table name")
        if schema:
            self._validate_identifier(schema, "schema name")
        
        if columns:
            for col in columns:
                self._validate_identifier(col, "column name")
            col_list = ", ".join(self._quote_identifier(col) for col in columns)
        else:
            col_list = "*"
        
        if schema:
            full_table = f"{self._quote_identifier(schema)}.{self._quote_identifier(table)}"
        else:
            full_table = self._quote_identifier(table)
        
        return f"SELECT {col_list} FROM {full_table}"
    
    @abstractmethod
    def _quote_identifier(self, identifier: str) -> str:
        """
        Quote an identifier for safe use in SQL.
        
        Must be implemented by subclasses (different quote styles).
        """
        ...
    
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
