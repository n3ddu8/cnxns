"""Tests for SQL base backend."""
from typing import Iterator, Optional
from unittest.mock import Mock

import pytest

from cnxns.backends.sql_base import SQLBackend
from cnxns.core.types import Capability, RowIterator


class MockSQLBackend(SQLBackend):
    """Mock SQL backend for testing."""
    
    def __init__(self):
        super().__init__(Mock())
        self.read_calls = []
        self.write_calls = []
        self.close_calls = []
    
    def _execute_read(self, query: str, chunk_size: Optional[int]) -> RowIterator:
        self.read_calls.append((query, chunk_size))
        yield {"id": 1, "name": "test"}
    
    def _execute_write(
        self,
        data: RowIterator,
        table: str,
        schema: Optional[str],
        if_exists: str,
    ) -> None:
        self.write_calls.append((list(data), table, schema, if_exists))
    
    def _close_connection(self) -> None:
        self.close_calls.append(True)


def test_capabilities():
    """Test that SQL backend advertises expected capabilities."""
    backend = MockSQLBackend()
    caps = backend.capabilities()
    
    assert Capability.STREAMING in caps
    assert Capability.CHUNKING in caps
    assert Capability.SCHEMA_SUPPORT in caps
    assert Capability.BATCH_WRITE in caps


def test_read_with_query():
    """Test reading with explicit query."""
    backend = MockSQLBackend()
    
    rows = list(backend.read(query="SELECT * FROM test"))
    
    assert len(rows) == 1
    assert rows[0]["id"] == 1
    assert len(backend.read_calls) == 1
    assert backend.read_calls[0][0] == "SELECT * FROM test"


def test_read_with_table():
    """Test reading with table name."""
    backend = MockSQLBackend()
    
    list(backend.read(table="users"))
    
    assert len(backend.read_calls) == 1
    assert "FROM users" in backend.read_calls[0][0]


def test_read_with_schema():
    """Test reading with schema and table."""
    backend = MockSQLBackend()
    
    list(backend.read(table="users", schema="public"))
    
    assert "FROM public.users" in backend.read_calls[0][0]


def test_read_with_columns():
    """Test reading with column selection."""
    backend = MockSQLBackend()
    
    list(backend.read(table="users", columns=["id", "name"]))
    
    assert "SELECT id, name" in backend.read_calls[0][0]


def test_read_requires_query_or_table():
    """Test that read requires either query or table."""
    backend = MockSQLBackend()
    
    with pytest.raises(ValueError, match="Must provide either"):
        list(backend.read())


def test_write_basic():
    """Test basic write operation."""
    backend = MockSQLBackend()
    data = [{"id": 1, "name": "test"}]
    
    backend.write(iter(data), table="users")
    
    assert len(backend.write_calls) == 1
    assert backend.write_calls[0][1] == "users"
    assert backend.write_calls[0][3] == "replace"


def test_write_with_schema():
    """Test write with schema."""
    backend = MockSQLBackend()
    data = [{"id": 1}]
    
    backend.write(iter(data), table="users", schema="public")
    
    assert backend.write_calls[0][2] == "public"


def test_write_if_exists_validation():
    """Test that invalid if_exists values are rejected."""
    backend = MockSQLBackend()
    data = [{"id": 1}]
    
    with pytest.raises(ValueError, match="Invalid if_exists"):
        backend.write(iter(data), table="users", if_exists="invalid")


def test_close():
    """Test closing the backend."""
    backend = MockSQLBackend()
    
    backend.close()
    
    assert len(backend.close_calls) == 1


def test_operations_after_close():
    """Test that operations fail after closing."""
    backend = MockSQLBackend()
    backend.close()
    
    with pytest.raises(RuntimeError, match="closed"):
        list(backend.read(query="SELECT 1"))
    
    with pytest.raises(RuntimeError, match="closed"):
        backend.write(iter([{"id": 1}]), table="test")
