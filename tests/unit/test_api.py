"""Tests for public API."""
from unittest.mock import Mock, patch, MagicMock

import pytest

from cnxns.api import cnxn, read, write, register_backend


def test_cnxn_mssql():
    """Test creating MSSQL connection."""
    with patch("cnxns.backends.mssql.pyodbc") as mock_pyodbc:
        mock_connection = Mock()
        mock_pyodbc.connect.return_value = mock_connection
        
        conn = cnxn("mssql://localhost/testdb", uid="sa", pwd="pass")
        
        assert mock_pyodbc.connect.called
        # Verify it returns a backend
        assert hasattr(conn, 'read')
        assert hasattr(conn, 'write')


def test_cnxn_mysql():
    """Test creating MySQL connection."""
    with patch("cnxns.backends.mysql.pyodbc") as mock_pyodbc:
        mock_connection = Mock()
        mock_pyodbc.connect.return_value = mock_connection
        
        conn = cnxn("mysql://host:3307/db", uid="user", pwd="pass")
        
        assert mock_pyodbc.connect.called


def test_cnxn_postgresql():
    """Test creating PostgreSQL connection."""
    with patch("cnxns.backends.postgresql.PSYCOPG2_AVAILABLE", True):
        with patch("cnxns.backends.postgresql.psycopg2") as mock_pg:
            mock_connection = Mock()
            mock_pg.connect.return_value = mock_connection
            mock_pg.extras = Mock()
            
            conn = cnxn("postgresql://pg.local/mydb", uid="postgres", pwd="secret")
            
            assert mock_pg.connect.called


def test_cnxn_with_embedded_credentials():
    """Test connection with credentials in URL."""
    with patch("cnxns.backends.mssql.pyodbc") as mock_pyodbc:
        mock_connection = Mock()
        mock_pyodbc.connect.return_value = mock_connection
        
        conn = cnxn("mssql://user:pass@localhost/db")
        
        assert mock_pyodbc.connect.called


def test_cnxn_missing_credentials():
    """Test that missing credentials raise error."""
    with pytest.raises(ValueError, match="Username and password are required"):
        cnxn("mssql://localhost/db")


def test_cnxn_unsupported_scheme():
    """Test that unsupported scheme raises error."""
    with pytest.raises(ValueError, match="Unsupported scheme"):
        cnxn("oracle://localhost/db", uid="user", pwd="pass")


def test_register_backend():
    """Test registering a custom backend."""
    def custom_factory(**kwargs):
        return Mock()
    
    register_backend("custom", custom_factory)
    
    from cnxns.api import BACKENDS
    assert "custom" in BACKENDS


def test_cnxn_context_manager():
    """Test connection as context manager."""
    with patch("cnxns.backends.mssql.pyodbc") as mock_pyodbc:
        mock_connection = Mock()
        mock_pyodbc.connect.return_value = mock_connection
        
        with cnxn("mssql://sa:pass@localhost/db") as conn:
            assert conn is not None
        
        # Verify close was called
        assert mock_connection.close.called


def test_read_raw_rows():
    """Test reading raw rows."""
    mock_backend = Mock()
    mock_backend.read.return_value = iter([{"id": 1}])
    
    rows = list(read(mock_backend, query="SELECT * FROM test"))
    
    assert rows == [{"id": 1}]
    mock_backend.read.assert_called_once()


def test_read_pandas():
    """Test reading with Pandas format."""
    with patch("cnxns.adapters.pandas_adapter.pd") as mock_pd:
        mock_df = Mock()
        mock_pd.DataFrame.return_value = mock_df
        
        mock_backend = Mock()
        mock_backend.read.return_value = iter([{"id": 1}])
        
        result = read(mock_backend, query="SELECT 1", format="pandas")
        
        assert mock_pd.DataFrame.called


def test_read_unsupported_format():
    """Test that unsupported format raises error."""
    mock_backend = Mock()
    
    with pytest.raises(ValueError, match="Unsupported format"):
        read(mock_backend, query="SELECT 1", format="polars")


def test_write_raw_rows():
    """Test writing raw rows."""
    mock_backend = Mock()
    data = iter([{"id": 1}])
    
    write(mock_backend, data, table="test")
    
    mock_backend.write.assert_called_once()
    args = mock_backend.write.call_args
    assert args[1]["table"] == "test"


def test_write_with_schema():
    """Test writing with schema."""
    mock_backend = Mock()
    data = iter([{"id": 1}])
    
    write(mock_backend, data, table="test", schema="public")
    
    args = mock_backend.write.call_args
    assert args[1]["schema"] == "public"


def test_write_auto_detect_pandas():
    """Test auto-detection of pandas format."""
    with patch("cnxns.adapters.pandas_adapter.pd") as mock_pd:
        mock_df = MagicMock()
        mock_df.__module__ = "pandas.core.frame"
        mock_df.__class__.__name__ = "DataFrame"
        mock_df.iterrows.return_value = iter([])
        
        mock_backend = Mock()
        
        write(mock_backend, mock_df, table="test")
        
        # Should auto-detect and convert
        mock_backend.write.assert_called_once()
