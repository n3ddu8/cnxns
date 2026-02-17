"""Tests for public API."""
from unittest.mock import Mock, patch

import pytest

from cnxns.api import cnxn, read, write


def test_cnxn_mssql():
    """Test creating MSSQL connection."""
    with patch("cnxns.api.connect_mssql") as mock_connect:
        mock_connect.return_value = Mock()
        
        conn = cnxn("mssql://localhost/testdb", uid="sa", pwd="pass")
        
        mock_connect.assert_called_once()
        args = mock_connect.call_args
        assert args[1]["server"] == "localhost"
        assert args[1]["database"] == "testdb"
        assert args[1]["uid"] == "sa"
        assert args[1]["pwd"] == "pass"


def test_cnxn_mysql():
    """Test creating MySQL connection."""
    with patch("cnxns.api.connect_mysql") as mock_connect:
        mock_connect.return_value = Mock()
        
        conn = cnxn("mysql://host:3307/db", uid="user", pwd="pass")
        
        args = mock_connect.call_args
        assert args[1]["server"] == "host"
        assert args[1]["port"] == 3307
        assert args[1]["database"] == "db"


def test_cnxn_postgresql():
    """Test creating PostgreSQL connection."""
    with patch("cnxns.api.connect_postgresql") as mock_connect:
        mock_connect.return_value = Mock()
        
        conn = cnxn("postgresql://pg.local/mydb", uid="postgres", pwd="secret")
        
        args = mock_connect.call_args
        assert args[1]["host"] == "pg.local"
        assert args[1]["database"] == "mydb"
        assert args[1]["user"] == "postgres"


def test_cnxn_with_embedded_credentials():
    """Test connection with credentials in URL."""
    with patch("cnxns.api.connect_mssql") as mock_connect:
        mock_connect.return_value = Mock()
        
        conn = cnxn("mssql://user:pass@localhost/db")
        
        args = mock_connect.call_args
        assert args[1]["uid"] == "user"
        assert args[1]["pwd"] == "pass"


def test_cnxn_missing_credentials():
    """Test that missing credentials raise error."""
    with pytest.raises(ValueError, match="Username and password are required"):
        cnxn("mssql://localhost/db")


def test_cnxn_unsupported_scheme():
    """Test that unsupported scheme raises error."""
    with pytest.raises(ValueError, match="Unsupported scheme"):
        cnxn("oracle://localhost/db", uid="user", pwd="pass")


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
        
        assert result == mock_df


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
