"""Tests for Pandas adapter."""
import pytest

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from cnxns.adapters.pandas_adapter import PandasAdapter


@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not installed")
def test_from_rows():
    """Test converting rows to DataFrame."""
    adapter = PandasAdapter()
    rows = iter([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ])
    
    df = adapter.from_rows(rows)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["id", "name"]
    assert df["id"].tolist() == [1, 2]


@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not installed")
def test_from_rows_empty():
    """Test converting empty iterator."""
    adapter = PandasAdapter()
    rows = iter([])
    
    df = adapter.from_rows(rows)
    
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0


@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not installed")
def test_to_rows():
    """Test converting DataFrame to rows."""
    adapter = PandasAdapter()
    df = pd.DataFrame([
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
    ])
    
    rows = list(adapter.to_rows(df))
    
    assert len(rows) == 2
    assert rows[0] == {"id": 1, "name": "Alice"}
    assert rows[1] == {"id": 2, "name": "Bob"}


@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="pandas not installed")
def test_to_rows_invalid_type():
    """Test that invalid type raises error."""
    adapter = PandasAdapter()
    
    with pytest.raises(TypeError, match="Expected pandas.DataFrame"):
        list(adapter.to_rows([{"id": 1}]))
