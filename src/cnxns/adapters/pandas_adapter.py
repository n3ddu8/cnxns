"""Pandas dataframe adapter."""
from typing import Any, Iterator

try:
    import pandas as pd
except ImportError:
    pd = None

from ..core.types import Row, RowIterator


class PandasAdapter:
    """Adapter for Pandas DataFrames."""
    
    def from_rows(self, rows: RowIterator) -> Any:
        """
        Convert row iterator to Pandas DataFrame.
        
        Args:
            rows: Iterator of row dictionaries
            
        Returns:
            pandas.DataFrame
        """
        if pd is None:
            raise ImportError(
                "pandas is required for Pandas support. "
                "Install with: pip install cnxns[pandas]"
            )
        
        rows_list = list(rows)
        
        if not rows_list:
            return pd.DataFrame()
        
        return pd.DataFrame(rows_list)
    
    def to_rows(self, data: Any) -> RowIterator:
        """
        Convert Pandas DataFrame to row iterator.
        
        Args:
            data: pandas.DataFrame
            
        Returns:
            Iterator of row dictionaries
        """
        if pd is None:
            raise ImportError(
                "pandas is required for Pandas support. "
                "Install with: pip install cnxns[pandas]"
            )
        
        if not isinstance(data, pd.DataFrame):
            raise TypeError(f"Expected pandas.DataFrame, got {type(data)}")
        
        for _, row in data.iterrows():
            yield row.to_dict()
