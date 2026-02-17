"""Pandas dataframe adapter."""
from itertools import islice
from typing import Any, Iterator, Optional

try:
    import pandas as pd
except ImportError:
    pd = None

from ..core.types import Row, RowIterator


class PandasAdapter:
    """Adapter for Pandas DataFrames."""
    
    def from_rows(self, rows: RowIterator, chunk_size: Optional[int] = None) -> Any:
        """
        Convert row iterator to Pandas DataFrame.
        
        Args:
            rows: Iterator of row dictionaries
            chunk_size: If provided, yields DataFrames in chunks instead of single DF
            
        Returns:
            pandas.DataFrame if chunk_size is None, else Iterator[pandas.DataFrame]
        """
        if pd is None:
            raise ImportError(
                "pandas is required for Pandas support. "
                "Install with: pip install cnxns[pandas]"
            )
        
        if chunk_size is None:
            rows_list = list(rows)
            
            if not rows_list:
                return pd.DataFrame()
            
            return pd.DataFrame(rows_list)
        else:
            return self._chunked_dataframes(rows, chunk_size)
    
    def _chunked_dataframes(self, rows: RowIterator, chunk_size: int) -> Iterator[Any]:
        """Yield DataFrames in chunks to avoid full materialization."""
        while True:
            chunk = list(islice(rows, chunk_size))
            if not chunk:
                break
            yield pd.DataFrame(chunk)
    
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
