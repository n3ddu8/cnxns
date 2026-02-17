"""PySpark dataframe adapter."""
from typing import Any, Iterator, Optional

try:
    from pyspark.sql import SparkSession, Row as SparkRow
except ImportError:
    SparkSession = None
    SparkRow = None

from ..core.types import Row, RowIterator


class SparkAdapter:
    """Adapter for PySpark DataFrames."""
    
    def __init__(self, spark: Optional[Any] = None):
        """
        Initialize Spark adapter.
        
        Args:
            spark: SparkSession instance (optional, can be inferred)
        """
        if SparkSession is None:
            raise ImportError(
                "pyspark is required for Spark support. "
                "Install with: pip install cnxns[spark]"
            )
        
        self._spark = spark or SparkSession.builder.getOrCreate()
    
    def from_rows(self, rows: RowIterator) -> Any:
        """
        Convert row iterator to Spark DataFrame.
        
        Args:
            rows: Iterator of row dictionaries
            
        Returns:
            pyspark.sql.DataFrame
        """
        rows_list = list(rows)
        
        if not rows_list:
            return self._spark.createDataFrame([], schema=None)
        
        spark_rows = [SparkRow(**row) for row in rows_list]
        
        return self._spark.createDataFrame(spark_rows)
    
    def to_rows(self, data: Any) -> RowIterator:
        """
        Convert Spark DataFrame to row iterator.
        
        Args:
            data: pyspark.sql.DataFrame
            
        Returns:
            Iterator of row dictionaries
        """
        for row in data.collect():
            yield row.asDict()
