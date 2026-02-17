"""Public API for cnxns library."""
from typing import Any, Optional, Union
from urllib.parse import urlparse

from .adapters.pandas_adapter import PandasAdapter
from .adapters.spark_adapter import SparkAdapter
from .backends.mssql import connect_mssql
from .backends.mysql import connect_mysql
from .backends.postgresql import connect_postgresql
from .core.protocols import Backend
from .core.types import RowIterator


def cnxn(
    url: str,
    uid: Optional[str] = None,
    pwd: Optional[str] = None,
    **kwargs: Any,
) -> Backend:
    """
    Create a connection to a data system.
    
    Args:
        url: Connection URL (e.g., 'mssql://localhost/mydb', 'mysql://host:3306/db')
        uid: Username (can also be in URL)
        pwd: Password (can also be in URL)
        **kwargs: Additional backend-specific options
        
    Returns:
        Backend connection object
        
    Examples:
        >>> conn = cnxn('mssql://localhost/dev', uid='sa', pwd='password')
        >>> conn = cnxn('mysql://localhost:3306/mydb', uid='user', pwd='pass')
        >>> conn = cnxn('postgresql://localhost/db', uid='postgres', pwd='secret')
    """
    parsed = urlparse(url)
    
    scheme = parsed.scheme.lower()
    host = parsed.hostname or "localhost"
    port = parsed.port
    database = parsed.path.lstrip("/") or None
    
    url_user = parsed.username
    url_pass = parsed.password
    
    username = uid or url_user
    password = pwd or url_pass
    
    if not username or not password:
        raise ValueError("Username and password are required")
    
    if scheme == "mssql":
        return connect_mssql(
            server=host,
            uid=username,
            pwd=password,
            database=database,
            port=port or 1433,
            **kwargs,
        )
    
    elif scheme == "mysql":
        return connect_mysql(
            server=host,
            uid=username,
            pwd=password,
            database=database,
            port=port or 3306,
            **kwargs,
        )
    
    elif scheme in ("postgresql", "postgres"):
        return connect_postgresql(
            host=host,
            user=username,
            password=password,
            database=database,
            port=port or 5432,
            **kwargs,
        )
    
    else:
        raise ValueError(
            f"Unsupported scheme: {scheme}. "
            f"Supported: mssql, mysql, postgresql"
        )


def read(
    connection: Backend,
    query: Optional[str] = None,
    table: Optional[str] = None,
    schema: Optional[str] = None,
    columns: Optional[list[str]] = None,
    chunk_size: Optional[int] = None,
    format: Optional[str] = None,
    **kwargs: Any,
) -> Union[RowIterator, Any]:
    """
    Read data from a connection.
    
    Args:
        connection: Backend connection from cnxn()
        query: SQL query or query string
        table: Table name (alternative to query)
        schema: Schema name (for databases with schema support)
        columns: List of columns to retrieve
        chunk_size: Number of rows per chunk for streaming
        format: Output format ('pandas', 'spark', or None for raw rows)
        **kwargs: Additional format-specific options (e.g., spark_session)
        
    Returns:
        Iterator of row dicts (if format=None), or framework-specific dataframe
        
    Examples:
        >>> # Raw row iteration
        >>> for row in read(conn, query="SELECT * FROM users"):
        ...     print(row)
        
        >>> # Pandas DataFrame
        >>> df = read(conn, table="users", format="pandas")
        
        >>> # PySpark DataFrame
        >>> spark_df = read(conn, query="SELECT * FROM users", 
        ...                 format="spark", spark_session=spark)
        
        >>> # Streaming/chunked reads
        >>> for chunk in read(conn, table="large_table", chunk_size=1000):
        ...     process(chunk)
    """
    rows = connection.read(
        query=query,
        table=table,
        schema=schema,
        columns=columns,
        chunk_size=chunk_size,
    )
    
    if format is None:
        return rows
    
    elif format == "pandas":
        adapter = PandasAdapter()
        return adapter.from_rows(rows)
    
    elif format == "spark":
        spark_session = kwargs.get("spark_session")
        adapter = SparkAdapter(spark=spark_session)
        return adapter.from_rows(rows)
    
    else:
        raise ValueError(
            f"Unsupported format: {format}. "
            f"Supported: pandas, spark, or None"
        )


def write(
    connection: Backend,
    data: Union[RowIterator, Any],
    table: str,
    schema: Optional[str] = None,
    if_exists: str = "replace",
    format: Optional[str] = None,
) -> None:
    """
    Write data to a connection.
    
    Args:
        connection: Backend connection from cnxn()
        data: Data to write (row iterator, DataFrame, etc.)
        table: Target table name
        schema: Schema name (for databases with schema support)
        if_exists: Behavior if table exists ('replace', 'append', 'fail')
        format: Format of input data ('pandas', 'spark', or None for raw rows)
        
    Examples:
        >>> # Write from row iterator
        >>> rows = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        >>> write(conn, rows, table="users")
        
        >>> # Write from Pandas DataFrame
        >>> write(conn, df, table="users", format="pandas")
        
        >>> # Write from PySpark DataFrame
        >>> write(conn, spark_df, table="users", schema="staging", format="spark")
    """
    if format == "pandas":
        adapter = PandasAdapter()
        rows = adapter.to_rows(data)
    
    elif format == "spark":
        adapter = SparkAdapter()
        rows = adapter.to_rows(data)
    
    elif format is None:
        rows = data
    
    else:
        raise ValueError(
            f"Unsupported format: {format}. "
            f"Supported: pandas, spark, or None"
        )
    
    connection.write(
        data=rows,
        table=table,
        schema=schema,
        if_exists=if_exists,
    )
