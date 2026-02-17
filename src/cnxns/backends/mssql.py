"""Microsoft SQL Server backend implementation."""
import urllib.parse
from typing import Any, Iterator, Optional

try:
    import pyodbc
except ImportError:
    pyodbc = None

from ..core.types import Row, RowIterator
from .sql_base import SQLBackend


class MSSQLBackend(SQLBackend):
    """Backend for Microsoft SQL Server."""
    
    def _execute_read(
        self,
        query: str,
        chunk_size: Optional[int],
    ) -> RowIterator:
        """Execute read query using pyodbc."""
        cursor = self._connection.cursor()
        cursor.execute(query)
        
        if chunk_size:
            yield from self._iter_chunks(cursor, chunk_size)
        else:
            yield from self._iter_all(cursor)
        
        cursor.close()
    
    def _iter_chunks(self, cursor: Any, chunk_size: int) -> RowIterator:
        """Iterate over result set in chunks."""
        columns = [desc[0] for desc in cursor.description]
        
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            
            for row in rows:
                yield dict(zip(columns, row))
    
    def _iter_all(self, cursor: Any) -> RowIterator:
        """Iterate over entire result set."""
        columns = [desc[0] for desc in cursor.description]
        
        for row in cursor:
            yield dict(zip(columns, row))
    
    def _execute_write(
        self,
        data: RowIterator,
        table: str,
        schema: Optional[str],
        if_exists: str,
    ) -> None:
        """Execute write operation using pyodbc."""
        full_table = f"{schema}.{table}" if schema else table
        
        data_iter = iter(data)
        first_row = next(data_iter, None)
        
        if first_row is None:
            return
        
        columns = list(first_row.keys())
        
        cursor = self._connection.cursor()
        
        try:
            if if_exists == "replace":
                cursor.execute(f"DROP TABLE IF EXISTS {full_table}")
            elif if_exists == "fail":
                cursor.execute(
                    f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
                    f"WHERE TABLE_NAME = '{table}'"
                )
                if cursor.fetchone():
                    raise ValueError(f"Table {full_table} already exists")
            
            if if_exists in ("replace", "fail"):
                self._create_table(cursor, full_table, first_row)
            
            self._insert_rows(cursor, full_table, columns, first_row, data_iter)
            
            self._connection.commit()
        
        except Exception:
            self._connection.rollback()
            raise
        
        finally:
            cursor.close()
    
    def _create_table(self, cursor: Any, table: str, sample_row: Row) -> None:
        """Create table based on sample row."""
        col_defs = []
        for col, val in sample_row.items():
            sql_type = self._infer_sql_type(val)
            col_defs.append(f"[{col}] {sql_type}")
        
        create_sql = f"CREATE TABLE {table} ({', '.join(col_defs)})"
        cursor.execute(create_sql)
    
    def _infer_sql_type(self, value: Any) -> str:
        """Infer SQL type from Python value."""
        if value is None:
            return "NVARCHAR(MAX)"
        elif isinstance(value, bool):
            return "BIT"
        elif isinstance(value, int):
            return "BIGINT"
        elif isinstance(value, float):
            return "FLOAT"
        elif isinstance(value, str):
            return "NVARCHAR(MAX)"
        elif isinstance(value, (bytes, bytearray)):
            return "VARBINARY(MAX)"
        else:
            return "NVARCHAR(MAX)"
    
    def _insert_rows(
        self,
        cursor: Any,
        table: str,
        columns: list[str],
        first_row: Row,
        remaining: Iterator[Row],
    ) -> None:
        """Insert rows using parameterized query."""
        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join([f"[{col}]" for col in columns])
        
        insert_sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
        
        batch = [[first_row[col] for col in columns]]
        
        for row in remaining:
            batch.append([row[col] for col in columns])
            
            if len(batch) >= 1000:
                cursor.executemany(insert_sql, batch)
                batch = []
        
        if batch:
            cursor.executemany(insert_sql, batch)
    
    def _close_connection(self) -> None:
        """Close pyodbc connection."""
        self._connection.close()


def connect_mssql(
    server: str,
    uid: str,
    pwd: str,
    database: Optional[str] = None,
    port: int = 1433,
    driver: str = "ODBC Driver 18 for SQL Server",
    trust_cert: bool = False,
) -> MSSQLBackend:
    """
    Create MSSQL backend connection.
    
    Args:
        server: Server hostname or IP
        uid: Username
        pwd: Password
        database: Database name
        port: Port number (default 1433)
        driver: ODBC driver name
        trust_cert: Trust server certificate
        
    Returns:
        MSSQLBackend instance
    """
    if pyodbc is None:
        raise ImportError(
            "pyodbc is required for MSSQL support. "
            "Install with: pip install cnxns[mssql]"
        )
    
    params = {
        "DRIVER": driver,
        "SERVER": f"{server},{port}",
        "UID": uid,
        "PWD": pwd,
        "MARS_Connection": "Yes",
        "TrustServerCertificate": "Yes" if trust_cert else "No",
    }
    
    if database:
        params["DATABASE"] = database
    
    conn_str = ";".join(f"{k}={v}" for k, v in params.items()) + ";"
    
    connection = pyodbc.connect(conn_str, autocommit=False)
    
    return MSSQLBackend(connection)
