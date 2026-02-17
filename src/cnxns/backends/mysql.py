"""MySQL/MariaDB backend implementation."""
import urllib.parse
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator, Optional

try:
    import pyodbc
except ImportError:
    pyodbc = None

from ..core.types import Row, RowIterator
from .sql_base import SQLBackend


class MySQLBackend(SQLBackend):
    """Backend for MySQL and MariaDB."""
    
    def _quote_identifier(self, identifier: str) -> str:
        """Quote identifier using MySQL style (backticks)."""
        return f"`{identifier}`"
    
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
        # Validate identifiers
        self._validate_identifier(table, "table name")
        if schema:
            self._validate_identifier(schema, "schema name")
        
        full_table = f"{self._quote_identifier(schema)}.{self._quote_identifier(table)}" if schema else self._quote_identifier(table)
        
        data_iter = iter(data)
        first_row = next(data_iter, None)
        
        if first_row is None:
            return
        
        columns = list(first_row.keys())
        for col in columns:
            self._validate_identifier(col, "column name")
        
        cursor = self._connection.cursor()
        
        try:
            if if_exists == "replace":
                cursor.execute(f"DROP TABLE IF EXISTS {full_table}")
            elif if_exists == "fail":
                schema_filter = f"AND table_schema = ?" if schema else ""
                params = [table]
                if schema:
                    params.append(schema)
                cursor.execute(
                    f"SELECT 1 FROM information_schema.tables "
                    f"WHERE table_name = ? {schema_filter} LIMIT 1",
                    params
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
            col_defs.append(f"{self._quote_identifier(col)} {sql_type}")
        
        create_sql = f"CREATE TABLE {table} ({', '.join(col_defs)})"
        cursor.execute(create_sql)
    
    def _infer_sql_type(self, value: Any) -> str:
        """Infer SQL type from Python value."""
        if value is None:
            return "TEXT"
        # Check bool BEFORE int
        elif isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            if -2147483648 <= value <= 2147483647:
                return "INT"
            else:
                return "BIGINT"
        elif isinstance(value, float):
            return "DOUBLE"
        elif isinstance(value, Decimal):
            return "DECIMAL(38, 10)"
        elif isinstance(value, str):
            if len(value) <= 255:
                return f"VARCHAR({max(len(value), 255)})"
            else:
                return "TEXT"
        elif isinstance(value, (bytes, bytearray)):
            return "BLOB"
        elif isinstance(value, datetime):
            return "DATETIME"
        elif isinstance(value, date):
            return "DATE"
        else:
            return "TEXT"
    
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
        col_names = ", ".join([self._quote_identifier(col) for col in columns])
        
        insert_sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders})"
        
        batch = [[first_row.get(col) for col in columns]]
        
        for row in remaining:
            batch.append([row.get(col) for col in columns])
            
            if len(batch) >= 1000:
                cursor.executemany(insert_sql, batch)
                batch = []
        
        if batch:
            cursor.executemany(insert_sql, batch)
    
    def _close_connection(self) -> None:
        """Close pyodbc connection."""
        self._connection.close()


def connect_mysql(
    server: str,
    uid: str,
    pwd: str,
    database: Optional[str] = None,
    port: int = 3306,
    driver: str = "MySQL ODBC 9.4 Driver",
    ssl_verify: bool = True,
) -> MySQLBackend:
    """
    Create MySQL backend connection.
    
    Args:
        server: Server hostname or IP
        uid: Username
        pwd: Password
        database: Database name
        port: Port number (default 3306)
        driver: ODBC driver name
        ssl_verify: Verify SSL certificate
        
    Returns:
        MySQLBackend instance
    """
    if pyodbc is None:
        raise ImportError(
            "pyodbc is required for MySQL support. "
            "Install with: pip install cnxns[mysql]"
        )
    
    params = {
        "DRIVER": driver,
        "SERVER": server,
        "PORT": str(port),
        "UID": uid,
        "PWD": pwd,
    }
    
    if database:
        params["DATABASE"] = database
    
    if not ssl_verify:
        params["ssl_verify_cert"] = "0"
    
    conn_str = ";".join(f"{k}={v}" for k, v in params.items() if v) + ";"
    
    connection = pyodbc.connect(conn_str, autocommit=False)
    
    return MySQLBackend(connection)
