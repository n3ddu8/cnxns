"""PostgreSQL backend implementation."""
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterator, Optional

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    psycopg2 = None
    PSYCOPG2_AVAILABLE = False

from ..core.types import Row, RowIterator
from .sql_base import SQLBackend


class PostgreSQLBackend(SQLBackend):
    """Backend for PostgreSQL."""
    
    def _quote_identifier(self, identifier: str) -> str:
        """Quote identifier using PostgreSQL style (double quotes)."""
        return f'"{identifier}"'
    
    def _execute_read(
        self,
        query: str,
        chunk_size: Optional[int],
    ) -> RowIterator:
        """Execute read query using psycopg2."""
        if not PSYCOPG2_AVAILABLE:
            raise ImportError("psycopg2 is required but not available")
        
        cursor = self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query)
        
        if chunk_size:
            yield from self._iter_chunks(cursor, chunk_size)
        else:
            yield from self._iter_all(cursor)
        
        cursor.close()
    
    def _iter_chunks(self, cursor: Any, chunk_size: int) -> RowIterator:
        """Iterate over result set in chunks."""
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            
            for row in rows:
                yield dict(row)
    
    def _iter_all(self, cursor: Any) -> RowIterator:
        """Iterate over entire result set."""
        for row in cursor:
            yield dict(row)
    
    def _execute_write(
        self,
        data: RowIterator,
        table: str,
        schema: Optional[str],
        if_exists: str,
    ) -> None:
        """Execute write operation using psycopg2."""
        # Validate identifiers
        self._validate_identifier(table, "table name")
        if schema:
            self._validate_identifier(schema, "schema name")
        
        full_table = f'{self._quote_identifier(schema)}.{self._quote_identifier(table)}' if schema else f'{self._quote_identifier(table)}'
        
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
                schema_cond = f"table_schema = %s" if schema else "table_schema = 'public'"
                params = [table]
                if schema:
                    params.append(schema)
                cursor.execute(
                    f"SELECT 1 FROM information_schema.tables "
                    f"WHERE table_name = %s AND {schema_cond}",
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
            col_defs.append(f'{self._quote_identifier(col)} {sql_type}')
        
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
                return "INTEGER"
            else:
                return "BIGINT"
        elif isinstance(value, float):
            return "DOUBLE PRECISION"
        elif isinstance(value, Decimal):
            return "NUMERIC(38, 10)"
        elif isinstance(value, str):
            if len(value) <= 255:
                return f"VARCHAR({max(len(value), 255)})"
            else:
                return "TEXT"
        elif isinstance(value, (bytes, bytearray)):
            return "BYTEA"
        elif isinstance(value, datetime):
            return "TIMESTAMP"
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
        placeholders = ", ".join(["%s"] * len(columns))
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
        """Close psycopg2 connection."""
        self._connection.close()


def connect_postgresql(
    host: str,
    user: str,
    password: str,
    database: Optional[str] = None,
    port: int = 5432,
    **kwargs: Any,
) -> PostgreSQLBackend:
    """
    Create PostgreSQL backend connection.
    
    Args:
        host: Server hostname or IP
        user: Username
        password: Password
        database: Database name
        port: Port number (default 5432)
        **kwargs: Additional psycopg2 connection parameters
        
    Returns:
        PostgreSQLBackend instance
    """
    if not PSYCOPG2_AVAILABLE:
        raise ImportError(
            "psycopg2 is required for PostgreSQL support. "
            "Install with: pip install cnxns[postgres]"
        )
    
    conn_params = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
    }
    
    if database:
        conn_params["database"] = database
    
    conn_params.update(kwargs)
    
    connection = psycopg2.connect(**conn_params)
    connection.autocommit = False
    
    return PostgreSQLBackend(connection)
