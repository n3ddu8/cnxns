# Migration Guide: v0.0.x → v0.1.0

## Overview

Version 0.1.0 is a **complete rewrite** of the cnxns library with breaking changes. The new design follows a framework-neutral architecture with a minimal public API.

## Breaking Changes Summary

1. **API redesigned** — `dbms_cnxn`, `dbms_reader`, `dbms_writer` replaced with `cnxn`, `read`, `write`
2. **Dependencies removed** — SQLAlchemy and Pandas are no longer required dependencies
3. **Connection format changed** — URLs instead of parameter dictionaries
4. **M365/Dynamics support removed** — Focus on SQL databases only
5. **Return types changed** — Core returns row iterators, not DataFrames

## Migration Examples

### Connecting to Databases

**Before (v0.0.x):**
```python
from cnxns import dbms

engine = dbms.dbms_cnxn(
    dbms="mssql",
    server="localhost",
    uid="sa",
    pwd="password",
    database="mydb"
)
```

**After (v0.1.0):**
```python
from cnxns import cnxn

conn = cnxn(
    'mssql://localhost/mydb',
    uid='sa',
    pwd='password'
)

# Or with credentials in URL
conn = cnxn('mssql://sa:password@localhost/mydb')
```

### Reading Data

**Before (v0.0.x):**
```python
from cnxns import dbms

# Read entire table into DataFrame
df = dbms.dbms_reader(
    engine,
    table_name="users",
    schema="dbo"
)

# Read with query
df = dbms.dbms_reader(
    engine,
    query="SELECT * FROM users WHERE active = 1"
)

# Read in chunks
for chunk in dbms.dbms_read_chunks(engine, table_name="users", chunksize=1000):
    process(chunk)  # chunk is a DataFrame
```

**After (v0.1.0):**
```python
from cnxns import cnxn, read

# Read as Pandas DataFrame (Pandas must be installed)
df = read(
    conn,
    table="users",
    schema="dbo",
    format="pandas"
)

# Read with query
df = read(
    conn,
    query="SELECT * FROM users WHERE active = 1",
    format="pandas"
)

# Read in chunks (returns iterator of row dicts)
for chunk in read(conn, table="users", chunk_size=1000):
    for row in chunk:
        process(row)  # row is a dict

# Or chunk into DataFrames (must materialize each chunk)
for i, chunk_rows in enumerate(read(conn, table="users", chunk_size=1000)):
    import pandas as pd
    chunk_df = pd.DataFrame(list(chunk_rows))
    process(chunk_df)
```

### Writing Data

**Before (v0.0.x):**
```python
from cnxns import dbms
import pandas as pd

df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})

dbms.dbms_writer(
    engine,
    df,
    "users",
    schema="dbo",
    if_exists="replace"
)
```

**After (v0.1.0):**
```python
from cnxns import cnxn, write
import pandas as pd

df = pd.DataFrame({'id': [1, 2], 'name': ['Alice', 'Bob']})

write(
    conn,
    df,
    table="users",
    schema="dbo",
    format="pandas",
    if_exists="replace"
)
```

### Using Different Databases

**Before (v0.0.x):**
```python
# MSSQL
engine = dbms.dbms_cnxn(dbms="mssql", server="localhost", uid="sa", pwd="pass")

# MySQL
engine = dbms.dbms_cnxn(dbms="mysql", server="localhost", uid="root", pwd="pass")

# PostgreSQL - NOT SUPPORTED
```

**After (v0.1.0):**
```python
# MSSQL
conn = cnxn('mssql://localhost/db', uid='sa', pwd='pass')

# MySQL
conn = cnxn('mysql://localhost/db', uid='root', pwd='pass')

# PostgreSQL - NOW SUPPORTED
conn = cnxn('postgresql://localhost/db', uid='postgres', pwd='pass')
```

## New Capabilities in v0.1.0

### Framework-Neutral Core

The library now works **without** Pandas or any dataframe library:

```python
from cnxns import cnxn, read, write

conn = cnxn('mssql://localhost/db', uid='user', pwd='pass')

# Read as dictionaries (no Pandas needed)
for row in read(conn, query="SELECT * FROM users"):
    print(f"User: {row['name']}")  # {'id': 1, 'name': 'Alice'}

# Write from dictionaries (no Pandas needed)
data = [
    {'id': 1, 'name': 'Alice'},
    {'id': 2, 'name': 'Bob'}
]
write(conn, data, table="users")
```

### PySpark Support

```python
from cnxns import cnxn, read, write
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
conn = cnxn('mssql://localhost/db', uid='user', pwd='pass')

# Read into Spark DataFrame
spark_df = read(
    conn,
    table="events",
    format="spark",
    spark_session=spark
)

# Write from Spark DataFrame
write(conn, spark_df, table="events_backup", format="spark")
```

### PostgreSQL Support

```python
conn = cnxn('postgresql://localhost/warehouse', uid='postgres', pwd='secret')

df = read(conn, table="customers", schema="public", format="pandas")
```

## Installation Changes

**Before (v0.0.x):**
```bash
pip install cnxns
# Always installed: pyodbc, sqlalchemy, pandas, msal
```

**After (v0.1.0):**
```bash
# Core library (no dependencies)
pip install cnxns

# With specific backends
pip install cnxns[mssql]      # adds pyodbc
pip install cnxns[mysql]      # adds pyodbc  
pip install cnxns[postgres]   # adds psycopg2

# With specific frameworks
pip install cnxns[pandas]     # adds pandas
pip install cnxns[spark]      # adds pyspark

# Everything
pip install cnxns[all]
```

## Removed Features

### Dynamics 365 / M365 Support

The `m365` module has been removed entirely. If you need this functionality, continue using v0.0.x or implement a custom backend.

**Before (v0.0.x):**
```python
from cnxns.api import m365

result = m365.query_api(
    client_id="...",
    client_secret="...",
    tenant_id="...",
    base_url="...",
    api_url="...",
    query="/accounts",
    chunksize=1000
)
```

**After (v0.1.0):**
Not supported. Use v0.0.x for Dynamics/M365.

## Step-by-Step Migration Checklist

1. **Update imports**
   - Replace `from cnxns import dbms` with `from cnxns import cnxn, read, write`
   - Remove `from cnxns.api import m365` (if used)

2. **Update connection code**
   - Replace `dbms.dbms_cnxn(dbms=..., server=..., ...)` with `cnxn('scheme://host/db', uid=..., pwd=...)`
   - Convert connection parameters to URL format

3. **Update read operations**
   - Replace `dbms.dbms_reader(engine, ...)` with `read(conn, ..., format="pandas")`
   - Replace `dbms.dbms_read_chunks(engine, ...)` with `read(conn, ..., chunk_size=...)`
   - Note: Chunking now returns row iterators, not DataFrames

4. **Update write operations**
   - Replace `dbms.dbms_writer(engine, df, table, ...)` with `write(conn, df, table=..., format="pandas", ...)`
   - Add `format="pandas"` when writing DataFrames

5. **Update dependencies**
   - Update `requirements.txt`: `cnxns` → `cnxns[mssql,pandas]` (or appropriate extras)
   - Remove `sqlalchemy` if it was only used for cnxns

6. **Test thoroughly**
   - Core behavior is different (streaming, row-based)
   - Ensure performance is acceptable for your use case

## Gradual Migration Strategy

If you have a large codebase, consider:

1. **Pin v0.0.x** in production: `cnxns==0.0.3`
2. **Create adapter functions** that wrap new API with old signatures
3. **Migrate incrementally** — one module at a time
4. **Test each migration** before moving to the next

Example adapter:

```python
# adapter.py - temporary migration helper
from cnxns import cnxn, read, write

def dbms_cnxn(dbms, server, uid, pwd, database=None, **kwargs):
    """Adapter for old dbms_cnxn API."""
    url = f"{dbms}://{server}/{database or ''}"
    return cnxn(url, uid=uid, pwd=pwd, **kwargs)

def dbms_reader(connection, query=None, table_name=None, schema=None, columns=None):
    """Adapter for old dbms_reader API."""
    return read(
        connection,
        query=query,
        table=table_name,
        schema=schema,
        columns=columns,
        format="pandas"
    )

def dbms_writer(connection, df, table_name, schema=None, if_exists="replace"):
    """Adapter for old dbms_writer API."""
    write(
        connection,
        df,
        table=table_name,
        schema=schema,
        format="pandas",
        if_exists=if_exists
    )
```

## Getting Help

- **Issues**: [GitHub Issues](https://github.com/philipbudden/cnxns/issues)
- **Architecture**: See [AGENTS.md](AGENTS.md) for design rationale
- **Examples**: See [README.md](README.md) for comprehensive examples

## Why the Rewrite?

The v0.1.0 rewrite addresses several fundamental limitations:

1. **Tight coupling** — v0.0.x forced Pandas and SQLAlchemy on all users
2. **Limited extensibility** — Adding PySpark or Polars was difficult
3. **Memory issues** — No clear streaming/chunking model for large data
4. **Framework lock-in** — Couldn't use library without Pandas
5. **Complex internals** — SQLAlchemy abstraction wasn't providing value

The new design is **simpler, more flexible, and more maintainable** long-term.
