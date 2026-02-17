<a name="readme-top"></a>

<h1 align="center">⚠️ THIS PROJECT IS IN ALPHA - v0.1.0 IS A COMPLETE REWRITE</h1>

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]

<br />
<div align="center">
  <a href="https://github.com/philipbudden/cnxns">
    <img src=".logo.png" alt="Logo" width="80" height="80">
  </a>

<h3 align="center">Cnxns</h3>

  <p align="center">
    A lightweight, extensible library for interacting with data systems.
    <br />
    Minimal API • Framework Neutral • Streaming by Default
    <br />
    <a href="https://github.com/philipbudden/cnxns/issues">Report Bug</a>
    ·
    <a href="https://github.com/philipbudden/cnxns/issues">Request Feature</a>
  </p>
</div>

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about">About</a></li>
    <li><a href="#installation">Installation</a></li>
    <li><a href="#quick-start">Quick Start</a></li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#architecture">Architecture</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
  </ol>
</details>

## About

Cnxns provides a unified interface for reading and writing data across diverse systems. It abstracts connection management, authentication, and data transfer into three simple functions:

- **`cnxn`** — Create a connection to a data system
- **`read`** — Read data from a connection  
- **`write`** — Write data to a connection

### Design Principles

- **Minimal API**: Three functions cover all use cases
- **Framework Neutral**: Core library has zero dependencies
- **Streaming by Default**: Designed for large datasets with bounded memory
- **Capability-Based**: Backends advertise what they support, no false uniformity
- **Adapter Pattern**: Dataframe libraries (Pandas, PySpark, Polars) are optional add-ons

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Installation

### Core Library
```bash
pip install cnxns
```

### With Backend Support
```bash
# SQL Server
pip install cnxns[mssql]

# MySQL/MariaDB
pip install cnxns[mysql]

# PostgreSQL
pip install cnxns[postgres]
```

### With Framework Support
```bash
# Pandas
pip install cnxns[pandas]

# PySpark
pip install cnxns[spark]

# Everything
pip install cnxns[all]
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Quick Start

### Basic Usage (No Dataframe Library Required)

```python
from cnxns import cnxn, read, write

# Connect
conn = cnxn('mssql://localhost/mydb', uid='sa', pwd='password')

# Read as row dictionaries
for row in read(conn, query="SELECT * FROM users"):
    print(row)  # {'id': 1, 'name': 'Alice', ...}

# Write from row dictionaries  
data = [
    {'id': 1, 'name': 'Alice'},
    {'id': 2, 'name': 'Bob'},
]
write(conn, data, table="users")

conn.close()
```

### With Pandas

```python
from cnxns import cnxn, read, write

conn = cnxn('mysql://localhost/mydb', uid='user', pwd='pass')

# Read into DataFrame
df = read(conn, table="sales", schema="analytics", format="pandas")

# Write from DataFrame
write(conn, df, table="sales_backup", format="pandas", if_exists="replace")
```

### With PySpark

```python
from cnxns import cnxn, read, write
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
conn = cnxn('postgresql://localhost/warehouse', uid='postgres', pwd='secret')

# Read into Spark DataFrame
spark_df = read(
    conn,
    query="SELECT * FROM events WHERE date > '2024-01-01'",
    format="spark",
    spark_session=spark
)

# Write from Spark DataFrame
write(conn, spark_df, table="events", schema="staging", format="spark")
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Usage

### Connections

Create connections using connection URLs:

```python
# SQL Server
conn = cnxn('mssql://server/database', uid='user', pwd='pass')
conn = cnxn('mssql://server:1433/db', uid='user', pwd='pass', trust_cert=True)

# MySQL/MariaDB  
conn = cnxn('mysql://host/database', uid='user', pwd='pass')
conn = cnxn('mysql://host:3307/db', uid='user', pwd='pass')

# PostgreSQL
conn = cnxn('postgresql://host/database', uid='user', pwd='pass')
conn = cnxn('postgres://host:5433/db', uid='user', pwd='pass')

# Credentials in URL
conn = cnxn('mssql://user:pass@server/database')
```

### Reading Data

**From Queries:**
```python
# Raw rows (no framework needed)
rows = read(conn, query="SELECT id, name FROM users WHERE active = 1")
for row in rows:
    print(row)

# As Pandas DataFrame
df = read(conn, query="SELECT * FROM orders", format="pandas")

# As PySpark DataFrame  
spark_df = read(conn, query="SELECT * FROM events", format="spark", spark_session=spark)
```

**From Tables:**
```python
# Entire table
rows = read(conn, table="users")

# With schema (data warehouse pattern)
rows = read(conn, table="customers", schema="ods_finance")

# Select specific columns
rows = read(conn, table="products", columns=["id", "name", "price"])
```

**Streaming/Chunking:**
```python
# Process large datasets in chunks (bounded memory)
for chunk in read(conn, table="transactions", chunk_size=10000):
    for row in chunk:
        process(row)
```

### Writing Data

```python
# From row dictionaries
data = [{'id': 1, 'value': 'test'}]
write(conn, data, table="logs")

# From Pandas DataFrame
write(conn, df, table="results", format="pandas")

# From PySpark DataFrame
write(conn, spark_df, table="aggregates", format="spark")

# With schema (data warehouse)
write(conn, data, table="fact_sales", schema="dwh")

# Control behavior if table exists
write(conn, data, table="users", if_exists="append")   # append rows
write(conn, data, table="users", if_exists="replace")  # drop and recreate (default)
write(conn, data, table="users", if_exists="fail")     # raise error
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Architecture

### Core Design

```

  Public API: cnxn, read, write          │

              ↓

  Framework Adapters (optional)          │
  • Pandas                               │
  • PySpark                              │
  • Polars (future)                      │

              ↓

  Core: Iterator[Mapping[str, Any]]      │
  (framework-neutral row streams)        │

              ↓

  Backends (capability-based)            │
  • MSSQL  • MySQL  • PostgreSQL         │

```

### Supported Backends

| Backend | Streaming | Schemas | Transactions | Driver Required |
|---------|-----------|---------|--------------|-----------------|
| MSSQL   | ✅ | ✅ | ✅ | pyodbc + ODBC Driver 18 |
| MySQL   | ✅ | ✅ | ✅ | pyodbc + MySQL ODBC 9.4 |
| PostgreSQL | psycopg2 | | ✅ | ✅ | 

### Supported Frameworks

| Framework | Status | Install |
|-----------|--------|---------|
| Pandas    | ✅ Stable | `pip install cnxns[pandas]` |
| PySpark   | ✅ Stable | `pip install cnxns[spark]` |
| Polars    | 🚧 Planned | - |

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Migration from v0.0.x

**v0.1.0 is a complete rewrite with breaking changes.**

### Old API (v0.0.x)
```python
from cnxns import dbms

engine = dbms.dbms_cnxn(dbms="mssql", server="localhost", ...)
df = dbms.dbms_reader(engine, query="SELECT * FROM users")
dbms.dbms_writer(engine, df, "users")
```

### New API (v0.1.0+)
```python
from cnxns import cnxn, read, write

conn = cnxn('mssql://localhost/db', uid='user', pwd='pass')
df = read(conn, query="SELECT * FROM users", format="pandas")
write(conn, df, table="users", format="pandas")
```

**Key Changes:**
- SQLAlchemy dependency removed
- Pandas is now optional
- Dynamics 365 support removed
- Connection URLs instead of parameter dicts
- Unified `read`/`write` instead of separate reader/writer functions

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Contributing

Contributions are welcome! This project follows these principles:

1. **Minimal API** — Don't add public functions unless absolutely necessary
2. **Framework Neutral** — Core must work without any dataframe library
3. **Capability-Based** — Backends declare what they support
4. **Streaming First** — Assume large datasets by default
5. **Clear Boundaries** — Transport, representation, and consumption are separate

See [AGENTS.md](AGENTS.md) for detailed architectural guidance.

### Development Setup

```bash
git clone https://github.com/philipbudden/cnxns
cd cnxns
pip install -e ".[dev]"
pytest tests/
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## License

Distributed under the MIT License. See `LICENSE` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
[contributors-shield]: https://img.shields.io/github/contributors/philipbudden/cnxns.svg?style=for-the-badge
[contributors-url]: https://github.com/philipbudden/cnxns/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/philipbudden/cnxns.svg?style=for-the-badge
[forks-url]: https://github.com/philipbudden/cnxns/network/members
[stars-shield]: https://img.shields.io/github/stars/philipbudden/cnxns.svg?style=for-the-badge
[stars-url]: https://github.com/philipbudden/cnxns/stargazers
[issues-shield]: https://img.shields.io/github/issues/philipbudden/cnxns.svg?style=for-the-badge
[issues-url]: https://github.com/philipbudden/cnxns/issues
[license-shield]: https://img.shields.io/github/license/philipbudden/cnxns.svg?style=for-the-badge
[license-url]: https://github.com/philipbudden/cnxns/blob/main/LICENSE
