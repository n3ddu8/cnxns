# Examples

This directory contains practical examples demonstrating various use cases of the cnxns library.

## Files

- **basic_usage.py** - Comprehensive examples covering common scenarios

## Running Examples

1. Update connection strings with your actual database credentials
2. Ensure required dependencies are installed
3. Uncomment the example functions you want to run
4. Execute the script:

```bash
python examples/basic_usage.py
```

## Example Scenarios

### 1. Raw Row Iteration (No Framework)
Works without any dataframe library installed.

```python
from cnxns import cnxn, read

conn = cnxn('mssql://localhost/db', uid='user', pwd='pass')
for row in read(conn, query="SELECT * FROM users"):
    print(row)  # {'id': 1, 'name': 'Alice'}
```

### 2. Pandas Integration
Requires: `pip install cnxns[pandas]`

```python
df = read(conn, table="products", format="pandas")
write(conn, df, table="products_backup", format="pandas")
```

### 3. Streaming Large Datasets
Process data in chunks to avoid memory issues.

```python
for chunk in read(conn, table="big_table", chunk_size=10000):
    for row in chunk:
        process(row)
```

### 4. Data Warehouse Pattern
Work with schemas for organized data storage.

```python
# Read from staging
data = read(conn, table="sales", schema="staging")

# Write to warehouse
write(conn, data, table="fact_sales", schema="dwh", if_exists="append")
```

### 5. PySpark Integration
Requires: `pip install cnxns[spark]`

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark_df = read(conn, query="SELECT * FROM events", format="spark", spark_session=spark)
write(conn, spark_df, table="events_backup", format="spark")
```

## Prerequisites

Ensure you have appropriate database drivers installed:

- **MSSQL**: ODBC Driver 18 for SQL Server
- **MySQL**: MySQL ODBC 9.4 Driver
- **PostgreSQL**: psycopg2 (installed via `cnxns[postgres]`)

## Notes

- Examples use placeholder credentials - replace with actual values
- Some databases may need to be running locally or accessible via network
- Test on non-production databases first
