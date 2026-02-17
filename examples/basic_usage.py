"""Basic usage examples for cnxns library."""

from cnxns import cnxn, read, write


def example_raw_rows():
    """Example: Working with raw row dictionaries (no framework needed)."""
    print("=== Example 1: Raw Row Iteration ===")
    
    # Connect to database
    conn = cnxn('mssql://localhost/mydb', uid='sa', pwd='YourPassword123')
    
    # Read as row dictionaries
    for row in read(conn, query="SELECT TOP 5 * FROM users"):
        print(f"User {row['id']}: {row['name']}")
    
    # Write from row dictionaries
    data = [
        {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'},
        {'id': 2, 'name': 'Bob', 'email': 'bob@example.com'},
    ]
    write(conn, data, table="users_backup")
    
    conn.close()
    print()


def example_with_pandas():
    """Example: Using Pandas DataFrames."""
    print("=== Example 2: Pandas Integration ===")
    
    try:
        import pandas as pd
    except ImportError:
        print("Pandas not installed. Install with: pip install cnxns[pandas]")
        return
    
    conn = cnxn('mysql://localhost/mydb', uid='root', pwd='password')
    
    # Read into DataFrame
    df = read(conn, table="products", schema="inventory", format="pandas")
    print(f"Loaded {len(df)} products")
    print(df.head())
    
    # Write from DataFrame
    write(conn, df, table="products_snapshot", format="pandas", if_exists="replace")
    
    conn.close()
    print()


def example_streaming():
    """Example: Streaming large datasets with chunking."""
    print("=== Example 3: Streaming/Chunking ===")
    
    conn = cnxn('postgresql://localhost/warehouse', uid='postgres', pwd='secret')
    
    # Process large table in chunks (bounded memory)
    total_rows = 0
    for chunk in read(conn, table="transactions", chunk_size=10000):
        chunk_rows = list(chunk)
        total_rows += len(chunk_rows)
        print(f"Processing chunk of {len(chunk_rows)} rows...")
        # Process chunk here
    
    print(f"Total rows processed: {total_rows}")
    
    conn.close()
    print()


def example_data_warehouse():
    """Example: Data warehouse pattern with schemas."""
    print("=== Example 4: Data Warehouse with Schemas ===")
    
    conn = cnxn('mssql://dwh-server/analytics', uid='etl_user', pwd='EtlPass123')
    
    # Read from staging schema
    staging_data = list(read(
        conn,
        table="daily_sales",
        schema="staging",
        columns=["date", "product_id", "quantity", "revenue"]
    ))
    
    print(f"Loaded {len(staging_data)} rows from staging")
    
    # Transform data (example)
    transformed = [
        {**row, 'processed_at': '2026-02-17'}
        for row in staging_data
    ]
    
    # Write to production schema
    write(
        conn,
        transformed,
        table="fact_sales",
        schema="dwh",
        if_exists="append"
    )
    
    print(f"Wrote {len(transformed)} rows to warehouse")
    
    conn.close()
    print()


def example_pyspark():
    """Example: Using PySpark DataFrames."""
    print("=== Example 5: PySpark Integration ===")
    
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        print("PySpark not installed. Install with: pip install cnxns[spark]")
        return
    
    spark = SparkSession.builder.appName("cnxns-example").getOrCreate()
    conn = cnxn('postgresql://localhost/events', uid='spark', pwd='SparkPass123')
    
    # Read into Spark DataFrame
    spark_df = read(
        conn,
        query="SELECT * FROM events WHERE date >= '2024-01-01'",
        format="spark",
        spark_session=spark
    )
    
    print(f"Loaded Spark DataFrame with schema:")
    spark_df.printSchema()
    
    # Transform with Spark
    filtered_df = spark_df.filter(spark_df.event_type == "purchase")
    
    # Write back to database
    write(
        conn,
        filtered_df,
        table="purchase_events",
        schema="analytics",
        format="spark"
    )
    
    conn.close()
    spark.stop()
    print()


if __name__ == "__main__":
    print("Cnxns Library - Usage Examples\n")
    print("Note: Update connection strings with your actual database credentials\n")
    
    # Uncomment the examples you want to run:
    
    # example_raw_rows()
    # example_with_pandas()
    # example_streaming()
    # example_data_warehouse()
    # example_pyspark()
    
    print("Examples complete!")
