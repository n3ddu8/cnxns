import urllib.parse
from typing import Generator

import pandas as pd
import sqlalchemy as sa
from pandas import DataFrame
from sqlalchemy import Engine
from sqlalchemy.engine.base import Connection


def dbms_cnxn(
    dbms: str,
    server: str,
    uid: str,
    pwd: str,
    **kwargs
) -> Engine:
    """
    Returns a SQLAlchemy Engine object.

    Creates and returns a SQLAlchemy Engine object from given connection
    details, using ODBC connection strings encoded with
    urllib.parse.quote_plus.

    Args:
        dbms (String): the DBMS flavour, accepted forms are:
            - mssql
            - mysql (use this for MariaDB also)
        server (String): the connection string or IP address for the instance.
        uid (String): the username for connecting to server instance.
        pwd (String): the corresponding password for the given username.
        **port (Integer): port to connect to dbms over. Defaults per dbms type.
        **driver (String): details of the ODBC driver installed on the host.
            - mssql default: ODBC Driver 18 for SQL Server
            - mysql default: MySQL ODBC 9.4 Driver
        **database (String): database name. Default = no database.
        **trust (Boolean): Trust the server certificate. Default = False.

    Returns:
        Engine: A SQLAlchemy Engine object.
    """

    database = kwargs.get("database", "")
    port = kwargs.get("port")
    trust = kwargs.get("trust", False)

    defaults = {
        "mssql": {"driver": "ODBC Driver 18 for SQL Server", "port": 1433},
        "mysql": {"driver": "MySQL ODBC 9.4 Driver", "port": 3306},
    }

    if dbms not in defaults:
        raise ValueError(f"Unsupported dbms: {dbms}")

    driver = kwargs.get("driver", defaults[dbms]["driver"])
    port = port or defaults[dbms]["port"]

    cnxn_params = {
        "DRIVER": driver,
        "SERVER": f"{server},{port}" if dbms == "mssql" else server,
        "PORT": "" if dbms == "mssql" else port,
        "UID": uid,
        "PWD": pwd,
        "DATABASE": database,
    }

    if dbms == "mssql":
        cnxn_params["MARS_Connection"] = "Yes"
        cnxn_params["TrustServerCertificate"] = "Yes" if trust else "No"

    elif dbms == "mysql":
        if trust:
            cnxn_params["ssl_verify_cert"] = "0"

    cnxn_str = ";".join(
        f"{k}={v}" for k, v in cnxn_params.items() if v not in (None, "")
    ) + ";"

    quoted_cnxn_str = urllib.parse.quote_plus(cnxn_str)

    url_prefix = {
        "mssql": "mssql+pyodbc",
        "mysql": "mysql+pyodbc",
    }[dbms]

    engine_kwargs = {
        "connect_args": (
            {"autocommit": True}
            if dbms in ("mysql", "mssql")
            else {}
        ),
    }

    if dbms == "mssql":
        engine_kwargs["fast_executemany"] = True

    engine = sa.create_engine(
        f"{url_prefix}:///?odbc_connect={quoted_cnxn_str}",
        **engine_kwargs
    )

    return engine


def dbms_reader(
    cnxn: Connection,
    **kwargs
) -> DataFrame:
    """
    Returns a DataFrame object of data from a SQL instance.

    Wrapper for dbms_read_chunks. Calls function with no chunksize and returns
    the DataFrame extracted from the Generator object.

    Args:
        cnxn (Connection): A SQLAlchemy connection object.
        **query (String): A query to be run against the connection object.
            Must provide either a query or a table_name. Default = None.
        **table_name (String): A table to return. Must provide either a query
            or table_name. Default = None.
        **schema (String): A schema for the table_name. Ignored if a query is
            provided. Default = None.
        **columns (List[String]): A list of columns to return from the given
            table_name. Ignored if a query is provided. Default = *.

    Returns:
        DataFrame: A DataFrame of the returned data.
    """

    query = kwargs.get("query", None)
    table_name = kwargs.get("table_name", None)
    schema = kwargs.get("schema", None)
    val = kwargs.get("columns")
    columns = ",".join(val) if isinstance(val, list) else "*"

    chunk = dbms_read_chunks(
        cnxn,
        query=query,
        table_name=table_name,
        schema=schema,
        columns=columns,
    )

    return next(chunk)


def dbms_read_chunks(
    cnxn: Connection,
    **kwargs
) -> Generator:
    """
    Yields a Generator object of data from a SQL instance.

    Given a SQLAlchemy Connection object and a set of criteria, read data from
    a SQL instance in chunks and return as a Generator object containing
    DataFrames.

    Args:
        cnxn (Connection): A SQLAlchemy connection object.
        **query (String): A query to be run against the connection object.
            Must provide either a query or a table_name. Default = None.
        **table_name (String): A table to return. Must provide either a query
            or table_name. Default = None.
        **schema (String): A schema for the table_name. Ignored if a query is
            provided. Default = None.
        **columns (List[String]): A list of columns to return from the given
            table_name. Ignored if a query is provided. Default = *.
        **chunksize (Integer): The size of each chunk of data to read-in.
            If not specified, the whole table will be read-in.

    Yields:
        Generator: A Generator object containing DataFrames of the returned
            data.
    """

    query = kwargs.get("query", None)
    table_name = kwargs.get("table_name", None)
    schema = kwargs.get("schema", None)
    val = kwargs.get("columns")
    columns = ",".join(val) if isinstance(val, list) else "*"
    chunksize = kwargs.get("chunksize", None)

    if not schema:
        table = table_name
    else:
        table = f"{schema}.{table_name}"

    if query is None:
        query = f"""
            SELECT {columns}
              FROM {table}
        """

    if chunksize:
        for chunk in pd.read_sql(query, cnxn, chunksize=chunksize):
            yield chunk

    else:
        chunk = pd.read_sql(query, cnxn)
        yield chunk


def dbms_writer(
    cnxn_engine: Engine,
    df: DataFrame,
    table_name: str,
    **kwargs
) -> None:
    """
    Writes a given DataFrame to a table.

    Given a SQLAlchemy Engine object, a DataFrame and a given set of criteria,
    writes the DataFrame to a table in the database represented by the engine
    using the Bulk Copy Program (BCP), or if that fails and fallback is set to
    True, via a SQLAlchemy Connection.

    Args:
        cnxn_engine (Engine): A SQLAlchemy Engine object.
        df (DataFrame): A DataFrame to be written out.
        schema (String): The schema of the table to be written to.
        table_name (String): The table to be written to.
        **schema (String): The schema of the table to be written to.
            Default = None.
        **if_exists (String): Behaviour if the table already exists.
            Default = "replace".

    Returns
        None.
    """

    schema = kwargs.get("schema", None)
    if_exists = kwargs.get("if_exists", "replace")

    with cnxn_engine.connect() as cnxn:
        df.to_sql(
            table_name,
            cnxn,
            schema=schema,
            index=False,
            if_exists=if_exists,
        )
