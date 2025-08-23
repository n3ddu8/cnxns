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

    Creates and returns an MSSQL Server SQLAlchemy Engine object from given
    connection details.

    Args:
        dbms (String): the DBMS flavour, accepted forms are:
            - mssql
            - mysql (use this for MariaDB also)
            - postgresql
        server (String): the connection string or IP address for server
            instance.
        uid (String): the username for connecting to server instance.
        pwd (String): the corresponding password for the given username.
        **port (Integer): port to connect to dbms over. If none given the
            default for each supported dbms type will be used.
        **driver (String): details of the ODBC driver installed on the codes
            host system, if using mssql.
            Default = ODBC Driver 18 for SQL Server.
        **database (String): name of the database, if connecting to a specific
            database on the server instance. Default = no database.
        **trust (Boolean): Trust the server certificate. Default = False.

    Returns:
        Engine: A SQLAlchemy Engine object.
    """

    port = kwargs.get("port", None)
    driver = kwargs.get("driver", "ODBC Driver 18 for SQL Server")
    database = kwargs.get("database", "")
    trust = kwargs.get("trust", False)

    match dbms:

        case "mssql":

            if not port:
                port = 1433

            trust_str = "Yes" if trust else "No"

            cnxn_str = (
                f"DRIVER={driver};"
                f"SERVER={server},{port};"
                f"UID={uid};"
                f"PWD={pwd};"
                f"DATABASE={database};"
                f"MARS_Connection=Yes;"
                f"TrustServerCertificate={trust_str};"
            )

            quoted_cnxn_str = urllib.parse.quote_plus(cnxn_str)

            engine = sa.create_engine(
                f"mssql+pyodbc:///?odbc_connect={quoted_cnxn_str}",
                connect_args={"autocommit": True},
                fast_executemany=True,
            )

        case "mysql" | "postgresql":

            flav = "mysql+pymysql" if dbms == "mysql" else "postgresql"
            if not port:
                port = 3306 if dbms == "mysql" else 5432

            cnxn_str = f"{flav}://{uid}:{pwd}@{server}:{port}"
            cnxn_str += f"/{database}" if database else ""

            if trust:
                trust_str = (
                    "ssl_verify_cert=False" if flav == "mysql+pymysql"
                    else "ssl_disabled=True"
                )

                cnxn_str += f"?{trust_str}"

            engine = sa.create_engine(
                cnxn_str,
                connect_args={"autocommit": True} if dbms == "mysql" else "",
            )

        case _:

            raise ValueError("""
                Unacceptable argument given for dbms,
                run help(dbms_cnxn) for more information.
            """)

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
            provided. Default = dbo.
        **columns (List[String]): A list of columns to return from the given
            table_name. Ignored if a query is provided. Default = *.

    Returns:
        DataFrame: A DataFrame of the returned data.
    """

    query = kwargs.get("query", None)
    table_name = kwargs.get("table_name", None)
    schema = kwargs.get("schema", "dbo")
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
            provided. Default = dbo.
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
    schema = kwargs.get("schema", "dbo")
    val = kwargs.get("columns")
    columns = ",".join(val) if isinstance(val, list) else "*"
    chunksize = kwargs.get("chunksize", None)

    if not query:
        query = f"""
            SELECT {columns}
              FROM {schema}.{table_name}
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
            Default = "dbo".
        **if_exists (String): Behaviour if the table already exists.
            Default = "replace".

    Returns
        None.
    """

    schema = kwargs.get("schema", "dbo")
    if_exists = kwargs.get("if_exists", "replace")

    with cnxn_engine.connect() as cnxn:
        df.to_sql(
            table_name,
            cnxn,
            schema=schema,
            index=False,
            if_exists=if_exists,
        )
