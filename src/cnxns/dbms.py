import urllib.parse
from typing import Generator

import pandas as pd
import sqlalchemy as sa
from bcpandas import SqlCreds
from bcpandas import to_sql
from pandas import DataFrame
from pandas.core.groupby.groupby import DataError
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

    Returns:
        Engine: A SQLAlchemy Engine object.
    """

    driver = kwargs.get("driver", "ODBC Driver 18 for SQL Server")
    database = kwargs.get("database", "")

    match dbms:

        case "mssql":

            cnxn_str = (
                f"DRIVER={driver};"
                f"SERVER={server};"
                f"UID={uid};"
                f"PWD={pwd};"
                f"DATABASE={database};"
                f"MARS_Connection=Yes;"
            )

            quoted_cnxn_str = urllib.parse.quote_plus(cnxn_str)

            engine = sa.create_engine(
                f"mssql+pyodbc:///?odbc_connect={quoted_cnxn_str}",
                connect_args={"autocommit": True},
                fast_executemany=True,
            )

        case "mysql" | "postgresql":

            flav = "mysql+pymysql" if dbms == "mysql" else "postgresql"
            port = 3306 if dbms == "mysql" else 5432

            cnxn_str = f"{flav}://{uid}:{pwd}@{server}:{port}"
            cnxn_str += f"/{database}" if database else ""

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
    columns = kwargs.get(",".join("columns"), "*")

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
    columns = kwargs.get("columns", "*")
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
        **bcp (Boolean): If True, attempt to use the BCP utility to write out
            data. SQL Server only. Default = False.
        **fallback (Boolean): When using bcp, if True, and BCP returns a
            DataError, write the data out using a SQLAlchemy Connection
            object. Default = False.

    Returns
        None.
    """

    schema = kwargs.get("schema", "dbo")
    if_exists = kwargs.get("if_exists", "replace")
    bcp = kwargs.get("bcp", False)
    fallback = kwargs.get("fallback", False)

    cnxn = cnxn_engine.connect()

    pandas_func = (
        lambda: df.to_sql(
            table_name,
            cnxn,
            schema=schema,
            index=False,
            if_exists=if_exists,
        )
    )

    if bcp:

        creds = SqlCreds.from_engine(cnxn_engine)

        try:
            to_sql(
                df,
                table_name,
                creds,
                schema=schema,
                index=False,
                if_exists=if_exists,
            )

        except DataError as e:

            if fallback:
                pandas_func()

            else:
                raise Exception(e)

    else:
        pandas_func()
