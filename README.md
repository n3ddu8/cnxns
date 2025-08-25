# cnxns

## About

cnxns abstracts the complexity of working with a variety of data systems.

## Supported Systems
### DBMS
- Microsoft SQL Server
- MySQL/MariaDB

## Getting started

```shell
pip install git+https://github.com/n3ddu8/cnxns.git#egg=cnxns
python
from cnxns import dbms
help(dbms)
```

## Example Usage

```python
from cnxns import dbms as db

e = db.dbms_cnxn(
    dbms = "mssql",
    server = "localhost",
    uid = "sa",
    pwd = "YourStrong@Passw0rd",
    database = "dev",
)

df = db.dbms_reader(
    e,
    table_name = "myAwesomeTable",
)

db.dbms_writer(
    e,
    df,
    "myAwesomeTableSnapshot",
    if_exists="append",
)
```

## Dependencies
- ODBC Driver X for SQL Server (tested with 18)
- MySQL ODBC X driver (tested with 9.4)
