# cnxns

## About

cnxns abstracts the complexity of working with a variety of data systems.

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
