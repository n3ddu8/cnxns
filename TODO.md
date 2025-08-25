# TODO

## Additional functionality:
- Add BCPandas for faster loading in SQL Server
  - Seems to be failing SSL certificate checks with ODBC Driver 18.

## Refactor
- Add x3 internal functions for connecting to each flavour of SQL using the default method (not specfiically ODBC). Call the relevant internal function from the public function depending on the flavour argument.

## Add/to Files:
### Dockerfile
- Install ODBC drivers
  - MSSQL
  - MySQL

### Devcontainer
- Add precommits
