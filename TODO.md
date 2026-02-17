# TODO

## v0.1.0 - Complete ✅

- [x] Core architecture redesign
- [x] Backend abstraction protocol
- [x] MSSQL, MySQL, PostgreSQL backends
- [x] Pandas and PySpark adapters
- [x] Public API (cnxn, read, write)
- [x] Comprehensive test suite
- [x] Documentation and migration guide
- [x] Remove old code (dbms, m365)

## Future Enhancements

### Short Term
- [ ] Add connection pooling support
- [ ] Improve error messages
- [ ] Add logging/debugging support
- [ ] Type hints refinement
- [ ] Performance benchmarking
- [ ] CI/CD pipeline (GitHub Actions)

### Medium Term
- [ ] Polars adapter
- [ ] Connection context managers (`with cnxn(...) as conn`)
- [ ] Async support for I/O-bound operations
- [ ] Query result metadata (column types, row counts)
- [ ] Batch insert optimization
- [ ] Connection string parsing improvements

### Long Term
- [ ] Additional backends (SQLite, DuckDB, ClickHouse)
- [ ] Query builder/DSL (optional)
- [ ] Schema introspection utilities
- [ ] Data validation hooks
- [ ] Observability/metrics integration
- [ ] Arrow native support for zero-copy transfers

### Documentation
- [ ] API reference documentation
- [ ] Tutorial/cookbook examples
- [ ] Performance tuning guide
- [ ] Backend development guide
- [ ] Adapter development guide

### Testing
- [ ] Integration tests with real databases (Docker containers)
- [ ] Property-based tests (Hypothesis)
- [ ] Load/stress testing
- [ ] Cross-platform testing (Linux, macOS, Windows)

## Non-Goals

- Full ORM functionality (use SQLAlchemy/Django instead)
- Complex query generation (use dedicated query builders)
- Database migrations (use Alembic/similar)
- Schema management (out of scope)

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
