# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-02-17

### 🚨 BREAKING CHANGES

Complete rewrite of the library with new architecture and API.

**Migration Required**: See [MIGRATION.md](MIGRATION.md) for detailed upgrade guide.

### Added

- **Minimal Public API**: Three functions (`cnxn`, `read`, `write`) replace all previous functionality
- **Framework-Neutral Core**: Library now works without any dataframe dependencies
- **Capability-Based Backends**: Backends explicitly advertise supported features
- **PostgreSQL Support**: New backend for PostgreSQL databases
- **PySpark Support**: Read/write PySpark DataFrames via adapter pattern
- **Streaming by Default**: All read operations support chunked iteration with bounded memory
- **Optional Dependencies**: Install only what you need via extras (`mssql`, `mysql`, `postgres`, `pandas`, `spark`)
- **Comprehensive Test Suite**: 28 unit tests with pytest and coverage reporting
- **Row-Based Core**: Data represented as `Iterator[Mapping[str, Any]]` internally
- **Connection URL Format**: Simplified connection creation with URLs (e.g., `mssql://host/db`)

### Changed

- **API Redesigned**: 
  - `dbms_cnxn()` → `cnxn()`
  - `dbms_reader()` / `dbms_read_chunks()` → `read()`
  - `dbms_writer()` → `write()`
- **Dependencies**:
  - Removed: SQLAlchemy (hard dependency)
  - Removed: Pandas (hard dependency)
  - Removed: MSAL (hard dependency)
  - Changed to optional: pyodbc, pandas, pyspark, psycopg2
- **Return Types**:
  - `read()` returns row iterator by default, not DataFrame
  - DataFrame support via `format="pandas"` or `format="spark"` parameter
- **Architecture**:
  - Separation of transport, representation, and consumption layers
  - Adapter pattern for dataframe libraries
  - Protocol-based backend interface

### Removed

- **Dynamics 365 / M365 Support**: Entire `m365` module removed (focus on SQL databases)
- **SQLAlchemy Integration**: No longer uses SQLAlchemy Engine/Connection
- **Automatic Pandas Import**: Pandas is now optional

### Fixed

- Memory issues with large datasets (streaming now built-in)
- Tight coupling to specific frameworks
- Inflexible connection configuration

### Documentation

- New comprehensive README with examples
- Migration guide from v0.0.x
- Architecture documentation in AGENTS.md
- Updated installation instructions

### Testing

- pytest-based test suite
- Coverage reporting configured
- Unit tests for all core components
- Mock-based tests for adapters and API

---

## [0.0.3] - Previous Release

### Features

- MSSQL support via SQLAlchemy and pyodbc
- MySQL/MariaDB support via SQLAlchemy and pyodbc  
- Dynamics 365 / M365 Graph API support
- Pandas integration for data reading/writing
- Chunked reading via generators

### Dependencies

- sqlalchemy>=2.0.43
- pandas>=2.3.2
- pyodbc>=5.2.0
- msal==1.20.0b1

---

[0.1.0]: https://github.com/philipbudden/cnxns/compare/v0.0.3...v0.1.0
[0.0.3]: https://github.com/philipbudden/cnxns/releases/tag/v0.0.3
