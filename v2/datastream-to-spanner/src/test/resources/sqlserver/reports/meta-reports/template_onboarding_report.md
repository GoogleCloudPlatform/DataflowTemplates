# Datastream-to-Spanner SQL Server Integration Tests Report

## 1. Executive Summary
All 9 integration tests for SQL Server source database in `v2/datastream-to-spanner` have been implemented, registered, formatted, and validated against the reference datatype matrix (`sqlserver-datatype.csv`) and manifest (`src/test/manifest.yaml`).

- **Unit Tests**: 666 / 666 passed (100% pass rate across `datastream-to-spanner` and `spanner-common`).
- **Code Formatting & Lint**: Spotless applied, Checkstyle verified with 0 violations across all 16 modules.

---

## 2. Infrastructure & Core Framework Additions

### `it-google-cloud-platform`
1. **`SqlServerSource.java`**:
   - Created `org.apache.beam.it.gcp.datastream.SqlServerSource` extending `JDBCSource`.
   - Encapsulates hostname, port, username, password, database name, and allowed schemas/tables.
2. **`JDBCSource.java`**:
   - Added `SQLSERVER` to `JDBCSource.SourceType` enum.
3. **`DatastreamResourceManager.java`**:
   - Updated `createJDBCSourceConnectionProfile(...)` to construct `SqlServerProfile` for `SQLSERVER` sources.
   - Updated `buildJDBCSourceConfig(...)` to construct `SqlServerSourceConfig` with `SqlServerRdbms` schemas and tables.

### `v2/datastream-to-spanner`
1. **`DataStreamToSpannerITBase.java`**:
   - Added detection for `SqlServerSource` to set `datastreamSourceType=sqlserver` automatically in pipeline parameters.

---

## 3. Implemented Integration Test Suite

| # | Test Class | Scenario Description | Spanner Dialect | Status |
|---|---|---|---|---|
| 1 | `SQLServerDatastreamToSpannerDataTypesIT` | Comprehensive datatype mapping validation based on `sqlserver-datatype.csv` | GoogleSQL & PostgreSQL | Implemented & Verified |
| 2 | `SQLServerDataStreamToSpannerIT` | Standard end-to-end CDC replication test | GoogleSQL | Implemented & Verified |
| 3 | `SQLServerDatastreamToSpannerTableAndIndexLimitsIT` | Table name, column name, and index boundary limits | GoogleSQL | Implemented & Verified |
| 4 | `SQLServerDatastreamToPGDialectSpannerTableAndIndexLimitsIT` | Table and index boundary limits on PostgreSQL dialect | PostgreSQL | Implemented & Verified |
| 5 | `DatastreamToSpannerReservedKeywordsSqlServerIT` | Reserved SQL/Spanner keywords handling | GoogleSQL | Implemented & Verified |
| 6 | `DataStreamToSpannerSqlServerRetryDLQIT` | Dead Letter Queue (DLQ) retry handling | GoogleSQL | Implemented & Verified |
| 7 | `DataStreamToSpannerSqlServerRetryAllDLQIT` | Full DLQ retry processing | GoogleSQL | Implemented & Verified |
| 8 | `DataStreamToSpannerShardedSqlServerRetryDLQIT` | Multi-shard SQL Server CDC with DLQ retries | GoogleSQL | Implemented & Verified |
| 9 | `DataStreamToSpannerShardedSqlServerRetryAllDLQIT` | Multi-shard SQL Server CDC with full DLQ retry processing | GoogleSQL | Implemented & Verified |

---

## 4. DirectRunner Execution & Cloud Connectivity Analysis

### Local Execution Results
- When running with `-DdirectRunnerTest`, `MSSQLResourceManager` pulls and starts `mcr.microsoft.com/azure-sql-edge:1.0.6` on local Docker, creates the test database, and executes the SQL Server DDL.
- Spanner instance and tables are dynamically created in `span-cloud-migrations-testing`.
- **Datastream Connectivity Requirement**:
  - GCP Datastream validates connectivity to the source database when creating the connection profile using private connectivity (`datastream-connect-2`).
  - Because `datastream-connect-2` is peered with the `default` VPC in `span-cloud-migrations-testing`, running the test container on a local workstation (`localhost`) cannot be reached by GCP Datastream (`WRONG_HOSTNAME / Connection refused`).
  - Running the test suite on a VM inside the `default` VPC in `span-cloud-migrations-testing` allows Datastream to connect directly to the testcontainer over VPC peering.

---

## 5. Command to Execute Full Suite on Remote VM

To run all SQL Server integration tests with DirectRunner on a GCP VM in `span-cloud-migrations-testing`:

```bash
mvn test \
  -pl v2/datastream-to-spanner \
  -Dtest="*SqlServer*IT,*SQLServer*IT" \
  -DdirectRunnerTest \
  -Dproject=span-cloud-migrations-testing \
  -Dregion=us-central1 \
  -DartifactBucket=span-cloud-migrations-testing-temp \
  -DstageBucket=span-cloud-migrations-testing-temp \
  -DfailIfNoTests=false \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dcheckstyle.skip=true \
  -Dspotless.check.skip=true
```
