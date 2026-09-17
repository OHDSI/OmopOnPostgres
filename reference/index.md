# Package index

## Create a cdm_reference

- [`cdmFromPostgres()`](https://ohdsi.github.io/OmopOnPostgres/reference/cdmFromPostgres.md)
  : Create a \<cdm_reference\> object from a postgres connection

## Copy a cdm_reference to a postgres source

- [`copyCdmToPostgres()`](https://ohdsi.github.io/OmopOnPostgres/reference/copyCdmToPostgres.md)
  : Copy a cdm (independently of its source) to a

## Create empty OMOP CDM tables

- [`createOmopTablesOnPostgres()`](https://ohdsi.github.io/OmopOnPostgres/reference/createOmopTablesOnPostgres.md)
  : Title

## Create and read logging files

- [`postgresLog()`](https://ohdsi.github.io/OmopOnPostgres/reference/postgresLog.md)
  : Start a postgres log
- [`readPostgresLog()`](https://ohdsi.github.io/OmopOnPostgres/reference/readPostgresLog.md)
  : Create a tibble from the log files in the logPath folder

## Job management

Cancel and manage jobs.

- [`getJobs()`](https://ohdsi.github.io/OmopOnPostgres/reference/getJobs.md)
  : Get ongoing Postgres jobs.
- [`cancelJob()`](https://ohdsi.github.io/OmopOnPostgres/reference/cancelJob.md)
  : Cancel a Postgres job.
- [`getUserTables()`](https://ohdsi.github.io/OmopOnPostgres/reference/getUserTables.md)
  : Get tables created by a user in a schema.

## Index mangement

Create and check existing indexes.

- [`reexports`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`expectedIndexes`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`existingIndexes`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`statusIndexes`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`createIndexes`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`createTableIndex`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`listSourceTables`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`insertTable`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`dropSourceTable`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`insertCdmTo`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`readSourceTable`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`cdmDisconnect`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`cdmTableFromSource`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`compute`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  : Objects exported from other packages

## Create a postgres source object

- [`postgresSource()`](https://ohdsi.github.io/OmopOnPostgres/reference/postgresSource.md)
  : Create a postgres source object

## CDM object managment

Reexported functions from other packages for the cdm_reference object
management.

- [`reexports`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`expectedIndexes`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`existingIndexes`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`statusIndexes`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`createIndexes`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`createTableIndex`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`listSourceTables`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`insertTable`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`dropSourceTable`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`insertCdmTo`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`readSourceTable`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`cdmDisconnect`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`cdmTableFromSource`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  [`compute`](https://ohdsi.github.io/OmopOnPostgres/reference/reexports.md)
  : Objects exported from other packages

## Create a local postgres

Create a local postgres connection using environmental variables.

- [`localPostgres()`](https://ohdsi.github.io/OmopOnPostgres/reference/localPostgres.md)
  : This function creates a connection to the local postgres instance
- [`mockPostgresCdmReference()`](https://ohdsi.github.io/OmopOnPostgres/reference/mockPostgresCdmReference.md)
  : Mock Postgres CDM reference

## Benchmark postgres

Assess time to collect from and insert data into a postgres cdm
reference

- [`benchmarkOmopOnPostgres()`](https://ohdsi.github.io/OmopOnPostgres/reference/benchmarkOmopOnPostgres.md)
  : Benchmark OMOP on Postgres

## DuckDb utilities

Utilities to support working with DuckDb and Postgres

- [`attachPgDb()`](https://ohdsi.github.io/OmopOnPostgres/reference/attachPgDb.md)
  : Attach a postres database to a duckdb database
- [`duckdbHasPgDbAttached()`](https://ohdsi.github.io/OmopOnPostgres/reference/duckdbHasPgDbAttached.md)
  : Check if a PostgreSQL database is attached to DuckDB
- [`duckdbUsePgDb()`](https://ohdsi.github.io/OmopOnPostgres/reference/duckdbUsePgDb.md)
  : Ensure duckdb is using Postgres as default catalog
- [`duckdbUseDuckDb()`](https://ohdsi.github.io/OmopOnPostgres/reference/duckdbUseDuckDb.md)
  : Ensure duckdb is using the native DuckDB catalog
- [`duckdbIsUsingPgDb()`](https://ohdsi.github.io/OmopOnPostgres/reference/duckdbIsUsingPgDb.md)
  : Check if duckdb is using Postgres as default catalog
