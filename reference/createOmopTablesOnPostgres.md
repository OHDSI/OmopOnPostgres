# Title

Title

## Usage

``` r
createOmopTablesOnPostgres(
  con,
  cdmSchema = "public",
  cdmVersion = "5.4",
  overwrite = FALSE,
  bigInt = FALSE,
  cdmPrefix = ""
)
```

## Arguments

- con:

  A PqConnection created with DBI and RPostgres.

- cdmSchema:

  String, name of the schema to create the OMOP CDM Standard tables.

- cdmVersion:

  Version of the OMOP CDM, it can be either '5.3' or '5.4'.

- overwrite:

  Whether to overwrite if tables already exist.

- bigInt:

  Whether to use `bigint` for person_id and unique identifier.

- cdmPrefix:

  String, prefix leading the OMOP CDM Standard tables names.

## Value

The omop tables will be created empty in the desired schema.
