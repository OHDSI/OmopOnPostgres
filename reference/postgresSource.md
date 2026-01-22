# Create a postgres source object

Create a postgres source object

## Usage

``` r
postgresSource(
  con,
  cdmSchema = "public",
  cdmPrefix = "",
  writeSchema = "results",
  writePrefix = "",
  achillesSchema = NULL,
  achillesPrefix = ""
)
```

## Arguments

- con:

  A PqConnection created with DBI and RPostgres.

- cdmSchema:

  String, name of the schema for the OMOP CDM Standard tables.

- cdmPrefix:

  String, prefix leading the OMOP CDM Standard tables.

- writeSchema:

  String, name of the schema for the cohort and permanent tables.

- writePrefix:

  String, prefix leading the cohort and permanent tables.

- achillesSchema:

  String, name of the schema for the achilles tables.

- achillesPrefix:

  String, prefix leading the achilles tables.

## Value

A \<pq_cdm\> source object
