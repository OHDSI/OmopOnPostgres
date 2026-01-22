# Create a \<cdm_reference\> object from a postgres connection

Create a \<cdm_reference\> object from a postgres connection

## Usage

``` r
cdmFromPostgres(
  con,
  cdmName = NULL,
  cdmSchema = "public",
  cdmPrefix = "",
  writeSchema = "results",
  writePrefix = "",
  achillesSchema = NULL,
  achillesPrefix = "",
  cdmVersion = NULL,
  cohortTables = character(),
  validation = TRUE
)
```

## Arguments

- con:

  A PqConnection created with DBI and RPostgres.

- cdmName:

  String, name of the cdm object.

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

- cdmVersion:

  String, OMOP CDM version of the cdm object.

- cohortTables:

  Character vector, names of the cohort tables.

- validation:

  Logical, wether to validate the cdm object or not.

## Value

A \<cdm_reference\> object.
