# Copy a cdm (independently of its source) to a

Copy a cdm (independently of its source) to a

## Usage

``` r
copyCdmToPostgres(
  cdm,
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

- cdm:

  A \<cdm_reference\> object.

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

A cdm reference inserted in the new pq_cdm source.

## Examples

``` r
#library(omock)
#library(OmopOnPostgres)
#
#pq <- localPostgres()
#cdm <- mockCdmFromDataset(datasetName = "GiBleed")
#
#pq_cdm <- copyCdmToPostgres(cdm = cdm, con = pq)
#pq_cdm
```
