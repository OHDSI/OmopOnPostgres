# This function creates a connection to the local postgres instance

It relays on environmental variables such as:

- dbname = `OMOP_POSTGRES_CONNECTOR_DB`

- host = `OMOP_POSTGRES_CONNECTOR_HOST`

- port = `OMOP_POSTGRES_CONNECTOR_PORT`

- user = `OMOP_POSTGRES_CONNECTOR_USER`

- password = `OMOP_POSTGRES_CONNECTOR_PASSWORD`

## Usage

``` r
localPostgres()
```

## Value

A connection to your postgres local instance

## Examples

``` r
if (FALSE) { # \dontrun{
library(OmopOnPostgres)

localPostgres()
} # }
```
