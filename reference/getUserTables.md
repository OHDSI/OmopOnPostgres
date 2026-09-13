# Get tables created by a user in a schema.

Get tables created by a user in a schema.

## Usage

``` r
getUserTables(src, schema = "public", user = NULL)
```

## Arguments

- src:

  It can either be a cdm_reference, a postgres_source or a DBI
  connection object.

- schema:

  Character. The schema to search in.

- user:

  Character. Users to filter by. If NULL no filter is applied.

## Value

Tibble with the identified tables.
