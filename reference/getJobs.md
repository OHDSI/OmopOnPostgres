# Get ongoing Postgres jobs.

Get ongoing Postgres jobs.

## Usage

``` r
getJobs(src, user = NULL)
```

## Arguments

- src:

  It can either be a cdm_reference, a postgres_source or a PqConnection
  object.

- user:

  Users to filter by. If NULL no filter is applied.

## Value

Tibble with the identified jobs.
