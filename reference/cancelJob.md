# Cancel a Postgres job.

Cancel a Postgres job.

## Usage

``` r
cancelJob(src, pid)
```

## Arguments

- src:

  It can either be a cdm_reference, a postgres_source or a PqConnection
  object.

- pid:

  Numeric. The pid to cancel, multiple can be supplied.

## Value

Invisible TRUE if the process is successful.
