# Set up your environment to contribute

## Set up

In this vignette we explain how to set up your environment to be able to
contribute to the package. **OmopOnPostgres** is an open-source package
open for contributions, if you want to contribute you will need to be
able to run the tests locally. To do so you need a local postgres
instance with the following credentials:

    POSTGRES_USER=omop_postgres_connector
    POSTGRES_PASSWORD=omopverse
    POSTGRES_DB=omop_test

The GitHub actions run on a container where these
[credentials](https://github.com/oxford-pharmacoepi/ohdsi/blob/main/.github/workflows/postgres.yml#L13-L15)
have already been created, you can rely on the GitHub actions to run
tests there.

There are several ways to have this:

### Install Postgres and create manually a database

1.  [Download postgres](https://www.postgresql.org/download/)
2.  Set up your installer and install Postgre and pgAdmin 4. Please use
    PORT 5432, if port 5432 is not available create an environment
    variable `OMOP_POSTGRES_CONNECTOR_PORT` with the port that you are
    using:

&nbsp;

    OMOP_POSTGRES_CONNECTOR_PORT=5432

3.  Create your a user. Please use USER: omop_postgres_connector and
    PASSWORD: omopverse; if this is not possible please create
    environment variables to provide the credentials:

&nbsp;

    OMOP_POSTGRES_CONNECTOR_USER=omop_postgres_connector
    OMOP_POSTGRES_CONNECTOR_PASSWORD=omopverse

4.  Create a database. Please use DATABASE: omop_test; if this is not
    possible please create environment variables to provide the
    credentials:

&nbsp;

    OMOP_POSTGRES_CONNECTOR_DB=omop_test

If you successfully completed the 4 steps please go to [Check the set
up](https://ohdsi.github.io/OmopOnPostgres/articles/add%20link).

### Install Postgres via docker

1.  [Download docker
    desktop](https://www.docker.com/products/docker-desktop)

2.  Run the following code from the terminal:

&nbsp;

    docker run --name local-postgres \
      -e POSTGRES_USER=omop_postgres_connector \
      -e POSTGRES_PASSWORD=omopverse \
      -e POSTGRES_DB=omop_test \
      -p 5432:5432 \
      -d postgres:16

## Check the set up

If your connection is successful you should be able to run the following
lines of code without any error:

``` r
library(DBI)
library(RPostgres)
library(dplyr)
#> 
#> Attaching package: 'dplyr'
#> The following objects are masked from 'package:stats':
#> 
#>     filter, lag
#> The following objects are masked from 'package:base':
#> 
#>     intersect, setdiff, setequal, union

# create connection
con <- dbConnect(
  drv = Postgres(),
  dbname = Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "omop_test"),
  host = "localhost",
  port = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
  user = Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", "omop_postgres_connector"),
  password = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", "omopverse")
)
con
#> <PqConnection> omop_test@localhost:5432

# create new schema
dbExecute(conn = con, statement = "CREATE SCHEMA opc_main;")
#> [1] 0
dbWriteTable(conn = con, name = Id(schema = "opc_main", table = "test"), value = cars)
tbl(con, I("opc_main.test"))
#> # Source:   table<opc_main.test> [?? x 2]
#> # Database: postgres  [omop_postgres_connector@localhost:5432/omop_test]
#>    speed  dist
#>    <dbl> <dbl>
#>  1     4     2
#>  2     4    10
#>  3     7     4
#>  4     7    22
#>  5     8    16
#>  6     9    10
#>  7    10    18
#>  8    10    26
#>  9    10    34
#> 10    11    17
#> # ℹ more rows
dbExecute(con, "DROP SCHEMA opc_main CASCADE;")
#> NOTICE:  drop cascades to table opc_main.test
#> [1] 0

# create new database
dbExecute(conn = con, statement = "CREATE DATABASE opc_test;")
#> [1] 0
dbDisconnect(conn = con)

# connect to new database
con <- dbConnect(
  drv = Postgres(),
  dbname = "opc_test",
  host = "localhost",
  port = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
  user = Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", "omop_postgres_connector"),
  password = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", "omopverse")
)

dbExecute(conn = con, statement = "CREATE SCHEMA opc_main;")
#> [1] 0
dbWriteTable(conn = con, name = Id(schema = "opc_main", table = "test"), value = cars)
tbl(con, I("opc_main.test"))
#> # Source:   table<opc_main.test> [?? x 2]
#> # Database: postgres  [omop_postgres_connector@localhost:5432/opc_test]
#>    speed  dist
#>    <dbl> <dbl>
#>  1     4     2
#>  2     4    10
#>  3     7     4
#>  4     7    22
#>  5     8    16
#>  6     9    10
#>  7    10    18
#>  8    10    26
#>  9    10    34
#> 10    11    17
#> # ℹ more rows
dbDisconnect(conn = con)

# delete created database
con <- dbConnect(
  drv = Postgres(),
  dbname = Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "omop_test"),
  host = "localhost",
  port = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
  user = Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", "omop_postgres_connector"),
  password = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", "omopverse")
)
dbExecute(con, "DROP DATABASE opc_test;")
#> [1] 0
dbDisconnect(conn = con)
```

Now that you are ready try cloning the code locally and run
`devtools::check()` if no error arises you are all set up.
