drv <- get_test_drivers()[get_test_drivers() == "duckdb"]
# if(get_test_drivers() == "duckdb"){
test_that("duckdb utilities", {
    skip_on_cran()

    # empty duckdb
    con <- DBI::dbConnect(duckdb::duckdb(dbdir = ":memory:"))
    expect_false(duckdbIsUsingPgDb(con))
    expect_error(duckdbUsePgDb(con))
    uri_string <- sprintf(
      "postgresql://%s:%s@%s:%s/%s",
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", Sys.getenv("USER")),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", ""),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_HOST", "localhost"),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "postgres")
    )
    expect_no_error(attachPgDb(con, uri = uri_string))
    expect_true(duckdbIsUsingPgDb(con))
    DBI::dbDisconnect(con)

    # create connection (has pg attached)
    con <- localPostgres(client = drv)
    withr::defer(DBI::dbDisconnect(conn = con))
    expect_true(duckdbHasPgDbAttached(con))
    expect_true(duckdbIsUsingPgDb(con))
    expect_no_error(duckdbUsePgDb(con))
    expect_no_error(duckdbUseDuckDb(con))
    expect_false(duckdbIsUsingPgDb(con))
    expect_no_error(duckdbUsePgDb(con))
    expect_true(duckdbIsUsingPgDb(con))

  })



# }

