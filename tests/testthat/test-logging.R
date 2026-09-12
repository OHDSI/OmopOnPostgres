for (drv in get_test_drivers()) {

  test_that(sprintf("logging works using %s", drv), {
  skip_on_cran()
  expect_message(readPostgresLog())
  folder <- file.path(tempdir(), "logging")
  expect_no_error(postgresLog(path = folder, sql = T, explain = T, analyse = T))
  con <- localPostgres(client = drv)
  expect_no_error(src <- postgresSource(
    con = con, cdmPrefix = "log_cdm", writePrefix = "log_write"
  ))

  expect_true(length(list.files(path = folder)) >= 6)
  expect_no_error(logInfo <- readPostgresLog())
  expect_message(postgresLog(NULL, F, F, F))
  DBI::dbDisconnect(conn = con)
})
}
