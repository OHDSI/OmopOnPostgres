for (drv in get_test_drivers()) {

  test_that(sprintf("create schema using %s", drv), {
  skip_on_cran()
  withr::defer(resetSchemas())
  con <- localPostgres(client = drv)
  expect_false(schemaExists(con = con, schema = "test_schema"))
  expect_no_error(createSchema(con = con, schema = "test_schema"))
  expect_true(schemaExists(con = con, schema = "test_schema"))
  expect_false(schemaExists(con = con, schema = "test_schema"))
  DBI::dbDisconnect(conn = con)
})

}
