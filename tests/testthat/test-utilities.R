for (drv in get_test_drivers()) {

  test_that(sprintf("create schema using %s", drv), {
  skip_on_cran()
  con <- localPostgres(client = drv)
  withr::defer(DBI::dbDisconnect(conn = con))
  withr::defer(resetSchemas())
  withr::defer(dropSchema(con, "test_schema"))
  dropSchema(con, "test_schema")
  expect_false(schemaExists(con = con, schema = "test_schema"))
  expect_no_error(createSchema(con = con, schema = "test_schema"))
  expect_true(schemaExists(con = con, schema = "test_schema"))
})

}
