test_that("test you can create empty table", {
  con <- localPostgres()

  allTables <- unique(omopgenerics::omopTableFields(cdmVersion = "5.4")$cdm_table_name)

  expect_no_error(createOmopTablesOnPostgres(con = con))


  expect_no_error(createOmopTablesOnPostgres(con = con, bigInt = TRUE))
  # check prefix
  # check other schema
  # check overwrite
  expect_no_error(createOmopTablesOnPostgres(con = con, bigInt = TRUE, overwrite = TRUE))

  DBI::dbDisconnect(conn = con)
  deleteAllTables()
})
