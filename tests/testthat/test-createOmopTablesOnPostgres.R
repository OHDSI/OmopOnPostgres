test_that("test you can create empty table", {
  # delete tables
  deleteAllTables()

  # create connection
  con <- localPostgres()

  # all tables to be created
  allTables <- omopgenerics::omopTableFields(cdmVersion = "5.4") |>
    dplyr::filter(.data$type == "cdm_table") |>
    dplyr::pull("cdm_table_name") |>
    unique() |>
    sort()
  expect_identical(
    listTablesPostgres(con = con, schema = "public", prefix = "") |>
      sort(),
    character()
  )

  # create tables
  expect_no_error(createOmopTablesOnPostgres(con = con))
  expect_identical(
    listTablesPostgres(con = con, schema = "public", prefix = "") |>
      sort(),
    allTables
  )
  expect_false(
    dplyr::tbl(
      src = con,
      I(formatNamePostgres(schema = "public", prefix = "", name = "person"))
    ) |>
      dplyr::pull("person_id") |>
      bit64::is.integer64()
  )

  expect_no_error(createOmopTablesOnPostgres(con = con, bigInt = TRUE))
  expect_identical(
    listTablesPostgres(con = con, schema = "public", prefix = "") |>
      sort(),
    allTables
  )
  expect_false(
    dplyr::tbl(
      src = con,
      I(formatNamePostgres(schema = "public", prefix = "", name = "person"))
    ) |>
      dplyr::pull("person_id") |>
      bit64::is.integer64()
  )

  # check overwrite
  expect_no_error(createOmopTablesOnPostgres(con = con, bigInt = TRUE, overwrite = TRUE))
  expect_identical(
    listTablesPostgres(con = con, schema = "public", prefix = "") |>
      sort(),
    allTables
  )
  expect_true(
    dplyr::tbl(
      src = con,
      I(formatNamePostgres(schema = "public", prefix = "", name = "person"))
    ) |>
      dplyr::pull("person_id") |>
      bit64::is.integer64()
  )

  # check prefix
  prefix <- "test_"
  expect_no_error(createOmopTablesOnPostgres(con = con, cdmPrefix = prefix))
  expect_identical(
    listTablesPostgres(con = con, schema = "public", prefix = prefix) |>
      sort(),
    allTables
  )
  expect_false(
    dplyr::tbl(
      src = con,
      I(formatNamePostgres(schema = "public", prefix = prefix, name = "person"))
    ) |>
      dplyr::pull("person_id") |>
      bit64::is.integer64()
  )

  # check other schema
  schema <- "my_test"
  expect_no_error(createOmopTablesOnPostgres(con = con, cdmSchema = schema))
  expect_identical(
    listTablesPostgres(con = con, schema = schema, prefix = "") |>
      sort(),
    allTables
  )
  expect_false(
    dplyr::tbl(
      src = con,
      I(formatNamePostgres(schema = schema, prefix = "", name = "person"))
    ) |>
      dplyr::pull("person_id") |>
      bit64::is.integer64()
  )

  DBI::dbDisconnect(conn = con)
  deleteAllTables()
})
