for (drv in get_test_drivers()) {

test_that(sprintf("test you can create empty table using %s", drv), {
  skip_on_cran()

  # delete tables
  deleteAllTables()

  # create connection
  con <- localPostgres(client = drv)
  withr::defer(DBI::dbDisconnect(conn = con))
  withr::defer(resetSchemas())

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
  fn_person <- formatNamePostgres(schema = "public", prefix = "", name = "person")
  expect_true(
    DBI::dbGetQuery(con, paste0("SELECT person_id FROM ", fn_person, " LIMIT 0")) |>
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
  fn_prefix_person <- formatNamePostgres(schema = "public", prefix = prefix, name = "person")
  expect_false(
    DBI::dbGetQuery(con, paste0("SELECT person_id FROM ", fn_prefix_person, " LIMIT 0")) |>
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
  fn_schema_person <- formatNamePostgres(schema = schema, prefix = "", name = "person")
  expect_false(
    DBI::dbGetQuery(con, paste0("SELECT person_id FROM ", fn_schema_person, " LIMIT 0")) |>
      dplyr::pull("person_id") |>
      bit64::is.integer64()
  )

  deleteAllTables()
  })

}
