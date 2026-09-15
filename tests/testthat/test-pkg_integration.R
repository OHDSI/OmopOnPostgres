
for (drv in get_test_drivers()) {

  test_that(sprintf("copyCdmToPostgres works using %s", drv), {
    skip_on_cran()
    withr::defer(resetSchemas())
    postgres_cdm <- mockPostgresCdmReference(drv)
    expect_no_error(
      bench_cc <- CohortConstructor::benchmarkCohortConstructor(
        cdm = postgres_cdm,
        runCIRCE = FALSE)  |>
        suppressMessages() |>
        suppressWarnings()
    )

  })
  }
