for (drv in get_test_drivers()) {

  test_that(sprintf("benchmark using %s", drv), {
    skip_on_cran()
    withr::defer(resetSchemas())
    cdm <- mockPostgresCdmReference(client = drv)
    expect_no_error(benchmarkOmopOnPostgres(cdm, n_iterations = 5))
    cdmDisconnect(cdm)
  })

}
