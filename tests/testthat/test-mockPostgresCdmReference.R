for (drv in get_test_drivers()) {

  test_that(sprintf("mockPostgresCdmReference works using %s", drv), {
    skip_on_cran()
    expect_no_error(
      cdm <- mockPostgresCdmReference(client = drv)
    )
    expect_no_error(cdmDisconnect(cdm))

    })
}
