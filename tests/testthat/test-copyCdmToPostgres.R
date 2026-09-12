for (drv in get_test_drivers()) {

  test_that(sprintf("copyCdmToPostgres works using %s", drv), {
    skip_on_cran()

    cdm <- omock::mockCdmFromDataset(datasetName = "GiBleed")
    con <- localPostgres(client = drv)

    expect_no_error(pq_cdm <- copyCdmToPostgres(
      cdm = cdm,
      con = con,
      cdmPrefix = omopgenerics::tmpPrefix(),
      writePrefix = omopgenerics::tmpPrefix(),
      achillesPrefix = omopgenerics::tmpPrefix()
    ))
    expect_no_error(dropCdm(cdm = pq_cdm))
    DBI::dbDisconnect(con)
  })

}
