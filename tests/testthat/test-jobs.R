for (drv in get_test_drivers()) {

  test_that(sprintf("job management using %s", drv), {
  skip_on_cran()
  con <- localPostgres(client = drv)
  cdm <- omock::mockCdmFromDataset(datasetName = "GiBleed")
  pcdm <- copyCdmToPostgres(cdm = cdm, con = con, cdmPrefix = "job_c_", writePrefix = "job_w_")

  expect_no_error(jb <- getJobs(pcdm))
  expect_true(inherits(jb, "tbl"))
  expect_true(nrow(jb) > 0)

  user <- Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", "omop_postgres_connector")
  expect_no_error(getJobs(src = pcdm, user = user))

  expect_message(cancelJob(pcdm, 123456789))

  expect_no_error(tb <- getUserTables(pcdm, schema = "public"))
  expect_true(length(tb) > 0)

  user <- Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", "omop_postgres_connector")
  expect_no_error(getUserTables(src = pcdm, schema = "public", user = user))
  expect_no_error(tb_2 <- getUserTables(src = pcdm, schema = "public", user = "somebody_else"))
  expect_true(length(tb_2) == 0)

  dropCdm(pcdm)
  })

}
