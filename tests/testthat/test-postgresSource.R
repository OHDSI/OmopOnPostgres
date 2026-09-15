for (drv in get_test_drivers()) {

  test_that(sprintf("check postgres source using %s", drv), {
  skip_on_cran()

  # local connection works
  expect_no_error(con <- localPostgres(client = drv))

  # create postgres source
  expect_no_error(src <- postgresSource(
    con = con,
    cdmPrefix = "pg_cdm",
    writePrefix = "pg_write",
    achillesSchema = "results",
    achillesPrefix = "pg_achilles"
  ))

  # copy cdm to postgres
  cdm <- omock::mockCdmFromDataset(datasetName = "GiBleed") |>
    omock::mockCohort() |>
    omopgenerics::emptyAchillesTable(name = "achilles_analysis") |>
    omopgenerics::emptyAchillesTable(name = "achilles_results") |>
    omopgenerics::emptyAchillesTable(name = "achilles_results_dist")
  cdm$my_random_table <- dplyr::tibble(person_id = 1L, value = "xyz")
  expect_no_error(pq_cdm <- insertCdmTo(cdm = cdm, to = src))

  # summary
  expect_no_error(summ <- summary(omopgenerics::cdmSource(x = pq_cdm)))
  expect_true(length(summ$work_mem) == 1)

  # disconnect
  expect_no_error(dropCdm(cdm = pq_cdm))
})

}
