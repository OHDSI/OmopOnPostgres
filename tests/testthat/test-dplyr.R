for (drv in get_test_drivers()) {

  test_that(sprintf("copyCdmToPostgres works using %s", drv), {
    skip_on_cran()
    withr::defer(resetSchemas())

    local_cdm <- omock::mockCdmFromDataset()
    postgres_cdm <- mockPostgresCdmReference(drv)

    # collect and compute (to temp or permanent)
    expect_equal(local_cdm$person |>
                   dplyr::arrange(person_id) |>
                   dplyr::pull(person_id),
                 postgres_cdm$person |>
                   dplyr::collect()  |>
                   dplyr::arrange(person_id) |>
                   dplyr::pull(person_id))
    expect_equal(local_cdm$person |>
                   dplyr::arrange(person_id) |>
                   dplyr::pull(person_id),
                 postgres_cdm$person  |>
                   dplyr::compute(temporary = TRUE) |>
                   dplyr::collect()  |>
                   dplyr::arrange(person_id) |>
                   dplyr::pull(person_id))
    newTbl <- omopgenerics::uniqueTableName()
    expect_equal(local_cdm$person |>
                   dplyr::arrange(person_id) |>
                   dplyr::pull(person_id),
                 postgres_cdm$person |>
                   dplyr::compute(temporary = FALSE,
                                  name = newTbl) |>
                   dplyr::collect() |>
                   dplyr::arrange(person_id) |>
                   dplyr::pull(person_id))
    dropSourceTable(cdm = postgres_cdm, name = newTbl)

    # count records
    expect_equal(local_cdm$person |>
                   dplyr::tally() |>
                   dplyr::mutate(n = as.integer(n)) |>
                   dplyr::pull("n"),
                 postgres_cdm$person |>
                   dplyr::tally()|>
                   dplyr::collect() |>
                   dplyr::mutate(n = as.integer(n)) |>
                   dplyr::pull("n"))
    expect_equal(local_cdm$person |>
                   dplyr::summarise(n = dplyr::n()) |>
                   dplyr::mutate(n = as.integer(n)) |>
                   dplyr::pull("n"),
                 postgres_cdm$person |>
                   dplyr::summarise(n = dplyr::n()) |>
                   dplyr::collect() |>
                   dplyr::mutate(n = as.integer(n)) |>
                   dplyr::pull("n"))


    # filter
    expect_equal(local_cdm$person |>
                   dplyr::filter(gender_concept_id == 8532) |>
                   dplyr::arrange(person_id) |>
                   dplyr::pull("person_id"),
                 postgres_cdm$person |>
                   dplyr::filter(gender_concept_id == 8532) |>
                   dplyr::collect()|>
                   dplyr::arrange(person_id) |>
                   dplyr::pull("person_id"))
    # mutate
    expect_equal(local_cdm$person |>
                   dplyr::mutate(new_variable = "a")|>
                   dplyr::arrange(person_id) |>
                   dplyr::pull("person_id"),
                 postgres_cdm$person |>
                   dplyr::mutate(new_variable = "a") |>
                   dplyr::collect()|>
                   dplyr::arrange(person_id) |>
                   dplyr::pull("person_id"))
    # select
    expect_equal(local_cdm$person |>
                   dplyr::select("person_id") |>
                   dplyr::arrange(person_id) |>
                   dplyr::pull("person_id"),
                 postgres_cdm$person |>
                   dplyr::select("person_id") |>
                   dplyr::collect()|>
                   dplyr::arrange(person_id) |>
                   dplyr::pull("person_id"))

    # count distinct records
    expect_equal(local_cdm$person  |>
                   dplyr::distinct() |>
                   dplyr::tally() |>
                   dplyr::mutate(n = as.integer(n)) |>
                   dplyr::pull("n"),
                 postgres_cdm$person |>
                   dplyr::distinct() |>
                   dplyr::tally()|>
                   dplyr::collect() |>
                   dplyr::mutate(n = as.integer(n)) |>
                   dplyr::pull("n"))
    expect_equal(local_cdm$person  |>
                   dplyr::distinct() |>
                   dplyr::summarise(n = dplyr::n()) |>
                   dplyr::mutate(n = as.integer(n)) |>
                   dplyr::pull("n"),
                 postgres_cdm$person |>
                   dplyr::distinct() |>
                   dplyr::summarise(n = dplyr::n()) |>
                   dplyr::collect() |>
                   dplyr::mutate(n = as.integer(n)) |>
                   dplyr::pull("n"))

    # join
    expect_equal(local_cdm$person  |>
      dplyr::semi_join(local_cdm$observation_period,
                        by = dplyr::join_by(person_id)) |>
      dplyr::semi_join(local_cdm$condition_occurrence,
                        by = dplyr::join_by(person_id)) |>
      dplyr::arrange(person_id) |>
      dplyr::pull("person_id"),
    postgres_cdm$person  |>
      dplyr::semi_join(postgres_cdm$observation_period,
                       by = dplyr::join_by(person_id)) |>
      dplyr::semi_join(postgres_cdm$condition_occurrence,
                       by = dplyr::join_by(person_id)) |>
      dplyr::collect() |>
      dplyr::arrange(person_id) |>
      dplyr::pull("person_id"))

    # show query
    expect_no_error(postgres_cdm$person  |>
      dplyr::semi_join(postgres_cdm$observation_period,
                       by = dplyr::join_by(person_id)) |>
      dplyr::show_query())
    # explain query
    if(drv != "adbc"){ # not supported by adbc
    expect_no_error(postgres_cdm$person  |>
      dplyr::semi_join(postgres_cdm$observation_period,
                       by = dplyr::join_by(person_id)) |>
      dplyr::explain())
    }

  }
  )
}











