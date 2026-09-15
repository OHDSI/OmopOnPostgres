
#' Benchmark OMOP on Postgres
#'
#' @param cdm CDM reference
#' @param n_iterations Number of iterations to run
#'
#' @returns Benchmarking results
#' @export
#'
benchmarkOmopOnPostgres <- function(cdm, n_iterations = 1){

  rlang::check_installed("tictoc")
  rlang::check_installed("scales")
  tictoc::tic.clearlog()

  omopgenerics::validateCdmArgument(cdm = cdm)
  omopgenerics::assertNumeric(x = n_iterations,
                              length = 1,
                              min = 1)

  n_person <- cdm$person |>
    dplyr::tally() |>
    dplyr::pull("n") |>
    as.numeric() |>
    scales::comma()

  for(i in seq_along(1:n_iterations)){
  cli::cli_inform(paste0("Running benchmark iteration ", i))
  tictoc::tic(msg = paste0("Collecting person table (", n_person," rows): ", i))
  cdm$person |>
    dplyr::collect()
  tictoc::toc(log = TRUE)

  sample_df <- dplyr::tibble(
    id = 1:100000,
    text_col = paste0("Row_", 1:100000)
  )
  tmpName <- omopgenerics::uniqueTableName()

  tictoc::tic(msg = paste0("Inserting 100,000 row table with 2 columns: ", i))
  cdm <- omopgenerics::insertTable(cdm = cdm,
                            name = tmpName,
                            table = sample_df)

  tictoc::toc(log = TRUE)
  }

  times <- getTimes(tictoc::tic.log(format = FALSE),
                    cdm = cdm)

  times
}


getTimes <- function(log, cdm) {
    log |>
      purrr::map_df(~dplyr::as_tibble(.x)) |>
      dplyr::distinct() |>
      dplyr::mutate(
        estimate_value = as.character(round((as.numeric(.data$toc) - as.numeric(.data$tic)), 2)),
      ) |>
      dplyr::select(-dplyr::all_of(c("tic", "toc", "callback_msg"))) |>
      dplyr::rename("variable_name" = "msg") |>
      tidyr::separate_wider_delim(
        cols = "variable_name",
        delim = ": ",
        names = c("variable_name", "group_level"),
      ) |>
      dplyr::mutate(group_name = "iteration") |>
      dplyr::arrange(.data$variable_name) |>
      dplyr::mutate(
        variable_level = "overall",
        variable_name = dplyr::if_else(
          grepl("Cohort set", .data$variable_name), "Cohort set", .data$variable_name
        ),
        cdm_name = omopgenerics::cdmName(cdm),
        result_id = 1L,
        estimate_name = "time_seconds",
        estimate_type = "numeric"
      ) |>
      omopgenerics::uniteStrata() |>
      omopgenerics::uniteAdditional() |>
      omopgenerics::newSummarisedResult(
        settings = dplyr::tibble(
          result_id = 1L,
          result_type = "benchmark",
          package_name = "OmopOnPostgres",
          package_version = as.character(utils::packageVersion("OmopOnPostgres"))
        )
      )

}
