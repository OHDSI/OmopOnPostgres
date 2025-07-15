
cdmIndexes <- readLines("https://raw.githubusercontent.com/OHDSI/CommonDataModel/refs/heads/main/inst/ddl/5.4/postgresql/OMOPCDM_postgresql_5.4_indices.sql") |>
  purrr::keep(\(x) startsWith(x, "CREATE INDEX")) |>
  stringr::str_replace_all(pattern = " ASC", replacement = "") |>
  purrr::map(\(x) {
    dplyr::tibble(
      index_name = stringr::str_match(x, "CREATE INDEX ([^ ]+)")[,2],
      table_name = stringr::str_match(x, "\\.([^ ]+)")[,2],
      expected_index = stringr::str_match(x, "\\(([^)]+)\\)")[,2]
    )
  }) |>
  dplyr::bind_rows()

cohortIndexes <- dplyr::tibble(
  expected_index = "cohort_definition_id, subject_id, cohort_start_date"
)

achillesIndexes <- dplyr::tribble(
  ~table_name, ~expected_index,
  "achilles_results", "analysis_id",
  "achilles_results_dist", "analysis_id"
)

expectedIdx <- list(
  omop_table = cdmIndexes,
  cohort_table = cohortIndexes,
  achilles_table = achillesIndexes
)

usethis::use_data(expectedIdx, internal = TRUE, overwrite = TRUE)
