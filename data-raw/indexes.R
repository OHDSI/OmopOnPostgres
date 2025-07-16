
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

vocabs <- c("5.3", "5.4")

ids <- vocabs |>
  purrr::map(\(v) {
    omopgenerics::omopTables(v) |>
      purrr::map(\(tab) {
        omopgenerics::omopColumns(table = tab, field = "unique_id", version = v)
      })
  }) |>
  unlist() |>
  unique()
ids <- c(
  "subject_id", "number_records", "number_subjects", "excluded_records",
  "excluded_subjects", ids
)

postgresDatatypes <- vocabs |>
  rlang::set_names() |>
  purrr::map(\(v) {
    omopgenerics::omopTableFields(cdmVersion = v) |>
      dplyr::mutate(cdm_datatype = dplyr::case_when(
        .data$cdm_datatype == "logical" ~ "boolean",
        .data$cdm_datatype == "datetime" ~ "timestamp",
        .data$cdm_datatype == "float" ~ "numeric",
        .data$cdm_datatype == "varchar(max)" ~ "TEXT",
        .data$cdm_field_name %in% .env$ids ~ "bigint",
        .default = .data$cdm_datatype
      ))
  })

usethis::use_data(expectedIdx, postgresDatatypes, internal = TRUE, overwrite = TRUE)
