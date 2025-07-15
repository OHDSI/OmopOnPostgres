
#' @importFrom omopgenerics expectedIndexes
#' @export
omopgenerics::expectedIndexes

#' @importFrom omopgenerics existingIndexes
#' @export
omopgenerics::existingIndexes

#' @importFrom omopgenerics statusIndexes
#' @export
omopgenerics::statusIndexes

#' @importFrom omopgenerics createIndexes
#' @export
omopgenerics::createIndexes

#' @importFrom omopgenerics createTableIndex
#' @export
omopgenerics::createTableIndex

#' @export
expectedIndexes.pq_cdm <- function(cdm, name) {
  # convert back the cdm object
  class(cdm) <- "cdm_reference"

  # table indexes
  omopgenerics::cdmClasses(cdm = cdm) |>
    purrr::map(\(x) dplyr::tibble(table_name = x)) |>
    dplyr::bind_rows(.id = "table_class") |>
    dplyr::filter(.data$table_name %in% .env$name) |>
    dplyr::group_by(.data$table_class) |>
    dplyr::group_split() |>
    purrr::map(\(x) {
      cl <- unique(x$table_class)
      if (cl == "omop_table") {
        res <- x |>
          dplyr::inner_join(
            expectedIdx$omop_table |>
              dplyr::select("table_name", "expected_index"),
            by = "table_name"
          )
      } else if (cl == "cohort_table") {
        res <- x |>
          dplyr::cross_join(
            expectedIdx$cohort_table |>
              dplyr::select("table_name", "expected_index")
          )
      } else if (cl == "achilles_table") {
        res <- x |>
          dplyr::inner_join(
            expectedIdx$achilles_table |>
              dplyr::select("table_name", "expected_index"),
            by = "table_name"
          )
      } else {
        res <- dplyr::tibble(
          table_class = character(),
          table_name = character(),
          expected_index = character()
        )
      }
      return(res)
    }) |>
    dplyr::bind_rows()
}

#' @export
existingIndexes.pq_cdm <- function(cdm, name) {
  # convert back the cdm object
  class(cdm) <- "cdm_reference"

  # get connection
  src <- omopgenerics::cdmSource(x = cdm)
  con <- getCon(src = src)

  # get schema and table names
  x <- getSchemaAndTables(cdm = cdm, name = name)

  # insert table
  nm <- omopgenerics::uniqueTableName()
  cdm <- omopgenerics::insertTable(cdm = cdm, name = nm, table = x)

  # filter data
  indexes <- dplyr::tbl(con, I("pg_indexes")) |>
    dplyr::inner_join(cdm[[nm]], by = c("schemaname", "tablename")) |>
    dplyr::collect() |>
    dplyr::mutate(existing_index = stringr::str_extract(
      string = .data$indexdef, pattern = "(?<=\\()[^)]*(?=\\))"
    )) |>
    dplyr::select("table_name" = "tablename", "existing_index")

  # drop table
  omopgenerics::dropSourceTable(cdm = cdm, name = nm)

  # table classes
  classes <- omopgenerics::cdmClasses(cdm = cdm) |>
    purrr::map(\(x) dplyr::tibble(table_name = x)) |>
    dplyr::bind_rows(.id = "table_class")

  # order result
  indexes <- indexes |>
    dplyr::inner_join(classes, by = "table_name")

  # return
  return(indexes)
}

#' @export
createTableIndex.pq_cdm <- function(table, index) {
  # get attributes
  con <- dbplyr::remote_con(x = table)
  nm <- dbplyr::remote_name(x = table$lazy_query) |>
    as.character() |>
    stringr::str_split_1(pattern = "\\.")
  schema <- nm[1]
  table <- nm[2]

  # create index if it does not exist
  if (indexExists(con, schema, table, index)) {
    cli::cli_inform(c("!" = "Index already existing so no new index added."))
    res <- FALSE
  } else {
    st <- paste0("CREATE INDEX ON ", schema, ".", table, " (", index, ")")
    DBI::dbExecute(conn = con, statement = st)
    res <- TRUE
  }

  invisible(res)
}

getSchemaAndTables <- function(cdm, name) {
  name |>
    purrr::map(\(nm) {
      rnm <- as.character(dbplyr::remote_name(cdm[[nm]]$lazy_query))
      if (is.na(rnm)) {
        cli::cli_inform(c("!" = "cdm[['{nm}']] is a query and will be ignored."))
        x <- dplyr::tibble(schemaname = character(), tablename = character())
      } else {
        x <- stringr::str_split_1(string = rnm, pattern = "\\.") |>
          rlang::set_names(nm = c("schemaname", "tablename")) |>
          as.list() |>
          dplyr::as_tibble()
      }
      x
    }) |>
    dplyr::bind_rows()
}
getIndexes <- function(con, schema = NULL, table = NULL) {
  x <- dplyr::tbl(con, I("pg_indexes"))
  if (!is.null(schema)) {
    x <- x |>
      dplyr::filter(.data$schemaname %in% .env$schema)
  }
  if (!is.null(table)) {
    x <- x |>
      dplyr::filter(.data$tablename %in% .env$table)
  }
  x |>
    dplyr::collect() |>
    dplyr::mutate(index = stringr::str_extract(
      string = .data$indexdef, pattern = "(?<=\\()[^)]*(?=\\))"
    ))
}
indexExists <- function(con, schema, table, index) {
  index %in% getIndexes(con, schema, table)$index
}
