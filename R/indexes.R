
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

  # table classes
  expected <- omopgenerics::cdmClasses(cdm = cdm) |>
    purrr::map(\(x) dplyr::tibble(table_name = x)) |>
    dplyr::bind_rows(.id = "table_class") |>
    dplyr::filter(.data$table_name %in% .env$name) |>
    dplyr::group_by(.data$table_class) |>
    dplyr::group_split() |>
    purrr::map(\(x) {
      cl <- unique(x$table_class)
      if (cl == "omop_table") {
        res <- 2
      } else if (cl == "cohort_table") {
        res <- 1
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

  # get schema and table names
  x <- getSchemaAndTables(cdm = cdm, name = name)

  # insert table
  nm <- omopgenerics::uniqueTableName()
  cdm <- omopgenerics::insertTable(cdm = cdm, name = nm, table = x)

  # filter data
  indexes <- dplyr::tbl(con, I("pg_indexes")) |>
    dplyr::inner_join(cdm[[nm]], by = c("schemaname", "tablename")) |>
    dplyr::collect() |>
    dplyr::mutate(index = stringr::str_extract(
      string = .data$indexdef, pattern = "(?<=\\()[^)]*(?=\\))"
    )) |>
    dplyr::select("table_name" = "tablename", "index")

  # drop table
  omopgenerics::dropSourceTable(cdm = cdm, name = nm)

  # table classes
  classes <- omopgenerics::cdmClasses(cdm = cdm) |>
    purrr::map(\(x) dplyr::tibble(table_name = x)) |>
    dplyr::bind_rows(.id = "table_class")

  # order result
  indexes <- indexes |>
    dplyr::inner_join(classes, by = "table_name") |>

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

existingCdmIndexes <- function(cdm) {
  # initial check
  omopgenerics::validateCdmArgument(cdm = cdm)

  # get src
  src <- omopgenerics::cdmSource(cdm)
  con <- getCon(src)
  x <- cdmTableClasses(cdm = cdm)

  # get cdm_schema indexes
  schema <- getSchema(src, "cdm")
  nms <- paste0(getPrefix(src, "cdm"), x$omop_tables)
  idx_cdm <- getIndexes(con = con, schema = schema, table = nms)

  # get write_schema indexes
  schema <- getSchema(src, "write")
  nms <- paste0(getPrefix(src, "write"), c(x$cohort_tables, x$other_tables))
  idx_write <- getIndexes(con = con, schema = schema, table = nms)

  # get achilles_schema indexes
  schema <- getSchema(src, "achilles")
  nms <- paste0(getPrefix(src, "write"), x$achilles_tables)
  idx_achilles <- getIndexes(con = con, schema = schema, table = nms)

  dplyr::bind_rows(idx_cdm, idx_write, idx_achilles)
}
existingTableIndexes <- function(table) {
  omopgenerics::validateCdmTable(table = table)
  rnm <- dbplyr::remote_name(table)

  # check table is not a query
  if (is.null(rnm)) {
    cli::cli_inform(c("!" = "Table is query and not a reference to a table."))
    return(emptyIndexesMatrix())
  }

  # check is not temp table
  if (is.na(omopgenerics::tableName(table))) {
    cli::cli_inform(c("!" = "Temp tables do not have indexes."))
    return(emptyIndexesMatrix())
  }

  src <- omopgenerics::cdmSource(table)
  con <- getCon(src)
  if (stringr::str_detect(string = rnm, pattern = "\\.")) {
    rnm <- stringr::str_split_1(string = rnm, pattern = "\\.")
    schema <- rnm[1]
    name <- rnm[2]
  } else {
    schema <- DBI::dbGetQuery(con, "SELECT current_schema();")$current_schema
    name <- rnm
  }
  return(getIndexes(con = con, schema = schema, table = name))
}
existingIndexes <- function(x, schema = NULL, tableName = NULL) {
  # input check
  if (inherits(x, "cdm_reference") | inherits(x, "cdm_table")) {
    x <- omopgenerics::cdmSource(x)
  }
  if (inherits(x, "pq_cdm")) {
    x <- getCon(x)
  }
  if (!inherits(x, "PqConnection")) {
    cli::cli_abort(c(x = "{.cls PqConnection} not found."))
  }
  con <- assertCon(x)
  omopgenerics::assertCharacter(schema, null = TRUE)
  omopgenerics::assertCharacter(tableName, null = TRUE)

  # get indexes
  getIndexes(con = con, schema = schema, table = tableName)
}
expectedCdmIndexes <- function(cdm) {
  # input check
  cdm <- omopgenerics::validateCdmArgument(cdm = cdm)

  x <- cdmTableClasses(cdm = cdm)

  expectedIndex(tableName = x$omop_tables, tableClass = "omop_table")
}
expectedIndex <- function(tableName = NULL, tableClass, columns = NULL) {
  expIdx <- expectedIdx |>
    dplyr::filter(.data$table_class %in% .env$tableClass)
  if (tableClass %in% c("omop_table", "achilles_table")) {
    expIdx <- expIdx |>
      dplyr::filter(.data$index_table == .env$tableName)
  } else if (tableClass == "cohort_table") {

  } else if (tableClass == "other_table") {

  }
  expIdx |>
    dplyr::select("index_name", "index_table", "index_column" = "index")
}
emptyIndexesMatrix <- function() {
  c("schemaname", "tablename", "indexname", "tablespace", "indexdef", "index") |>
    rlang::set_names() |>
    as.list() |>
    dplyr::as_tibble() |>
    utils::head(0)
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
