
createSchema <- function(con, schema) {
  st <- paste0("CREATE SCHEMA ", schema)
  is_dbc <- inherits(con, "DatabaseConnectorConnection") || inherits(con, "DatabaseConnectorDbiConnection")

  if (is_dbc) {
    DatabaseConnector::executeSql(
      connection = con,
      sql = st,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  } else {
    DBI::dbExecute(conn = con, statement = st)
  }
  invisible(con)
}

deleteSchema <- function(con, schema) {
  st <- paste0("DROP SCHEMA ", schema)
  is_dbc <- inherits(con, "DatabaseConnectorConnection") || inherits(con, "DatabaseConnectorDbiConnection")

  if (is_dbc) {
    DatabaseConnector::executeSql(
      connection = con,
      sql = st,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  } else {
    DBI::dbExecute(conn = con, statement = st)
  }
  invisible(con)
}
schemaExists <- function(con, schema) {
  x <- dplyr::tbl(con, I("information_schema.schemata")) |>
    dplyr::filter(.data$schema_name %in% .env$schema) |>
    dplyr::collect()
  nrow(x) > 0
}
question <- function(message, .envir = parent.frame()) {
  if (!rlang::is_interactive()) return(TRUE)
  res <- ""
  while (!res %in% c("yes", "no")) {
    cli::cli_inform(message = message, .envir = .envir)
    res <- tolower(readline())
    res[res == "y"] <- "yes"
    res[res == "n"] <- "no"
  }
  res == "yes"
}
