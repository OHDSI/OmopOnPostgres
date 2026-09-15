
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

#' Reset Postgres schemas to a completely blank state
#'
#' @export
resetSchemas <- function() {
  con <- localPostgres()
  is_dbc <- inherits(con, "DatabaseConnectorConnection") || inherits(con, "DatabaseConnectorDbiConnection")
  is_duckdb <- inherits(con, "duckdb_connection")

  schemas_to_reset <- c("public", "results")

  for (schema in schemas_to_reset) {
      if (is_duckdb) {
      drop_sql <- sprintf("CALL postgres_execute('pg_db', 'DROP SCHEMA IF EXISTS %s CASCADE');", schema)
      create_sql <- sprintf("CALL postgres_execute('pg_db', 'CREATE SCHEMA IF NOT EXISTS %s');", schema)
    } else {
      drop_sql <- sprintf("DROP SCHEMA IF EXISTS %s CASCADE;", schema)
      create_sql <- sprintf("CREATE SCHEMA IF NOT EXISTS %s;", schema)
    }
    if (is_dbc) {
      DatabaseConnector::executeSql(con, drop_sql, progressBar = FALSE, reportOverallTime = FALSE)
      DatabaseConnector::executeSql(con, create_sql, progressBar = FALSE, reportOverallTime = FALSE)
    } else {
      DBI::dbExecute(con, drop_sql)
      DBI::dbExecute(con, create_sql)
    }

    cli::cli_inform(c("v" = "Successfully reset schema: {.pkg {schema}}"))
  }

  DBI::dbDisconnect(conn = con)
  invisible(TRUE)
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
