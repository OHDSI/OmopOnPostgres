
#' Check if a PostgreSQL database is attached to DuckDB
#'
#' @param con DuckDb connection
#'
#' @returns TRUE if postgres database attached, otherwise FALSE
#' @export
#'
duckdbHasPgDbAttached <- function(con){
  query <- "SELECT COUNT(*) FROM duckdb_databases() WHERE type = 'postgres';"
  pg_count <- DBI::dbGetQuery(con, query)[[1]]
  return(!is.na(pg_count) && pg_count > 0)
}

#' Check if duckdb is using Postgres as default catalog
#'
#' @param con DuckDb connection
#'
#' @returns TRUE if using Postgres as default catalog, otherwise FALSE
#' @export
#'
duckdbIsUsingPgDb <- function(con) {

  query <- "
    SELECT type
    FROM duckdb_databases()
    WHERE database_name = current_database();
  "
  db_type <- DBI::dbGetQuery(con, query)[[1]]
  if (length(db_type) > 0 && !is.na(db_type) && db_type == "postgres") {
    return(TRUE)
  } else {
    return(FALSE)
  }
}

#' Ensure duckdb is using Postgres as default catalog
#'
#' @param con DuckDb connection
#'
#' @returns DuckDb connection that uses Postgres as default catalog
#' @export
#'
duckdbUsePgDb <- function(con) {
  if(isFALSE(duckdbHasPgDbAttached(con))){
    cli::cli_abort("DuckDb database does not have a postgres database attached")
  }

  if(isTRUE(duckdbIsUsingPgDb(con))){
    return(con)
  }

  DBI::dbExecute(con, "USE pg_db;")
  return(invisible(con))
}

#' Ensure duckdb is using the native DuckDB catalog
#'
#' @param con DuckDb connection
#'
#' @returns DuckDb connection that uses native DuckDB as default catalog
#' @export
#'
duckdbUseDuckDb <- function(con) {
  query <- "SELECT database_name FROM duckdb_databases() WHERE type = 'duckdb' LIMIT 1;"
  duck_db_name <- DBI::dbGetQuery(con, query)[[1]]

  if (length(duck_db_name) == 0 || is.na(duck_db_name)) {
    cli::cli_abort("No native DuckDB database found attached to this connection")
  }
  current_db <- DBI::dbGetQuery(con, "SELECT current_database();")[[1]]
  if (!is.na(current_db) && current_db == duck_db_name) {
    return(con)
  }
  DBI::dbExecute(con, sprintf("USE %s;", duck_db_name))
  return(invisible(con))
}

#' Attach a postres database to a duckdb database
#'
#' @param con DuckDb connection
#' @param uri URI for the postgres database
#'
#' @returns DuckDb connection with postgres database attached
#' @export
#'
attachPgDb <- function(con, uri){

  if(isTRUE(duckdbHasPgDbAttached(con))){
    return(con)
  }

  DBI::dbExecute(con, "INSTALL postgres;")
  DBI::dbExecute(con, "LOAD postgres;")
  attach_query <- sprintf("ATTACH '%s' AS pg_db (TYPE postgres);", uri)
  DBI::dbExecute(con, attach_query)
  DBI::dbExecute(con, "USE pg_db;")

  return(con)

}
