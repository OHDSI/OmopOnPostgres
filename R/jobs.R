
#' Get ongoing Postgres jobs.
#'
#' @param src It can either be a cdm_reference, a postgres_source or a
#' PqConnection object.
#' @param user Users to filter by. If NULL no filter is applied.
#'
#' @return Tibble with the identified jobs.
#' @export
# library(DBI)
# library(RPostgres)
# library(OmopOnPostgres)
#
# con <- dbConnect(
#   drv = Postgres(),
#   dbname = Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "omop_test"),
#   host = "localhost",
#   port = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
#   user = Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", "omop_postgres_connector"),
#   password = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", "omopverse")
#   )
# getJobs(con)
#
# cdm <- cdmFromPostgres(con = con)
# getJobs(cdm)
#
getJobs <- function(src, user = NULL) {
  UseMethod("getJobs")
}

#' @export
getJobs.cdm_reference <- function(src, user = NULL) {
  getJobs(src = omopgenerics::cdmSource(x = src), user  = user)
}

#' @export
getJobs.pq_cdm <- function(src, user = NULL) {
  getJobs(src = getCon(src = src), user = user)
}

#' @export
getJobs.PqConnection <- function(src, user = NULL) {
  omopgenerics::assertCharacter(user, null = TRUE)

  rlang::local_options(nanoarrow.warn_unregistered_extension = FALSE)

  x <- dplyr::tbl(src, I("pg_stat_activity"))

  if (!is.null(user)) {
    x <- x |>
      dplyr::filter(.data$usename %in% .env$user)
  }

  dplyr::collect(x)
}

#' @export
getJobs.AdbiConnection <- getJobs.PqConnection

#' @export
getJobs.PostgreSQL <- getJobs.PqConnection

#' @export
getJobs.DatabaseConnectorJdbcConnection <- getJobs.PqConnection

#' Cancel a Postgres job.
#'
#' @param src It can either be a cdm_reference, a postgres_source or a
#' PqConnection object.
#' @param pid Numeric. The pid to cancel, multiple can be supplied.
#'
#' @return Invisible TRUE if the process is successful.
#' @export
#'
cancelJob <- function(src, pid) {
  UseMethod("cancelJob")
}

#' @export
cancelJob.cdm_reference <- function(src, pid) {
  cancelJob(src = omopgenerics::cdmSource(x = src), pid = pid)
}

#' @export
cancelJob.pq_cdm <- function(src, pid) {
  cancelJob(src = getCon(src = src), pid = pid)
}

#' @export
#' @export
cancelJob.PqConnection <- function(src, pid) {
  omopgenerics::assertNumeric(pid, integerish = TRUE)
  pids <- unique(pid)

  active_pids <- DBI::dbGetQuery(src, "SELECT pid FROM pg_stat_activity")$pid

  # Separate valid and invalid PIDs
  valid_pids <- intersect(pids, active_pids)
  invalid_pids <- setdiff(pids, active_pids)
  if (length(invalid_pids) > 0) {
    cli::cli_inform("The following PIDs are not active and will be skipped: {.pkg {invalid_pids}}")
  }
  for (p in valid_pids) {
    cli::cli_inform(c(i = "Cancelling job with `pid = {.pkg {p}}`."))
    statement <- paste0("SELECT pg_cancel_backend(", p, ")")
    DBI::dbExecute(conn = src, statement = statement)
  }

  invisible(TRUE)
}

#' @export
cancelJob.AdbiConnection <- cancelJob.PqConnection

#' @export
cancelJob.PostgreSQL <- cancelJob.PqConnection

#' @export
cancelJob.DatabaseConnectorJdbcConnection <- cancelJob.PqConnection



#' Get tables created by a user in a schema.
#'
#' @param src It can either be a cdm_reference, a postgres_source or a
#' DBI connection object.
#' @param schema Character. The schema to search in.
#' @param user Character. Users to filter by. If NULL no filter is applied.
#'
#' @return Tibble with the identified tables.
#' @export
getUserTables <- function(src, schema = "public", user = NULL) {
  UseMethod("getUserTables")
}

#' @export
getUserTables.cdm_reference <- function(src, schema = "public", user = NULL) {
  getUserTables(src = omopgenerics::cdmSource(x = src), schema = schema, user = user)
}

#' @export
getUserTables.pq_cdm <- function(src, schema = "public", user = NULL) {
  getUserTables(src = getCon(src = src), schema = schema, user = user)
}

#' @export
getUserTables.PqConnection <- function(src, schema = "public", user = NULL) {
  omopgenerics::assertCharacter(schema, length = 1)
  omopgenerics::assertCharacter(user, null = TRUE)

  rlang::local_options(nanoarrow.warn_unregistered_extension = FALSE)

  x <- dplyr::tbl(src, I("pg_tables")) |>
    dplyr::filter(.data$schemaname == .env$schema)

  if (!is.null(user)) {
    x <- x |>
      dplyr::filter(.data$tableowner %in% .env$user)
  }

  dplyr::collect(x) |>
    dplyr::pull("tablename")
}

#' @export
getUserTables.AdbiConnection <- getUserTables.PqConnection

#' @export
getUserTables.PostgreSQL <- getUserTables.PqConnection

#' @export
getUserTables.OdbcConnection <- getUserTables.PqConnection

#' @export
getUserTables.DatabaseConnectorJdbcConnection <- getUserTables.PqConnection
