
dropCdm <- function(cdm) {
  src <- omopgenerics::cdmSource(x = cdm)

  # get tables
  x <- cdmTableClasses(cdm = cdm)

  # drop cdm tables
  dropTable(src = src, type = "cdm", name = x$omop_tables)

  # drop write tables
  cohort_tables <- x$cohort_tables |>
    purrr::map(\(x) paste0(x, c("", "_set", "_attrition", "_codelist"))) |>
    unlist()
  dropTable(src = src, type = "write", name = c(cohort_tables, x$other_tables))

  # drop achilles tables
  dropTable(src = src, type = "achilles", name = x$achilles_tables)
}
deleteAllTables <- function() {
  con <- localPostgres()
  is_dbc <- inherits(con, "DatabaseConnectorConnection") || inherits(con, "DatabaseConnectorDbiConnection")

  st <- "SELECT schemaname, tablename FROM pg_tables WHERE schemaname NOT IN ('pg_catalog', 'information_schema');"

  if (is_dbc) {
    tables <- DatabaseConnector::querySql(connection = con, sql = st)
    names(tables) <- tolower(names(tables))
  } else {
    tables <- DBI::dbGetQuery(conn = con, statement = st)
  }

  if (nrow(tables) > 0) {
    for (i in seq_len(nrow(tables))) {
      drop_st <- sprintf('DROP TABLE IF EXISTS "%s"."%s" CASCADE;', tables$schemaname[i], tables$tablename[i])

      if (is_dbc) {
        DatabaseConnector::executeSql(
          connection = con,
          sql = drop_st,
          progressBar = FALSE,
          reportOverallTime = FALSE
        )
      } else {
        DBI::dbExecute(conn = con, statement = drop_st)
      }
    }
  }

  DBI::dbDisconnect(conn = con)
}
