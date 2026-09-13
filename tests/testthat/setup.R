
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

  statement <- "DO
    $$
    DECLARE
        r RECORD;
    BEGIN
        FOR r IN
            SELECT schemaname, tablename
            FROM pg_tables
            WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        LOOP
            EXECUTE format('DROP TABLE IF EXISTS %I.%I CASCADE;', r.schemaname, r.tablename);
        END LOOP;
    END
    $$;"

  is_dbc <- inherits(con, "DatabaseConnectorConnection") || inherits(con, "DatabaseConnectorDbiConnection")

  if (is_dbc) {
    DatabaseConnector::executeSql(
      connection = con,
      sql = statement,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  } else {
    DBI::dbExecute(
      conn = con,
      statement = statement
    )
  }

  DBI::dbDisconnect(conn = con)
}
