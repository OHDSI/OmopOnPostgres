
#' This function creates a connection to the local postgres instance
#'
#' It relays on environmental variables such as:
#' * dbname = `OMOP_POSTGRES_CONNECTOR_DB`
#' * host = `OMOP_POSTGRES_CONNECTOR_HOST`
#' * port = `OMOP_POSTGRES_CONNECTOR_PORT`
#' * user = `OMOP_POSTGRES_CONNECTOR_USER`
#' * password = `OMOP_POSTGRES_CONNECTOR_PASSWORD`
#'
#' @param client Client. Can be "RPostgres", "adbc", "odbc", or "DatabaseConnector".
#' @return A connection to your postgres local instance
#' @export
#'
#' @examples
#' \dontrun{
#' library(OmopOnPostgres)
#'
#' localPostgres()
#' }
localPostgres <- function(client = Sys.getenv("TEST_PG_DRIVER", "RPostgres")) {
  omopgenerics::assertCharacter(client, length = 1)

  if (client == "RPostgres") {

    DBI::dbConnect(
      drv = RPostgres::Postgres(),
      dbname = Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "postgres"),
      host = Sys.getenv("OMOP_POSTGRES_CONNECTOR_HOST", "localhost"),
      port = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
      user = Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", Sys.getenv("USER")),
      password = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", "")
    )

  } else if (client == "adbc") {

    uri_string <- sprintf(
      "postgresql://%s:%s@%s:%s/%s",
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", Sys.getenv("USER")),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", ""),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_HOST", "localhost"),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "postgres")
    )

    DBI::dbConnect(
      adbi::adbi("adbcpostgresql"),
      uri = uri_string,
      bigint = "integer64"
    )

  } else if (client == "odbc") {

    DBI::dbConnect(
      drv = odbc::odbc(),
      driver = Sys.getenv("OMOP_POSTGRES_ODBC_DRIVER", "PostgreSQL Unicode"),
      server = Sys.getenv("OMOP_POSTGRES_CONNECTOR_HOST", "localhost"),
      port = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
      database = Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "postgres"),
      uid = Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", Sys.getenv("USER")),
      pwd = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", "")
    )

  } else if (client == "DatabaseConnector") {

    server_string <- sprintf(
      "%s/%s",
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_HOST", "localhost"),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "postgres")
    )

    DatabaseConnector::connect(
      dbms = "postgresql",
      server = server_string,
      user = Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", Sys.getenv("USER")),
      password = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", ""),
      port = Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
      pathToDriver = Sys.getenv("DATABASECONNECTOR_JAR_FOLDER")
    )


  } else if (client == "duckdb") {

    uri_string <- sprintf(
      "postgresql://%s:%s@%s:%s/%s",
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_USER", Sys.getenv("USER")),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_PASSWORD", ""),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_HOST", "localhost"),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_PORT", "5432"),
      Sys.getenv("OMOP_POSTGRES_CONNECTOR_DB", "postgres")
    )

    con_duck <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
    DBI::dbExecute(con_duck, "CREATE SCHEMA IF NOT EXISTS public;")
    DBI::dbExecute(con_duck, "CREATE SCHEMA IF NOT EXISTS results;")
    DBI::dbExecute(con_duck, "INSTALL postgres;")
    DBI::dbExecute(con_duck, "LOAD postgres;")
    attach_query <- sprintf("ATTACH '%s' AS pg_db (TYPE postgres);", uri_string)
    DBI::dbExecute(con_duck, attach_query)
    DBI::dbExecute(con_duck, "USE pg_db;")

    con_duck

  } else {
    cli::cli_abort("{client} not supported")
  }
}

#' Create a postgres source object
#'
#' @inheritParams pqSourceDoc
#'
#' @return A <pq_cdm> source object
#' @export
#'
postgresSource <- function(con,
                           cdmSchema = "public",
                           cdmPrefix = "",
                           writeSchema = "results",
                           writePrefix = "",
                           achillesSchema = NULL,
                           achillesPrefix = "") {
  # input checks
  con <- validateCon(con = con)
  cdmSchema <- validateSchema(con = con, schema = cdmSchema, null = FALSE)
  cdmPrefix <- validatePrefix(prefix = cdmPrefix)
  writeSchema <- validateSchema(con = con, schema = writeSchema, null = FALSE)
  writePrefix <- validatePrefix(prefix = writePrefix)
  achillesSchema <- validateSchema(con = con, schema = achillesSchema, null = TRUE)
  achillesPrefix <- validatePrefix(prefix = achillesPrefix)

  # create source
  source <- structure(
    .Data = list(),
    pq_con = con,
    cdm_schema = cdmSchema,
    cdm_prefix = cdmPrefix,
    write_schema = writeSchema,
    write_prefix = writePrefix,
    achilles_schema = achillesSchema,
    achilles_prefix = achillesPrefix,
    class = "pq_cdm"
  )

  # create source
  source <- omopgenerics::newCdmSource(src = source, sourceType = "postgres")

  return(source)
}

#' @export
insertTable.pq_cdm <- function(cdm, name, table, overwrite = TRUE, temporary = FALSE, ...) {
  # initial checks
  omopgenerics::assertCharacter(name, length = 1)
  table <- dplyr::as_tibble(table)
  omopgenerics::assertLogical(overwrite, length = 1)
  omopgenerics::assertLogical(temporary, length = 1)

  # check overwrite
  if (overwrite & name %in% listTables(src = cdm, type = "write")) {
    dropTable(src = cdm, type = "write", name = name, callFrom = "insert_table")
  }

  # write table
  writeTable(src = cdm, name = name, value = table, type = "write")

  # indexes?

  readTable(src = cdm, name = name, type = "write")
}

#' @export
dropSourceTable.pq_cdm <- function(cdm, name) {
  dropTable(src = cdm, type = "write", name = name, callFrom = "drop_table")
}

#' @export
compute.pq_cdm <- function(x, name, temporary = FALSE, overwrite = TRUE, type = "write", logPrefix = NULL, jobName = NULL, ...) {
  # get source
  src <- attr(x, "tbl_source")
  con <- getCon(src)

  # find job name
  if (!is.null(jobName)) {
    jobName <- paste0(jobName, collapse = "; ")
  } else if (!is.null(logPrefix)) {
    jobName <- paste0(logPrefix, collapse = "; ")
  } else {
    jobName <- paste0("COMPUTE TABLE ", name)
  }

  # get rendered sql
  render <- as.character(dbplyr::sql_render(x))

  if (temporary) {
    type <- "temp"
    name <- omopgenerics::uniqueTableName()
  }

  ls <- listTables(src = src, type = type)

  if (name %in% ls & isFALSE(overwrite)) {
    cli::cli_abort(c(x = "Can not write table `{name}` as it already exists and `overwrite = FALSE`."))
  }

  formattedName <- formatName(src = src, name = name, type = type)
  if (stringr::str_detect(string = render, pattern = formattedName)) {
    # compute into intermediate table
    jn <- paste0(jobName, " into temp intermediate")
    intermediate <- omopgenerics::uniqueTableName()
    computeTable(src = src, type = "temp", name = intermediate, sql = render, jobName = jn)
    # delete blocking table
    dropTable(src = src, type = type, name = name, callFrom = "compute")
    # compute intermediate to final destination
    jn <- paste0(jobName, " from temp intermediate")
    sql <- paste0("SELECT * FROM ", formatName(src = src, name = intermediate, type = "temp"), ";")
    computeTable(src = src, type = type, name = name, sql = sql, jobName = jn)
    # delete intermediate table
    dropTable(src = src, type = "temp", name = intermediate, callFrom = "compute")
  } else {
    # delete blocking table
    if (name %in% ls) {
      dropTable(src = src, type = type, name = name, callFrom = "compute")
    }
    computeTable(src = src, type = type, name = name, sql = render, jobName = jobName)
  }

  # reference final table
  readTable(src = src, name = name, type = type)
}

#' @export
listSourceTables.pq_cdm <- function(cdm) {
  listTables(src = cdm, type = "write")
}

#' @export
cdmDisconnect.pq_cdm <- function(cdm, ...) {
  con <- getCon(cdm)
  DBI::dbDisconnect(conn = con)
  invisible(TRUE)
}

#' @export
cdmTableFromSource.pq_cdm <- function(src, value) {
  # check it is not data.frame
  if (inherits(value, "data.frame")) {
    cli::cli_abort(c(x = "To insert a local table to a cdm_reference object use insertTable function."))
  }

  # check it is lazy table
  if (!inherits(value, "tbl_PqConnection")) {
    cli::cli_abort(c(x = "Can't assign an object of class: {.cls {class(value)}} to a pq_cdm cdm_reference object."))
  }

  # check it comes from same connection
  con <- getCon(src)
  if (!identical(con, dbplyr::remote_con(value))) {
    cli::cli_abort(c(x = "The cdm object and the table have different connection sources."))
  }

  # check remote name
  remoteName <- dbplyr::remote_name(value)
  if (is.null(remoteName)) {
    name <- NA_character_
  } else if (startsWith(remoteName, "dbplyr") | startsWith(remoteName, "og_")) {
    name <- NA_character_
  } else {
    prefix <- getPrefix(src = src, type = "write")
    name <- substr(remoteName, nchar(prefix) + 1, nchar(remoteName))
  }

  omopgenerics::newCdmTable(table = value, src = src, name = name)
}

#' @export
insertCdmTo.pq_cdm <- function(cdm, to) {
  # identify table types
  tables <- cdmTableClasses(cdm)

  # insert omop tables
  for (nm in tables$omop_tables) {
    writeTable(src = to, name = nm, value = cdm[[nm]], type = "cdm")
  }

  # insert cohort tables
  for (nm in tables$cohort_tables) {
    x <- dplyr::collect(cdm[[nm]])
    writeTable(src = to, name = nm, value = x, type = "write")
    writeTable(src = to, name = paste0(nm, "_set"), value = attr(x, "cohort_set"), type = "write")
    writeTable(src = to, name = paste0(nm, "_attrition"), value = attr(x, "cohort_attrition"), type = "write")
    writeTable(src = to, name = paste0(nm, "_codelist"), value = attr(x, "cohort_codelist"), type = "write")
  }

  # insert achilles tables
  for (nm in tables$achilles_tables) {
    writeTable(src = to, name = nm, value = cdm[[nm]], type = "achilles")
  }

  # insert other tables
  for (nm in tables$other_tables) {
    writeTable(src = to, name = nm, value = cdm[[nm]], type = "write")
  }

  cdm <- cdmFromPostgres(
    con <- getCon(to),
    cdmName = omopgenerics::cdmName(cdm),
    cdmVersion = omopgenerics::cdmVersion(cdm),
    cdmSchema = getSchema(to, "cdm"),
    cdmPrefix = getPrefix(to, "cdm"),
    writeSchema = getSchema(to, "write"),
    writePrefix = getPrefix(to, "write"),
    achillesSchema = getSchema(to, "achilles"),
    achillesPrefix = getPrefix(to, "achilles"),
    cohortTables = tables$cohort_tables
  )

  for (nm in tables$other_tables) {
    cdm[[nm]] <- readTable(src = to, name = nm, type = "write")
  }

  # do we want to add indexes?

  return(cdm)
}

#' @export
readSourceTable.pq_cdm <- function(cdm, name) {
  readTable(src = cdm, name = name, type = "write")
}

#' @export
summary.pq_cdm <- function(object, ...) {

  rlang::check_installed("benchmarkme")
  rlang::check_installed("ps")
  rlang::check_installed("sessioninfo")

  version <- as.character(utils::packageVersion(pkg = "OmopOnPostgres"))
  r_infra <- as.list(tibble::deframe(rInfra()))
  postgres_infra <- as.list(tibble::deframe(postgresInfra(attr(object,"pq_con"))))
c(
  list(
    package = paste0("OmopOnPostgres (", version, ")"),
    cdm_schema = attr(object, "cdm_schema"),
    cdm_prefix = attr(object, "cdm_prefix"),
    write_schema = attr(object, "write_schema"),
    write_prefix = attr(object, "write_prefix"),
    achilles_schema = attr(object, "achilles_schema"),
    achilles_prefix = attr(object, "achilles_prefix")
  ) |>
    purrr::compact(),
  r_infra,
  postgres_infra)
}

rInfra <- function() {

  r_info <- sessioninfo::platform_info()
  cpu_info <- benchmarkme::get_cpu()
  ram_bytes <- as.numeric(benchmarkme::get_ram())
  total_ram_gb <- paste(round(ram_bytes / (1024^3), 2), "GB")
  mem_info <- ps::ps_system_memory()
  available_ram_gb <- paste(round(mem_info$avail / (1024^3), 2), "GB")

  check_val <- function(x) {
    if (is.null(x) || length(x) == 0) "unknown" else as.character(x)
  }

  dplyr::tibble(
    metric = c(
      "Operating System",
      "R Version",
      "RStudio Version",
      "Language",
      "Collate",
      "CPU Model",
      "Number of Cores",
      "Total System RAM",
      "Currently Available RAM"
    ),
    value = c(
      check_val(r_info$os),
      check_val(r_info$version),
      check_val(r_info$rstudio),
      check_val(r_info$language),
      check_val(r_info$collate),
      check_val(cpu_info$model_name),
      check_val(cpu_info$no_of_cores),
      total_ram_gb,
      available_ram_gb
    )
  )

}

postgresInfra <- function(con) {
  is_duckdb <- inherits(con, "duckdb_connection")

  if (is_duckdb) {
    query <- "
      SELECT * FROM postgres_query('pg_db', '
        SELECT ''PostgreSQL Version'' AS metric, version() AS value
        UNION ALL
        SELECT
          name AS metric,
          setting AS value
        FROM pg_settings
        WHERE name IN (
          ''shared_buffers'',
          ''work_mem'',
          ''effective_cache_size'',
          ''max_parallel_workers_per_gather'',
          ''max_parallel_workers'',
          ''max_worker_processes'',
          ''random_page_cost'',
          ''effective_io_concurrency''
        )
      ') ORDER BY metric
    "
  } else {
    query <- "
      SELECT 'PostgreSQL Version' AS metric, version() AS value
      UNION ALL
      SELECT
        name AS metric,
        setting AS value
      FROM pg_settings
      WHERE name IN (
        'shared_buffers',
        'work_mem',
        'effective_cache_size',
        'max_parallel_workers_per_gather',
        'max_parallel_workers',
        'max_worker_processes',
        'random_page_cost',
        'effective_io_concurrency'
      )
      ORDER BY metric
    "
  }

  res <- DBI::dbGetQuery(con, query)

  if (is.null(res) || nrow(res) == 0) {
    return(dplyr::tibble(metric = character(), value = character()))
  }

  dplyr::as_tibble(res)
}
computeTable <- function(src, type, name, sql, jobName) {
  # create sql
  name <- formatName(src = src, name = name, type = type)
  temp <- ifelse(type == "temp", " TEMP", "")
  sql <- paste0("CREATE", temp, " TABLE ", name, " AS ", sql, ";")

  # whether to log
  toLog <- logSql()

  # create log file
  if (toLog) {
    logName <- startLogger(
      jobName = jobName,
      jobType = "compute",
      sql = extractSql(sql = sql),
      explain = extractExplain(src = src, sql = sql)
    )
  }

  # finish logger
  if (toLog) {
    # analyse will also run the query
    analyse <- extractAnalyse(src = src, sql)
    finishLogger(logName = logName, analyse = analyse)
  } else {
    DBI::dbExecute(conn = getCon(src = src), statement = sql)
  }

  invisible(TRUE)
}
dropTable <- function(src, type, name, callFrom = "drop_table") {
  con <- getCon(src = src)
  is_dbc <- inherits(con, "DatabaseConnectorConnection") || inherits(con, "DatabaseConnectorDbiConnection")

  for (nm in name) {
    # create sql
    nm_formatted <- formatName(src = src, name = nm, type = type)
    st <- paste0("DROP TABLE IF EXISTS ", nm_formatted, ";")

    # whether to log
    toLog <- logSql()

    # create log file
    if (toLog) {
      logName <- startLogger(
        jobName = paste0("DROP TABLE ", nm_formatted, " (", type, ")"),
        jobType = "drop_table",
        callFrom = callFrom,
        sql = extractSql(sql = st),
        explain = NA_character_
      )
    }

    # drop table natively for DatabaseConnector, or via DBI for others
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

    # finish logger
    if (toLog) {
      finishLogger(logName = logName, analyse = NA_character_)
    }
  }

  invisible(TRUE)
}
listTables <- function(src, type) {
  con <- getCon(src = src)
  schema <- getSchema(src = src, type = type)
  prefix <- getPrefix(src = src, type = type)
  listTablesPostgres(con = con, schema = schema, prefix = prefix)
}
listTablesPostgres <- function(con, schema, prefix) {
  is_duckdb <- inherits(con, "duckdb_connection")

  if (is_duckdb) {
    if (schema == "") {
      st <- "SELECT tablename FROM postgres_query('pg_db', 'SELECT tablename FROM pg_tables WHERE schemaname LIKE ''pg_temp%''');"
    } else {
      st <- paste0("SELECT tablename FROM postgres_query('pg_db', 'SELECT tablename FROM pg_tables WHERE schemaname = ''", schema, "''');")
    }
  } else {
    if (schema == "") {
      st <- "SELECT tablename FROM pg_tables WHERE schemaname LIKE 'pg_temp%';"
    } else {
      st <- paste0("SELECT tablename FROM pg_tables WHERE schemaname = '", schema, "';")
    }
  }

  x <- DBI::dbGetQuery(conn = con, statement = st)$tablename

  if (prefix != "") {
    x <- x |>
      purrr::keep(\(x) startsWith(x = x, prefix = prefix)) |>
      stringr::str_replace(pattern = paste0("^", prefix), replacement = "") |>
      purrr::keep(\(x) nchar(x) > 0)
  }
  return(x)
}
writeTable <- function(src, name, value, type) {
  vocab <- "5.4"
  # whether to log
  toLog <- logSql()

  # get attributes
  con <- getCon(src = src)
  fn <- formatName(src = src, name = name, type = type)
  idn <- IdName(src = src, name = name, type = type)

  # type
  if (type %in% c("cdm", "achilles")) {
    typ <- dplyr::if_else(type == "cdm", "cdm_table", "achilles")
    colTypes <- postgresDatatypes[[vocab]] |>
      dplyr::filter(
        .data$type == .env$typ & .data$cdm_table_name == .env$name
      ) |>
      dplyr::select("cdm_field_name", "cdm_datatype") |>
      dplyr::distinct()
  } else if (type == "cohort") {
    colTypes <- postgresDatatypes[[vocab]] |>
      dplyr::filter(.data$type == "cohort") |>
      dplyr::select("cdm_field_name", "cdm_datatype") |>
      dplyr::distinct()
  } else {
    colTypes <- dplyr::tibble(
      cdm_field_name = c("subject_id", "person_id"),
      cdm_datatype = "bigint"
    )
  }

  # column type
  value <- dplyr::as_tibble(value)
  colTypes <- colTypes$cdm_datatype |>
    rlang::set_names(nm = colTypes$cdm_field_name)
  colTypes <- value |>
    purrr::imap_chr(\(x, nm) {
      if (nm %in% names(colTypes)) {
        colTypes[[nm]]
      } else {
        DBI::dbDataType(dbObj = con, obj = x)
      }
    })

  # start log
  if (toLog) {
    logName <- startLogger(
      jobName = paste0("CREATE TABLE ", fn),
      jobType = "insert_table",
      sql = extractSql(sql = paste("INSERT INTO", fn, "A TABLE WITH", nrow(value), "ROWS;\n")),
      explain = NA_character_
    )
  }

  problem_types <- c("DOUBLE", "NUMERIC", "DECIMAL", "FLOAT")
  colTypes[toupper(colTypes) %in% problem_types] <- "DOUBLE PRECISION"

  is_dbc <- inherits(con, "DatabaseConnectorConnection") || inherits(con, "DatabaseConnectorDbiConnection")
  if (is_dbc) {
    DatabaseConnector::executeSql(
      connection = con,
      sql = paste0("DROP TABLE IF EXISTS ", fn, ";"),
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    quoted_cols <- paste0('"', names(colTypes), '"')
    col_defs <- paste(quoted_cols, colTypes, collapse = ", ")

    temp_kw <- if (type == "temp") "TEMP " else ""
    create_sql <- sprintf("CREATE %sTABLE %s (%s);", temp_kw, fn, col_defs)

    DatabaseConnector::executeSql(
      connection = con,
      sql = create_sql,
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
    if (nrow(value) > 0) {
      DatabaseConnector::insertTable(
        connection = con,
        tableName = fn,
        data = as.data.frame(value),
        dropTableIfExists = FALSE,
        createTable = FALSE,
        camelCaseToSnakeCase = FALSE
      )
    }
  } else {
    if (DBI::dbExistsTable(con, idn)) {
      DBI::dbRemoveTable(con, idn)
    }
    DBI::dbCreateTable(
      conn = con,
      name = idn,
      fields = colTypes,
      temporary = type == "temp"
    )

    if (nrow(value) > 0) {
      DBI::dbAppendTable(
        conn = con,
        name = idn,
        value = value
      )
    }
  }

  # finish log
  if (toLog) {
    finishLogger(logName = logName, analyse = NA_character_)
  }

  invisible(TRUE)
}
readTable <- function(src, name, type) {
  dplyr::tbl(src = getCon(src), I(formatName(src, name, type))) |>
    omopgenerics::newCdmTable(src = src, name = name)
}

getCon <- function(src) {
  attr(src, "pq_con")
}
getSchema <- function(src, type) {
  if (type == "cdm") {
    attr(src, "cdm_schema")
  } else if (type == "write") {
    attr(src, "write_schema")
  } else if (type == "achilles") {
    attr(src, "achilles_schema")
  } else if (type == "temp") {
    ""
  }
}
getPrefix <- function(src, type) {
  if (type == "cdm") {
    attr(src, "cdm_prefix")
  } else if (type == "write") {
    attr(src, "write_prefix")
  } else if (type == "achilles") {
    attr(src, "achilles_prefix")
  } else if (type == "temp") {
    ""
  }
}
formatName <- function(src, name, type) {
  schema <- getSchema(src = src, type = type)
  prefix <- getPrefix(src = src, type = type)
  formatNamePostgres(schema = schema, prefix = prefix, name = name)
}
formatNamePostgres <- function(schema, prefix, name) {
  if (schema == "") {
    paste0(prefix, name)
  } else {
    paste0(schema, ".", prefix, name)
  }
}
IdName <- function(src, name, type) {
  schema <- getSchema(src, type)
  name <- paste0(getPrefix(src, type), name)
  if (schema == "") {
    DBI::Id(table = name)
  } else {
    DBI::Id(schema = schema, table = name)
  }
}
validateCon <- function(con, call = parent.frame()) {

  allowed_classes <- c(
    "PqConnection",
    "AdbiConnection",
    "PostgreSQL",
    "DatabaseConnectorConnection",
    "DatabaseConnectorDbiConnection",
    "duckdb_connection"
  )

  if (!inherits(con, allowed_classes)) {
    c(x = "`con` is not supported") |>
      cli::cli_abort(call = call)
  }

  if (!DBI::dbIsValid(con)) {
    cli::cli_abort(c(x = "Connection is no longer valid."),
                   call = call)
  }
  invisible(con)
}
validateSchema <- function(con, schema, null, call = parent.frame()) {
  omopgenerics::assertCharacter(schema, length = 1, null = null, call = call)
  emptySchema <- is.null(schema) | identical(schema, "")
  if (emptySchema) {
    if (null) {
      schema <- ""
    } else {
      cli::cli_abort(c(x = "Schema must be defined."), call = call)
    }
  } else {
    if (!schemaExists(con, schema)) {
      if (question("Schema {.pkg {schema}} does not exist. Do you want to create it? Y/n")) {
        cli::cli_inform(c("i" = "Creating schema: {.pkg {schema}}."))
        createSchema(con, schema)
      } else {
        cli::cli_abort(c(x = "schema: {.pkg {schema}} does not exist."), call = call)
      }
    }
  }
  invisible(schema)
}
validatePrefix <- function(prefix, call = parent.frame()) {
  if (is.null(prefix)) {
    prefix <- ""
  } else {
    omopgenerics::assertCharacter(prefix, length = 1, call = call)
  }
  invisible(prefix)
}
