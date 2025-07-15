
#' Title
#'
#' @param con A PqConnection created with DBI and RPostgres.
#' @param cdmSchema String, name of the schema to create the OMOP CDM Standard
#' tables.
#' @param cdmVersion Version of the OMOP CDM, it can be either '5.3' or '5.4'.
#' @param overwrite Whether to overwrite if tables already exist.
#' @param bigInt Whether to use `bigint` for person_id and unique identifier.
#' @param cdmPrefix String, prefix leading the OMOP CDM Standard tables names.
#'
#' @return The omop tables will be created empty in the desired schema.
#' @export
#'
createOmopTablesOnPostgres <- function(con,
                                       cdmSchema = "public",
                                       cdmVersion = "5.4",
                                       overwrite = FALSE,
                                       bigInt = FALSE,
                                       cdmPrefix = "") {
  # input check
  con <- validateCon(con = con)
  cdmSchema <- validateSchema(con = con, schema = cdmSchema, null = FALSE)
  omopgenerics::assertChoice(x = cdmVersion, choices = c("5.3", "5.4"))
  omopgenerics::assertLogical(x = overwrite, length = 1)
  omopgenerics::assertLogical(x = bigInt, length = 1)
  cdmPrefix <- validatePrefix(prefix = cdmPrefix)

  # tables to create
  fields <- postgresDatatypes[[cdmVersion]]
  if (isFALSE(bigInt)) {
    fields <- fields |>
      dplyr::mutate(cdm_datatype = dplyr::if_else(
        .data$cdm_datatype == "bigint", "integer", .data$cdm_datatype
      ))
  }

  # list creating tables
  ls <- listTablesPostgres(con = con, schema = cdmSchema, prefix = cdmPrefix)

  # create tables
  for (nm in unique(fields$cdm_table_name)) {
    # check if table already exists
    tableExists <- nm %in% ls
    create <- TRUE
    if (tableExists) {
      if (overwrite) {
        fn <- formatNamePostgres(schema = cdmSchema, prefix = cdmPrefix, name = nm)
        st <- paste0("DROP TABLE IF EXISTS ", fn, ";")
        DBI::dbExecute(conn = con, statement = st)
      } else {
        create <- FALSE
        cli::cli_inform(c(x = "Table {.pkg {nm}} could not be created because it already exists and overwrite is {.emph FALSE}."))
      }
    }
    if (create) {
      cols <- fields |>
        dplyr::filter(.data$cdm_table_name == .env$nm)
      fn <- formatNamePostgres(schema = cdmSchema, prefix = cdmPrefix, name = nm)
      st <- createStatement(name = fn, cols = cols)
      DBI::dbExecute(conn = con, statement = st)
      cli::cli_inform(c(v = "Table {.pkg {nm}} created succesfully."))
    }
  }

  invisible()
}
createStatement <- function(name, cols) {
  cols <- purrr::map_chr(seq_along(cols$cdm_field_name), \(k) {
    cfn <- cols$cdm_field_name[k]
    ir <- cols$is_required[k]
    cd <- cols$cdm_datatype[k]
    paste0("\"", cfn, "\" ", cd, dplyr::if_else(ir, " NOT NULL", " NULL"))
  }) |>
    paste0(collapse = ",\n")
  paste0("CREATE TABLE ", name, "(\n", cols, ");")
}
