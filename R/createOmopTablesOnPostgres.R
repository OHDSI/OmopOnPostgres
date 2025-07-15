
#' Title
#'
#' @param con A PqConnection created with DBI and RPostgres.
#' @param cdmSchema String, name of the schema to create the OMOP CDM Standard
#' tables.
#' @param cdmVersion Version of the OMOP CDM, it can be either '5.3' or '5.4'.
#' @param overwrite Whether to overwrite if tables already exist.
#' @param bigInt
#' @param cdmPrefix String, prefix leading the OMOP CDM Standard tables names.
#'
#' @return
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

  # create source
  src <- postgresSource(
    con = con,
    cdmSchema = cdmSchema,
    cdmPrefix = cdmPrefix,
    writeSchema = cdmSchema,
    writePrefix = cdmPrefix
  )

  # tables to create
  fields <- omopgenerics::omopTableFields(cdmVersion = cdmVersion) |>
    dplyr::filter(.data$type == "cdm_table")
  if (bigInt) {
    fields <- fields |>
      dplyr::mutate(cdm_datatype = dplyr::if_else(
        .data$cdm_field_name == "person_id", "bigint", .data$cdm_datatype
      ))
  }

  # correct types
  fields <- fields |>
    dplyr::mutate(cdm_datatype = dplyr::case_when(
      .data$cdm_datatype == "datetime" ~ "timestamp",
      .data$cdm_datatype == "float" ~ "numeric",
      .data$cdm_datatype == "varchar(max)" ~ "TEXT",
      .default = .data$cdm_datatype
    ))

  # list creating tables
  ls <- listTables(src = src, type = "write")

  # create tables
  for (nm in unique(fields$cdm_table_name)) {
    # check if table already exists
    tableExists <- nm %in% ls
    create <- TRUE
    if (tableExists) {
      if (overwrite) {
        dropSourceTable(cdm = src, name = nm)
      } else {
        create <- FALSE
        cli::cli_inform(c(x = "Table {.pkg {nm}} could not be created because it already exists and overwrite is {.emph FALSE}."))
      }
    }
    if (create) {
      cols <- fields |>
        dplyr::filter(.data$cdm_table_name == .env$nm)
      fn <- formatName(src = src, name = nm, type = "write")
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
