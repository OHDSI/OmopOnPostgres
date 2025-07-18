% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/createOmopTablesOnPostgres.R
\name{createOmopTablesOnPostgres}
\alias{createOmopTablesOnPostgres}
\title{Title}
\usage{
createOmopTablesOnPostgres(
  con,
  cdmSchema = "public",
  cdmVersion = "5.4",
  overwrite = FALSE,
  bigInt = FALSE,
  cdmPrefix = ""
)
}
\arguments{
\item{con}{A PqConnection created with DBI and RPostgres.}

\item{cdmSchema}{String, name of the schema to create the OMOP CDM Standard
tables.}

\item{cdmVersion}{Version of the OMOP CDM, it can be either '5.3' or '5.4'.}

\item{overwrite}{Whether to overwrite if tables already exist.}

\item{bigInt}{Whether to use \code{bigint} for person_id and unique identifier.}

\item{cdmPrefix}{String, prefix leading the OMOP CDM Standard tables names.}
}
\value{
The omop tables will be created empty in the desired schema.
}
\description{
Title
}
