
#' Mock Postgres CDM reference
#'
#' @param client Client supported by OmopOnPostgres
#' @param datasetName Mock dataset to include. See
#' 'omock::availableMockDatasets()' for available
#' datasets.
#' @param cdmVersion CDM version, see
#' 'omock::mockCdmFromDataset()' for more details.
#'
#' @returns CDM reference for mock data in a local
#' Postgres database
#' @export
#'
mockPostgresCdmReference <- function(client,
                                     datasetName = "GiBleed",
                                     cdmVersion = NULL){
con <- localPostgres(client = client)
cdm <- omock::mockCdmFromDataset(datasetName = "GiBleed",
                                 source = "local",
                                 cdmVersion = NULL)
pcdm <- copyCdmToPostgres(cdm = cdm,
                          con = con,
                          cdmPrefix = "mock_",
                          writePrefix = "mock_")
return(pcdm)

}
