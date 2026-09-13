# Mock Postgres CDM reference

Mock Postgres CDM reference

## Usage

``` r
mockPostgresCdmReference(client, datasetName = "GiBleed", cdmVersion = NULL)
```

## Arguments

- client:

  Client supported by OmopOnPostgres

- datasetName:

  Mock dataset to include. See 'omock::availableMockDatasets()' for
  available datasets.

- cdmVersion:

  CDM version, see 'omock::mockCdmFromDataset()' for more details.

## Value

CDM reference for mock data in a local Postgres database
