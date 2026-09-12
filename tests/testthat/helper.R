get_test_drivers <- function() {
  ci_driver <- Sys.getenv("TEST_PG_DRIVER", "")
  if (nzchar(ci_driver)) {
    # If in CI, only run the matrix driver (from action)
    return(ci_driver)
  } else {
    # If local, test everything
    return(c("RPostgres", "adbc", "odbc"))
  }
}
