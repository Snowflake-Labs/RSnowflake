.pkg_env <- new.env(parent = emptyenv())

.onLoad <- function(libname, pkgname) {
  op <- options()
  op_rsf <- list(
    RSnowflake.timeout              = 600L,
    RSnowflake.retry_max            = 3L,
    RSnowflake.result_format        = "json",
    RSnowflake.insert_batch_size    = 16384L,
    RSnowflake.upload_method        = "auto",
    RSnowflake.identifier_case      = "upper",
    RSnowflake.use_simdjson         = TRUE,
    RSnowflake.parallel_fetch       = TRUE,
    RSnowflake.fetch_workers        = 0L,
    RSnowflake.use_session          = FALSE,
    RSnowflake.use_native_arrow     = FALSE,
    RSnowflake.verbose              = FALSE,
    RSnowflake.backend              = "auto",
    RSnowflake.adbc_write_threshold = 50000L
  )
  toset <- !(names(op_rsf) %in% names(op))
  if (any(toset)) options(op_rsf[toset])

  .register_dbplyr_methods()

  invisible()
}

#' Apply identifier case policy
#'
#' When `RSnowflake.identifier_case` is `"upper"` (the default), identifiers
#' are uppercased before quoting, matching Snowflake's default behavior for
#' unquoted identifiers and the behavior of the ODBC driver.  When set to
#' `"preserve"`, identifiers retain their original case.
#' @param x Character vector of identifier names.
#' @returns Character vector, possibly uppercased.
#' @noRd
.maybe_upcase <- function(x) {
  if (identical(getOption("RSnowflake.identifier_case", "upper"), "upper")) {
    toupper(x)
  } else {
    x
  }
}
