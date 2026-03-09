.pkg_env <- new.env(parent = emptyenv())

.onLoad <- function(libname, pkgname) {
  op <- options()

  # Bulk write threshold: in "auto" mode, writes above this cell count

  # (rows * cols) are routed to the fast bulk backend:
  #   - Workspace: Snowpark write_pandas (internal SPCS path, ~2.5s/50K rows)
  #   - External:  ADBC PUT+COPY INTO (public endpoint, ~5-10s/50K rows)
  # Below threshold, literal SQL INSERT is used (fast for small data).
  # Workspace threshold is higher because even the fast Snowpark path has
  # ~2s fixed overhead vs ~1s for literal INSERT on small data.
  in_workspace <- nzchar(Sys.getenv("SNOWFLAKE_HOST", ""))
  bulk_threshold <- if (in_workspace) 200000L else 50000L

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
    RSnowflake.bulk_write_threshold = bulk_threshold,
    RSnowflake.adbc_write_threshold = bulk_threshold
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
