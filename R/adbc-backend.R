# ADBC Backend (Optional Accelerator)
# =============================================================================
# When adbcsnowflake and adbcdrivermanager are installed, RSnowflake can
# transparently use ADBC for bulk reads (Arrow-native) and writes (PUT +
# COPY INTO via the Go driver).  The REST API v2 remains the universal
# fallback when ADBC packages are absent or initialisation fails.
#
# Architecture note (confirmed by Snowflake engineering via GLEAN, Mar 2026):
# - ADBC uses the Go Snowflake driver, which connects to the **public**
#   account endpoint (https://<account>.snowflakecomputing.com) as an
#   external client.  It does NOT use the internal SPCS GS path.
# - In Workspace Notebooks (SPCS containers), ADBC authenticates with a
#   PAT (Programmatic Access Token) -- the SPCS token at
#   /snowflake/session/token is only accepted by blessed clients (Snowpark
#   Python, Snowpark Scala/Java JDBC).  ADBC is not in that list.
# - PAT auth from SPCS requires a network policy or authentication policy
#   with PAT_POLICY = (NETWORK_POLICY_EVALUATION = ENFORCED_NOT_REQUIRED).
# - SQL API v2 does NOT support Arrow result format (gated behind an
#   internal flag, not GA).  ADBC is the only way to get Arrow-native
#   reads from R today without a GS session.
#
# The ADBC backend is lazy-initialised on first use and stored in the
# connection's mutable .state environment.

# ---------------------------------------------------------------------------
# Availability checks
# ---------------------------------------------------------------------------

#' Check whether the ADBC packages are installed
#' @noRd
.adbc_packages_available <- function() {
  requireNamespace("adbcsnowflake", quietly = TRUE) &&
    requireNamespace("adbcdrivermanager", quietly = TRUE)
}

#' Check whether a connection has a live ADBC backend
#' @noRd
.has_adbc <- function(conn) {
  !is.null(conn@.state$adbc) && !is.null(conn@.state$adbc$con)
}

# ---------------------------------------------------------------------------
# Initialisation & lifecycle
# ---------------------------------------------------------------------------

#' Lazy-init the ADBC backend, returning the backend list or NULL
#'
#' On first call the backend is created and cached in conn@.state$adbc.
#' Subsequent calls return the cached value immediately.
#' @noRd
.ensure_adbc <- function(conn) {
  if (!is.null(conn@.state$adbc)) return(conn@.state$adbc)

  backend <- getOption("RSnowflake.backend", "auto")
  if (identical(backend, "rest")) return(NULL)

  adbc <- .init_adbc_backend(conn)
  conn@.state$adbc <- adbc
  adbc
}

#' Create an ADBC database + connection using the same credentials as the
#' REST API v2 connection.
#' @noRd
.init_adbc_backend <- function(conn) {
  if (!.adbc_packages_available()) return(NULL)

  tryCatch({
    auth_info <- .adbc_auth_args(conn)

    args <- c(
      list(
        driver   = adbcsnowflake::adbcsnowflake(),
        username = conn@user,
        `adbc.snowflake.sql.account`   = conn@account,
        `adbc.snowflake.sql.db`        = conn@database,
        `adbc.snowflake.sql.schema`    = conn@schema,
        `adbc.snowflake.sql.warehouse` = conn@warehouse
      ),
      auth_info
    )

    host <- Sys.getenv("SNOWFLAKE_HOST", "")
    if (nzchar(host)) {
      args[["adbc.snowflake.sql.uri.host"]] <- host
    }

    if (nzchar(conn@role)) {
      args[["adbc.snowflake.sql.role"]] <- conn@role
    }

    db <- do.call(adbcdrivermanager::adbc_database_init, args)
    adbc_con <- adbcdrivermanager::adbc_connection_init(db)

    if (isTRUE(getOption("RSnowflake.verbose", FALSE))) {
      in_workspace <- nzchar(Sys.getenv("SNOWFLAKE_HOST", ""))
      if (in_workspace) {
        cli_inform(c(
          "i" = "ADBC backend initialised (Workspace: external-client pattern via public endpoint).",
          "i" = "ADBC uses PAT auth to {.val {conn@account}.snowflakecomputing.com}.",
          "i" = "Ensure a network policy or authentication policy allows PAT from this container."
        ))
      } else {
        cli_inform("i" = "ADBC backend initialised.")
      }
    }

    list(db = db, con = adbc_con)
  }, error = function(e) {
    if (!identical(getOption("RSnowflake.backend", "auto"), "auto")) {
      in_workspace <- nzchar(Sys.getenv("SNOWFLAKE_HOST", ""))
      hints <- conditionMessage(e)
      if (in_workspace && grepl("auth|token|oauth|network", hints, ignore.case = TRUE)) {
        cli_warn(c(
          "!" = "ADBC backend initialisation failed inside Workspace.",
          "i" = hints,
          "i" = "ADBC in Workspace requires PAT auth to the public endpoint.",
          "i" = "Ensure the user has an authentication policy that allows PAT without a network policy,",
          "i" = "or configure a network policy that includes the container's egress IP."
        ))
      } else {
        cli_warn(c(
          "!" = "ADBC backend initialisation failed.",
          "i" = hints
        ))
      }
    }
    NULL
  })
}

#' Map RSnowflake auth to ADBC driver arguments
#' @returns Named list of ADBC auth options.
#' @noRd
.adbc_auth_args <- function(conn) {
  auth <- conn@.auth

  if (auth$type %in% c("pat", "token")) {
    return(list(
      `adbc.snowflake.sql.auth_type` = "auth_pat",
      `adbc.snowflake.sql.client_option.auth_token` = auth$token
    ))
  }

  if (auth$type == "jwt") {
    key_path <- auth$private_key_path %||% ""
    if (nzchar(key_path) && file.exists(key_path)) {
      return(list(
        `adbc.snowflake.sql.auth_type` = "auth_jwt",
        `adbc.snowflake.sql.client_option.jwt_private_key` = normalizePath(key_path)
      ))
    }
    return(list(
      `adbc.snowflake.sql.auth_type` = "auth_jwt",
      `adbc.snowflake.sql.client_option.auth_token` = auth$token
    ))
  }

  list()
}

# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------

#' Execute a SQL query via ADBC and return a data.frame
#' @noRd
.adbc_get_query <- function(adbc, statement) {
  result <- adbcdrivermanager::read_adbc(adbc$con, statement)
  as.data.frame(result)
}

#' Execute a SQL query via ADBC and return a nanoarrow array stream
#' @noRd
.adbc_get_query_arrow <- function(adbc, statement) {
  stmt <- adbcdrivermanager::adbc_statement_init(adbc$con)
  adbcdrivermanager::adbc_statement_set_sql_query(stmt, statement)
  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  adbcdrivermanager::adbc_statement_execute_query(stmt, stream)
  stream
}

# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

#' Bulk ingest a data.frame via ADBC (PUT + COPY INTO under the hood)
#'
#' Uses the high-level `write_adbc()` which handles statement lifecycle
#' and is compatible across adbcdrivermanager versions.
#' For schema-qualified names, switches the ADBC session schema first.
#'
#' @param adbc The ADBC backend list (with $db and $con).
#' @param table_name Unquoted table name, possibly schema-qualified (e.g. "SCHEMA.TABLE").
#' @param df data.frame to ingest.
#' @param mode ADBC ingest mode: "default", "create", or "append".
#' @noRd
.adbc_write_table <- function(adbc, table_name, df, mode = "append") {
  parts <- strsplit(table_name, ".", fixed = TRUE)[[1L]]

  if (length(parts) >= 2L) {
    schema_part <- parts[length(parts) - 1L]
    table_part <- parts[length(parts)]

    prev_schema <- tryCatch(
      .adbc_get_query(adbc, "SELECT CURRENT_SCHEMA()")[[1L]],
      error = function(e) NULL
    )

    .adbc_execute_sql(adbc, paste("USE SCHEMA", schema_part))
    on.exit({
      if (!is.null(prev_schema) && nzchar(prev_schema)) {
        tryCatch(
          .adbc_execute_sql(adbc, paste("USE SCHEMA", prev_schema)),
          error = function(e) NULL
        )
      }
    }, add = TRUE)
  } else {
    table_part <- table_name
  }

  adbcdrivermanager::write_adbc(df, adbc$con, table_part, mode = mode)
  invisible(TRUE)
}

#' Execute a SQL statement on the ADBC connection (fire-and-forget)
#' @noRd
.adbc_execute_sql <- function(adbc, sql) {
  stmt <- adbcdrivermanager::adbc_statement_init(adbc$con)
  on.exit(tryCatch(
    adbcdrivermanager::adbc_statement_release(stmt),
    error = function(e) NULL
  ))
  adbcdrivermanager::adbc_statement_set_sql_query(stmt, sql)
  adbcdrivermanager::adbc_statement_execute_query(stmt)
  invisible()
}

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

#' Release ADBC connection and database resources
#' @noRd
.adbc_cleanup <- function(adbc) {
  if (is.null(adbc)) return(invisible())
  tryCatch({
    if (!is.null(adbc$con)) {
      adbcdrivermanager::adbc_connection_release(adbc$con)
    }
    if (!is.null(adbc$db)) {
      adbcdrivermanager::adbc_database_release(adbc$db)
    }
  }, error = function(e) NULL)
  invisible()
}
