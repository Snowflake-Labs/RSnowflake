# Low-Level Snowflake SQL API v2 Client
# =============================================================================

#' Submit a SQL statement to Snowflake
#'
#' @param con A SnowflakeConnection object.
#' @param sql SQL string.
#' @param bindings Named list of parameter bindings, or NULL.
#' @param async Logical. If TRUE, don't wait for completion.
#' @returns Parsed JSON response body (list).
#' @noRd
sf_api_submit <- function(con, sql, bindings = NULL, async = FALSE) {
  host <- sf_host(con@account, con@.auth)
  url <- paste0(host, "/api/v2/statements")

  body <- list(
    statement = sql,
    timeout   = getOption("RSnowflake.timeout", 600L),
    resultSetMetaData = list(format = "jsonv2")
  )

  if (nzchar(con@database))  body$database  <- con@database
  if (nzchar(con@schema))    body$schema    <- con@schema
  if (nzchar(con@warehouse)) body$warehouse <- con@warehouse
  if (nzchar(con@role))      body$role      <- con@role

  if (!is.null(bindings)) {
    body$bindings <- bindings
  }

  params <- list()
  if (async) params$async <- "true"
  if (length(params) > 0L) {
    qs <- paste0(names(params), "=", params, collapse = "&")
    url <- paste0(url, "?", qs)
  }

  .sf_api_request_with_refresh(con, "POST", url, body = body)
}

#' Fetch a result partition (JSON)
#' @noRd
sf_api_fetch_partition <- function(con, handle, partition) {
  host <- sf_host(con@account, con@.auth)
  url <- paste0(host, "/api/v2/statements/", handle,
                "?partition=", partition)
  .sf_api_request_with_refresh(con, "GET", url)
}

#' Check the status of an async statement
#' @noRd
sf_api_status <- function(con, handle) {
  host <- sf_host(con@account, con@.auth)
  url <- paste0(host, "/api/v2/statements/", handle)
  .sf_api_request_with_refresh(con, "GET", url)
}

#' Cancel a running statement
#' @noRd
sf_api_cancel <- function(con, handle) {
  host <- sf_host(con@account, con@.auth)
  url <- paste0(host, "/api/v2/statements/", handle, "/cancel")
  tryCatch(
    .sf_api_request_with_refresh(con, "POST", url),
    error = function(e) NULL
  )
}


# ---------------------------------------------------------------------------
# Token refresh wrapper
# ---------------------------------------------------------------------------

.sf_api_request_with_refresh <- function(con, method, url, body = NULL) {
  resp <- .sf_api_request_raw(con, method, url, body)
  status <- httr2::resp_status(resp)

  if (status == 401L) {
    refreshed <- .try_refresh_token(con)
    if (refreshed) {
      resp <- .sf_api_request_raw(con, method, url, body)
      status <- httr2::resp_status(resp)
    }
  }

  .handle_response(resp, url)
}

#' Attempt to refresh the auth token
#' @returns TRUE if token was refreshed, FALSE otherwise.
#' @noRd
.try_refresh_token <- function(con) {
  auth <- con@.auth

  if (auth$type == "jwt") {
    return(tryCatch({
      new_jwt <- sf_generate_jwt(auth$account, auth$user, auth$private_key_path)
      con@.auth$token <- new_jwt
      con@.auth$generated_at <- Sys.time()
      TRUE
    }, error = function(e) FALSE))
  }

  if (auth$type %in% c("token", "oauth")) {
    old_token <- auth$token
    new_token <- .read_workspace_token()
    token_changed <- nzchar(new_token) && new_token != old_token
    if (token_changed) {
      con@.auth$token <- new_token
    }
    return(token_changed)
  }

  FALSE
}


# ---------------------------------------------------------------------------
# Internal HTTP helper
# ---------------------------------------------------------------------------

.sf_api_request_raw <- function(con, method, url, body = NULL) {
  auth <- con@.auth
  token <- auth$token
  token_type <- auth$token_type %||% "KEYPAIR_JWT"

  req <- httr2::request(url) |>
    httr2::req_headers(
      "Authorization" = paste("Bearer", token),
      "X-Snowflake-Authorization-Token-Type" = token_type,
      "Content-Type"  = "application/json",
      "Accept"        = "application/json",
      "User-Agent"    = sf_user_agent()
    ) |>
    httr2::req_timeout(getOption("RSnowflake.timeout", 600L))

  if (!is.null(body)) {
    req <- req |> httr2::req_body_json(body, auto_unbox = TRUE)
  }

  if (toupper(method) == "GET") {
    req <- req |> httr2::req_method("GET")
  }

  max_retries <- getOption("RSnowflake.retry_max", 3L)
  req <- req |>
    httr2::req_error(is_error = function(resp) FALSE) |>
    httr2::req_retry(
      max_tries    = max_retries,
      is_transient = function(resp) httr2::resp_status(resp) %in% c(429L, 503L)
    )

  tryCatch(
    httr2::req_perform(req),
    error = function(e) {
      cli_abort(c(
        "x" = "Snowflake API request failed.",
        "i" = "URL: {.url {url}}",
        "x" = conditionMessage(e)
      ))
    }
  )
}

.handle_response <- function(resp, url) {
  status <- httr2::resp_status(resp)

  if (status %in% c(200L, 202L)) {
    return(.parse_json_body(resp))
  }

  err_body <- tryCatch(.parse_json_body(resp), error = function(e) list())
  sf_code <- err_body$code %||% as.character(status)
  sf_msg  <- err_body$message %||%
             tryCatch(httr2::resp_body_string(resp), error = function(e) "(no body)")

  cli_abort(c(
    "x" = "Snowflake SQL API error (HTTP {status}, code {sf_code}).",
    "i" = sf_msg
  ))
}

#' Parse a JSON response body, using RcppSimdJson when available
#'
#' Uses fparse with default simplification for maximum speed.  Downstream
#' parsers (sf_parse_metadata, .json_data_to_df) handle both the simplified
#' structures (data.frame/matrix) from fparse and the nested-list structures
#' from httr2::resp_body_json().
#' @noRd
.parse_json_body <- function(resp) {
  use_simd <- isTRUE(getOption("RSnowflake.use_simdjson", TRUE))
  if (use_simd && requireNamespace("RcppSimdJson", quietly = TRUE)) {
    raw_bytes <- httr2::resp_body_raw(resp)
    return(RcppSimdJson::fparse(rawToChar(raw_bytes)))
  }
  httr2::resp_body_json(resp)
}
