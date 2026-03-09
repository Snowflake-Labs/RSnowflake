sf_user_agent <- function() {
  paste0(
    "RSnowflake/", utils::packageVersion("RSnowflake"),
    " R/", getRversion(),
    " (", Sys.info()[["sysname"]], ")"
  )
}

sf_host <- function(account, auth = NULL) {
  # In Workspace (SPCS), route all REST API v2 traffic to the internal

  # gateway.  Bearer + SPCS OAuth works on SNOWFLAKE_HOST for both ADBC
  # and REST API v2 (/api/v2/statements), so no public endpoint is needed.
  spcs_host <- Sys.getenv("SNOWFLAKE_HOST", "")
  if (nzchar(spcs_host) && !is.null(auth) && auth$type == "oauth") {
    return(paste0("https://", spcs_host))
  }
  paste0("https://", account, ".snowflakecomputing.com")
}
