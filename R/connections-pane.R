# RStudio / Positron Connections Pane Integration
# =============================================================================
# Notifies the IDE when a SnowflakeConnection is opened or closed so it
# appears in the Connections Pane.  These hooks are no-ops when running
# outside an IDE that supports the connections contract.

#' Notify the IDE that a Snowflake connection was opened
#' @noRd
.on_connection_opened <- function(conn) {
  observer <- getOption("connectionObserver")
  if (is.null(observer)) return(invisible())

  tryCatch({
    observer$connectionOpened(
      type        = "Snowflake",
      displayName = paste0(conn@account, " [", conn@database, "]"),
      host        = paste0(conn@account, ".snowflakecomputing.com"),
      connectCode = .connect_code(conn),
      disconnect  = function() { dbDisconnect(conn) },
      listObjectTypes = function() {
        list(
          database = list(
            icon = "catalog",
            contains = list(
              schema = list(
                icon = "schema",
                contains = list(
                  table = list(icon = "table", contains = "data")
                )
              )
            )
          )
        )
      },
      listObjects = function(database = NULL, schema = NULL, ...) {
        # Look up from current namespace so pkgload::load_all picks up changes
        # without requiring disconnect/reconnect.
        fn <- get0(".pane_list_objects", envir = asNamespace("RSnowflake"),
                   ifnotfound = .pane_list_objects)
        fn(conn, database, schema)
      },
      listColumns = function(database = NULL, schema = NULL, table = NULL, ...) {
        fn <- get0(".pane_list_columns", envir = asNamespace("RSnowflake"),
                   ifnotfound = .pane_list_columns)
        fn(conn, database, schema, table)
      },
      previewObject = function(rowLimit, database = NULL, schema = NULL,
                               table = NULL, ...) {
        fn <- get0(".pane_preview", envir = asNamespace("RSnowflake"),
                   ifnotfound = .pane_preview)
        fn(conn, database, schema, table, rowLimit)
      },
      connectionObject = conn
    )
  }, error = function(e) {
    invisible()
  })
}

#' Notify the IDE that a Snowflake connection was closed
#' @noRd
.on_connection_closed <- function(conn) {
  observer <- getOption("connectionObserver")
  if (is.null(observer)) return(invisible())

  tryCatch({
    observer$connectionClosed(
      type = "Snowflake",
      host = paste0(conn@account, ".snowflakecomputing.com")
    )
  }, error = function(e) {
    invisible()
  })
}

#' Generate dbConnect() code for the Connections Pane "Connect" button
#' @noRd
.connect_code <- function(conn) {
  parts <- c('library(RSnowflake)')
  args <- character(0)
  if (nzchar(conn@account))   args <- c(args, paste0('account = "', conn@account, '"'))
  if (nzchar(conn@database))  args <- c(args, paste0('database = "', conn@database, '"'))
  if (nzchar(conn@schema))    args <- c(args, paste0('schema = "', conn@schema, '"'))
  if (nzchar(conn@warehouse)) args <- c(args, paste0('warehouse = "', conn@warehouse, '"'))
  parts <- c(parts, paste0("con <- dbConnect(Snowflake(), ", paste(args, collapse = ", "), ")"))
  paste(parts, collapse = "\n")
}

#' Run a query for the Connections Pane, forcing REST API (bypassing ADBC).
#' ADBC (Go driver) can return incorrect results for SHOW metadata commands.
#' @noRd
.pane_query <- function(conn, sql) {
  old <- getOption("RSnowflake.backend")
  options(RSnowflake.backend = "rest")
  on.exit(options(RSnowflake.backend = old), add = TRUE)
  dbGetQuery(conn, sql)
}

#' List objects for the Connections Pane hierarchy
#' @noRd
.pane_list_objects <- function(conn, database, schema) {
  empty <- data.frame(name = character(0), type = character(0))

  if (is.null(database)) {
    df <- tryCatch(.pane_query(conn, "SHOW DATABASES"), error = function(e) NULL)
    if (is.null(df) || nrow(df) == 0L) return(empty)
    name_col <- which(tolower(names(df)) == "name")
    if (length(name_col) == 0L) return(empty)
    return(data.frame(name = df[[name_col[1]]], type = "database"))
  }

  if (is.null(schema)) {
    safe_db <- toupper(gsub('"', '', database))
    df <- tryCatch(
      .pane_query(conn, paste0('SHOW SCHEMAS IN DATABASE "', safe_db, '"')),
      error = function(e) {
        tryCatch(
          .pane_query(conn, paste0(
            "SELECT SCHEMA_NAME AS NAME FROM \"", safe_db,
            "\".INFORMATION_SCHEMA.SCHEMATA ",
            "WHERE CATALOG_NAME = '", safe_db, "' ",
            "AND SCHEMA_NAME != 'INFORMATION_SCHEMA' ",
            "ORDER BY SCHEMA_NAME")),
          error = function(e2) NULL
        )
      }
    )
    if (is.null(df) || nrow(df) == 0L) return(empty)
    name_col <- which(tolower(names(df)) == "name")
    if (length(name_col) == 0L) return(empty)
    schemas <- df[[name_col[1]]]
    schemas <- schemas[toupper(schemas) != "INFORMATION_SCHEMA"]
    if (length(schemas) == 0L) return(empty)
    return(data.frame(name = schemas, type = "schema"))
  }

  safe_db  <- toupper(gsub('"', '', database))
  safe_sch <- toupper(gsub('"', '', schema))
  df <- tryCatch(
    .pane_query(conn, paste0('SHOW OBJECTS IN SCHEMA "', safe_db, '"."', safe_sch, '"')),
    error = function(e) {
      tryCatch(
        .pane_query(conn, paste0("SELECT TABLE_NAME AS NAME FROM \"",
                                 safe_db, "\".INFORMATION_SCHEMA.TABLES ",
                                 "WHERE TABLE_SCHEMA = '", safe_sch, "' ",
                                 "ORDER BY TABLE_NAME")),
        error = function(e2) NULL
      )
    }
  )
  if (is.null(df) || nrow(df) == 0L) return(empty)
  name_col <- which(tolower(names(df)) == "name")
  if (length(name_col) == 0L) return(empty)
  data.frame(name = df[[name_col[1]]], type = "table")
}

#' List columns for the Connections Pane column preview
#' @noRd
.pane_list_columns <- function(conn, database, schema, table) {
  fqn <- paste0(
    dbQuoteIdentifier(conn, database), ".",
    dbQuoteIdentifier(conn, schema), ".",
    dbQuoteIdentifier(conn, table)
  )
  df <- .pane_query(conn, paste0("SHOW COLUMNS IN TABLE ", fqn))
  if (nrow(df) == 0L) {
    return(data.frame(name = character(0), type = character(0)))
  }
  name_col <- which(tolower(names(df)) == "column_name")
  type_col <- which(tolower(names(df)) == "data_type")
  if (length(name_col) == 0L) {
    return(data.frame(name = character(0), type = character(0)))
  }
  data.frame(
    name = df[[name_col]],
    type = if (length(type_col) > 0L) df[[type_col]] else "unknown"
  )
}

#' Preview table data for the Connections Pane
#' @noRd
.pane_preview <- function(conn, database, schema, table, rowLimit = 100L) {
  fqn <- paste0(
    dbQuoteIdentifier(conn, database), ".",
    dbQuoteIdentifier(conn, schema), ".",
    dbQuoteIdentifier(conn, table)
  )
  .pane_query(conn, paste0("SELECT * FROM ", fqn, " LIMIT ", as.integer(rowLimit)))
}
