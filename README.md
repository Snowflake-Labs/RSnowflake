# RSnowflake

A DBI-compliant R interface to [Snowflake](https://www.snowflake.com) that
connects directly via the Snowflake SQL API over HTTPS. No dependency on
ODBC, JDBC, or Python.

> **Companion package:** For Snowflake ML features (Model Registry, Feature
> Store, Datasets, SPCS model serving), see
> [**snowflakeR**](https://github.com/Snowflake-Labs/snowflakeR).

## Features

- Full [DBI](https://dbi.r-dbi.org/) compliance -- works with any package
  that speaks DBI
- **dbplyr** integration -- write dplyr pipelines that execute as Snowflake SQL
- **RStudio / Positron Connections Pane** -- browse databases, schemas, tables,
  and columns visually
- **Authentication**: JWT key-pair, Programmatic Access Token (PAT), OAuth,
  and Workspace session tokens
- **`connections.toml`** support -- share profiles with the Snowflake Python
  connector and CLI
- **Arrow interface** -- `dbGetQueryArrow()` / `dbFetchArrow()` via `nanoarrow`
  for DBI Arrow method compatibility
- **Snowflake Workspace Notebooks** -- auto-detects the session token for
  zero-config auth

## Installation

```r
# install.packages("pak")
pak::pak("Snowflake-Labs/RSnowflake")
```

## Quick Start

```r
library(DBI)
library(RSnowflake)

# Connect using a connections.toml profile
con <- dbConnect(Snowflake(), name = "my_profile")

# Run a query
df <- dbGetQuery(con, "SELECT * FROM my_table LIMIT 10")

# Write a data.frame
dbWriteTable(con, "iris_copy", iris)

# dbplyr
library(dplyr)
tbl(con, "my_table") |>
  filter(score > 90) |>
  collect()

# Disconnect
dbDisconnect(con)
```

## Authentication

### connections.toml (recommended)

```toml
# ~/.snowflake/connections.toml
[default]
account   = "myaccount"
user      = "myuser"
authenticator = "SNOWFLAKE_JWT"
private_key_path = "/path/to/rsa_key.p8"
database  = "MY_DB"
schema    = "PUBLIC"
warehouse = "MY_WH"
```

```r
con <- dbConnect(Snowflake())                    # uses [default]
con <- dbConnect(Snowflake(), name = "staging")  # uses [staging]
```

### Programmatic Access Token (PAT)

```r
con <- dbConnect(
  Snowflake(),
  account = "myaccount",
  token   = Sys.getenv("SNOWFLAKE_PAT")
)
```

### Snowflake Workspace Notebooks

Inside a Workspace Notebook, authentication is automatic:

```r
con <- dbConnect(Snowflake())
```

## Identifier Case Handling

By default, RSnowflake uppercases unquoted identifiers in DDL/DML
operations to match Snowflake's native behaviour and the ODBC driver:

```r
dbWriteTable(con, "my_table", data.frame(id = 1))
dbListFields(con, "my_table")
#> [1] "ID"
```

Set `options(RSnowflake.identifier_case = "preserve")` to keep the
original case (useful for DBItest or when lowercase column names are
required).

## Using with snowflakeR

If you use the companion `snowflakeR` package for ML workflows, you can
obtain an RSnowflake connection from an existing `sfr_connection`:

```r
library(snowflakeR)
conn    <- sfr_connect()
dbi_con <- sfr_dbi_connection(conn)

DBI::dbGetQuery(dbi_con, "SELECT 1")
```

## Requirements

- R >= 4.1.0
- A Snowflake account

Optional R packages: `openssl` + `jose` (JWT auth), `RcppTOML`
(connections.toml), `nanoarrow` (Arrow interface), `dbplyr` + `dplyr`,
`snowflakeauth`.

## License

Apache License 2.0
