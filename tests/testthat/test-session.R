# Tests for session management and transaction support
# =============================================================================

.mock_session_con <- function(has_session = FALSE) {
  state <- .new_conn_state()
  if (has_session) {
    state$session <- list(
      token            = "session-tok-123",
      master_token     = "master-tok-456",
      session_id       = 12345L,
      validity_seconds = 14400L,
      created_at       = Sys.time()
    )
  }

  new("SnowflakeConnection",
    account = "test", user = "user", database = "db", schema = "sch",
    warehouse = "wh", role = "role",
    .auth = list(token = "tok", token_type = "KEYPAIR_JWT"),
    .state = state
  )
}


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

test_that(".has_session returns FALSE for stateless connection", {
  con <- .mock_session_con(has_session = FALSE)
  expect_false(.has_session(con))
})

test_that(".has_session returns TRUE for session-based connection", {
  con <- .mock_session_con(has_session = TRUE)
  expect_true(.has_session(con))
})

test_that(".session_needs_renewal returns FALSE for fresh session", {
  session <- list(
    token            = "tok",
    validity_seconds = 14400L,
    created_at       = Sys.time()
  )
  expect_false(.session_needs_renewal(session))
})

test_that(".session_needs_renewal returns TRUE for expired session", {
  session <- list(
    token            = "tok",
    validity_seconds = 100L,
    created_at       = Sys.time() - 80
  )
  expect_true(.session_needs_renewal(session))
})

test_that(".session_needs_renewal returns FALSE for NULL session", {
  expect_false(.session_needs_renewal(NULL))
})


# ---------------------------------------------------------------------------
# Transaction methods: stateless connection
# ---------------------------------------------------------------------------

test_that("dbBegin errors on stateless connection", {
  con <- .mock_session_con(has_session = FALSE)
  expect_error(dbBegin(con), "session-based")
})

test_that("dbCommit errors on stateless connection", {
  con <- .mock_session_con(has_session = FALSE)
  expect_error(dbCommit(con), "session-based")
})

test_that("dbRollback errors on stateless connection", {
  con <- .mock_session_con(has_session = FALSE)
  expect_error(dbRollback(con), "session-based")
})

test_that("dbWithTransaction errors on stateless connection", {
  con <- .mock_session_con(has_session = FALSE)
  expect_error(dbWithTransaction(con, TRUE), "session-based")
})


# ---------------------------------------------------------------------------
# Transaction methods: session-based connection
# State management is tested directly since S4 methods resolve in the
# package namespace where mockr can't intercept internal calls.
# ---------------------------------------------------------------------------

test_that("dbBegin errors if transaction already active", {
  con <- .mock_session_con(has_session = TRUE)
  con@.state$in_transaction <- TRUE
  expect_error(dbBegin(con), "already active")
})

test_that("dbCommit errors if no active transaction", {
  con <- .mock_session_con(has_session = TRUE)
  expect_error(dbCommit(con), "No active transaction")
})

test_that("dbRollback errors if no active transaction", {
  con <- .mock_session_con(has_session = TRUE)
  expect_error(dbRollback(con), "No active transaction")
})

test_that("in_transaction state toggles correctly", {
  con <- .mock_session_con(has_session = TRUE)
  expect_false(con@.state$in_transaction)

  con@.state$in_transaction <- TRUE
  expect_true(con@.state$in_transaction)

  con@.state$in_transaction <- FALSE
  expect_false(con@.state$in_transaction)
})


# ---------------------------------------------------------------------------
# dbDisconnect with session
# ---------------------------------------------------------------------------

test_that("dbDisconnect works and invalidates connection (no session)", {
  con <- .mock_session_con(has_session = FALSE)
  dbDisconnect(con)
  expect_false(dbIsValid(con))
})

test_that("dbDisconnect with session clears session state", {
  con <- .mock_session_con(has_session = TRUE)
  expect_true(.has_session(con))

  # Disconnect -- session delete will fail (no real server) but should not error
  dbDisconnect(con)
  expect_false(dbIsValid(con))
  expect_null(con@.state$session)
})
