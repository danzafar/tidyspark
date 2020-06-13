# there's not much we can do in terms of avoiding namespace conflicts
# with these functions, so instead we have to provide back-functionality
#' @include columns.R

#' @title Window functions for Column operations
#'
#' @description Window functions defined for \code{Column}.
#'
#' @param x In \code{lag}, \code{lead}, and \code{cume_dist}, it is the column
#'          as a character string or a Column to compute on. In \code{ntile},
#'          it is the number of ntile groups.
#' @param ... additional argument(s).
#' @name column_window_functions
#' @rdname column_window_functions
#' @family window functions
#' @examples
#' \dontrun{
#' # Dataframe used throughout this doc
#' df <- createDataFrame(cbind(model = rownames(mtcars), mtcars))
#' ws <- orderBy(windowPartitionBy("am"), "hp")
#' tmp <- mutate(df,
#'               rank = over(rank(), ws),
#'               dense_rank = over(dense_rank(), ws),
#'               percent_rank = over(percent_rank(), ws),
#'               dist = over(cume_dist(), ws),
#'               row_number = over(row_number(), ws))
#'               lag = over(lag(df$mpg), ws),
#'               lead = over(lead(df$mpg, 1), ws),
#' # Get ntile group id (1-4) for hp
#' tmp <- mutate(tmp, ntile = over(ntile(4), ws))
#' head(tmp)}
NULL

# rank -----------------------------------------------------------------
#' @details
#' \code{rank}: Returns the rank of rows within a window partition.
#' The difference between rank and dense_rank is that dense_rank leaves no gaps in ranking
#' sequence when there are ties. That is, if you were ranking a competition using dense_rank
#' and had three people tie for second place, you would say that all three were in second
#' place and that the next person came in third. Rank would give me sequential numbers, making
#' the person that came in third place (after the ties) would register as coming in fifth.
#' This is equivalent to the \code{RANK} function in SQL.
#' This can be used with either a \code{Column} or a \code{WindowSpec}.
#'
#' @rdname column_window_functions
#' @aliases rank
#' @note rank since 1.6.0
rank <- function(x, ...) {
  UseMethod("rank")
}

rank.WindowSpec <-
  function(x = windowOrderBy(monotonically_increasing_id())) {
  jc <- call_static("org.apache.spark.sql.functions", "rank")
  new("Column", call_method(jc, "over", x@sws))
}

rank.Column <- function(x, ...) {
  quos <- enquos(...)
  if (!(rlang::quo_name(quos$ties.method) %in% c("NULL", "min")) |
      !(rlang::quo_name(quos$na.last) %in% c("NULL", "keep"))) {
    stop("Spark only supports `na.last = 'keep', ties.method = 'min'")
  }
  wndw <- call_static("org.apache.spark.sql.expressions.Window",
                                           "orderBy", list(x@jc))
  jc <- call_static("org.apache.spark.sql.functions", "rank")
  new("Column", call_method(jc, "over", wndw))
}

rank.default <- function(x, ...) {
  base::rank(x, ...)
}

# min_rank -----------------------------------------------------------------
#' @details
#' \code{min_rank}: An alias for \code{rank}. In Spark \code{rank} and
#' functions as \code{min_rank}. This can be used with either a \code{Column}
#' or a \code{WindowSpec}.
#'
#' @rdname column_window_functions
#' @aliases rank
#' @note rank since 1.6.0
#' @export
min_rank <- function(x, ...) {
  UseMethod("min_rank")
}

min_rank.WindowSpec <-
  function(x = windowOrderBy(monotonically_increasing_id())) {
  jc <- call_static("org.apache.spark.sql.functions", "min_rank")
  new("Column", call_method(jc, "over", x@sws))
}

min_rank.Column <- function(x, ...) {
  wndw <- call_static("org.apache.spark.sql.expressions.Window",
                                           "orderBy", list(x@jc))
  jc <- call_static("org.apache.spark.sql.functions", "rank")
  new("Column", call_method(jc, "over", wndw))
}

min_rank.default <- function(x, ...) {
  dplyr::min_rank(x, ...)
}

# dense_rank -----------------------------------------------------------------
#' @details
#' \code{dense_rank}: Returns the rank of rows within a window partition,
#' without any gaps. The difference between rank and dense_rank is that
#' dense_rank leaves no gaps in ranking sequence when there are ties. That is,
#' if you were ranking a competition using dense_rank and had three people tie
#' for second place, you would say that all three were in second place and that
#' the next person came in third. Rank would give me sequential numbers, making
#' the person that came in third place (after the ties) would register as
#' coming in fifth. This is equivalent to the \code{DENSE_RANK} function in
#' SQL. This can be used with either a \code{Column} or a \code{WindowSpec}.
#'
#' @rdname column_window_functions
#' @aliases dense_rank
#' @note dense_rank since 1.6.0
#' @export
dense_rank <- function(x, ...) {
  UseMethod("dense_rank")
}

dense_rank.WindowSpec <-
  function(x = windowOrderBy(monotonically_increasing_id())) {
  jc <- call_static("org.apache.spark.sql.functions", "dense_rank")
  new("Column", call_method(jc, "over", x@sws))
}

dense_rank.Column <- function(x, ...) {
  wndw <- call_static("org.apache.spark.sql.expressions.Window",
                                           "orderBy", list(x@jc))
  jc <- call_static("org.apache.spark.sql.functions", "dense_rank")
  new("Column", call_method(jc, "over", wndw))
}

dense_rank.default <- function(x, ...) {
  dplyr::dense_rank(x, ...)
}

# percent_rank -----------------------------------------------------------------
#' @details
#' \code{percent_rank}: Returns the relative rank (i.e. percentile) of rows
#' within a window partition.
#' This is computed by: (rank of row in its partition - 1) / (number of rows in
#' the partition - 1). This is equivalent to the \code{PERCENT_RANK} function
#' in SQL. This can be used with either a \code{Column} or a \code{WindowSpec}.
#'
#' @rdname column_window_functions
#' @aliases percent_rank
#' @note percent_rank since 1.6.0
#' @export
percent_rank <- function(x, ...) {
  UseMethod("percent_rank")
}

percent_rank.WindowSpec <-
  function(x = windowOrderBy(monotonically_increasing_id())) {
  jc <- call_static("org.apache.spark.sql.functions", "percent_rank")
  new("Column", call_method(jc, "over", x@sws))
}

percent_rank.Column <- function(x, ...) {
  wndw <- call_static("org.apache.spark.sql.expressions.Window",
                      "orderBy", list(x@jc))
  jc <- call_static("org.apache.spark.sql.functions", "percent_rank")
  new("Column", call_method(jc, "over", wndw))
}

percent_rank.default <- function(x, ...) {
  dplyr::percent_rank(x, ...)
}

# cume_dist --------------------------------------------------------------------
#' @details
#' \code{cume_dist}: Returns the cumulative distribution of values within a
#' window partition, i.e. the fraction of rows that are below the current row:
#' (number of values before and including x) / (total number of rows in the
#' partition). This is equivalent to the \code{CUME_DIST} function in SQL.
#' This can be used with either a \code{Column} or a \code{WindowSpec}.
#'
#' @rdname column_window_functions
#' @aliases cume_dist
#' @note cume_dist since 1.6.0
#' @export
cume_dist <- function(x, ...) {
  UseMethod("cume_dist")
}

cume_dist.default <- function(x, ...) {
  dplyr::cume_dist(x, ...)
}

cume_dist.WindowSpec <-
  function(x = windowOrderBy(monotonically_increasing_id())) {
  jc <- call_static("org.apache.spark.sql.functions", "cume_dist")
  new("Column", call_method(jc, "over", x@sws))
}

cume_dist.Column <- function(x, ...) {
  jc <- call_static("org.apache.spark.sql.functions", "cume_dist")
  wndw <- windowOrderBy(x)
  new("Column", call_method(jc, "over", wndw@sws))
}

# row_number -------------------------------------------------------------------
#' @details
#' \code{row_number}: Returns a sequential number starting at 1 within a window
#' partition. This is equivalent to the \code{ROW_NUMBER} function in SQL.
#' This can be used with either a \code{Column}, \code{WindowSpec}, or without
#' an argument, which will order by \code{monotonically_increasing_id()}.
#'
#' @rdname column_window_functions
#' @aliases row_number
#' @note row_number since 1.6.0
row_number <- function(...) {
  UseMethod("row_number")
}

row_number.default <- function(...) {
  dplyr::row_number(...)
}

# since this function is often used without an argument, I have included it as
# one of the hidden functions, see hidden_functions.R

# ntile ------------------------------------------------------------------------
#' @details
#' \code{ntile}: Returns the ntile group id (from 1 to n inclusive) in an
#' ordered window partition. For example, if n is 4, the first quarter of the
#' rows will get value 1, the second quarter will get 2, the third quarter will
#' get 3, and the last quarter will get 4. This is equivalent to the
#' \code{NTILE} function in SQL.
#'
#' @rdname column_window_functions
#' @aliases ntile
#' @note ntile since 1.6.0
ntile <- function(...) {
  UseMethod("ntile")
}

ntile.default <- function(...) {
  dplyr::ntile(...)
}

ntile.WindowSpec <-
  function(x = windowOrderBy(monotonically_increasing_id()), n) {
  stopifnot(inherits(n, "numeric"))

  jc <- call_static("org.apache.spark.sql.functions", "ntile",
                    num_to_int(n))

  new("Column", call_method(jc, "over", x@sws))
}

ntile.Column <- function(x, n) {
  stopifnot(inherits(n, "numeric"))

  window = windowOrderBy(x)

  jc <- call_static("org.apache.spark.sql.functions", "ntile",
                    num_to_int(n))

  new("Column", call_method(jc, "over", window@sws))
}
