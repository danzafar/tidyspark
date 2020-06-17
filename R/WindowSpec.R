
#' @include generics.R
NULL

#' S4 class that represents a WindowSpec
#'
#' WindowSpec can be created by using windowPartitionBy() or windowOrderBy()
#'
#' @rdname WindowSpec
#' @seealso \link{windowPartitionBy}, \link{windowOrderBy}
#'
#' @param sws A Java object reference to the backing Scala WindowSpec
#' @note WindowSpec since 2.0.0
setClass("WindowSpec",
         slots = list(sws = "jobj"))

setMethod("initialize", "WindowSpec", function(.Object, sws) {
  .Object@sws <- sws
  .Object
})

windowSpec <- function(sws) {
  stopifnot(class(sws) == "jobj")
  new("WindowSpec", sws)
}

#' partitionBy
#'
#' Defines the partitioning columns in a WindowSpec.
#'
#' @param x a WindowSpec.
#' @param col a column to partition on (desribed by the name or Column).
#' @param ... additional column(s) to partition on.
#' @return A WindowSpec.
#'
#' @export
#' @rdname partitionBy
#' @name partitionBy
#' @aliases partitionBy,WindowSpec-method
#' @family windowspec_method
#' @examples
#' \dontrun{
#'   partitionBy(ws, "col1", "col2")
#'   partitionBy(ws, df$col1, df$col2)
#' }
#' @note partitionBy(WindowSpec) since 2.0.0
setMethod("partitionBy",
          signature(x = "WindowSpec"),
          function(x, col, ...) {
            stopifnot(class(col) %in% c("character", "Column"))

            if (class(col) == "character") {
              windowSpec(call_method(x@sws, "partitionBy", col, list(...)))
            } else {
              jcols <- lapply(list(col, ...), function(c) {
                c@jc
              })
              windowSpec(call_method(x@sws, "partitionBy", jcols))
            }
          })

#' Ordering Columns in a WindowSpec
#'
#' Defines the ordering columns in a WindowSpec.
#' @param x a WindowSpec
#' @param col a character or Column indicating an ordering column
#' @param ... additional sorting fields
#' @return A WindowSpec.
#'
#' @export
#' @name orderBy
#' @rdname orderBy
#' @aliases orderBy,WindowSpec,character-method
#' @family windowspec_method
#' @seealso See \link{arrange} for use in sorting a SparkDataFrame
#' @examples
#' \dontrun{
#'   orderBy(ws, "col1", "col2")
#'   orderBy(ws, df$col1, df$col2)
#' }
#' @note orderBy(WindowSpec, character) since 2.0.0
setMethod("orderBy",
          signature(x = "WindowSpec", col = "character"),
          function(x, col, ...) {
            windowSpec(call_method(x@sws, "orderBy", col, list(...)))
          })

#' @export
#' @rdname orderBy
#' @name orderBy
#' @aliases orderBy,WindowSpec,Column-method
#' @note orderBy(WindowSpec, Column) since 2.0.0
setMethod("orderBy",
          signature(x = "WindowSpec", col = "Column"),
          function(x, col, ...) {
            jcols <- lapply(list(col, ...), function(c) {
              c@jc
            })
            windowSpec(call_method(x@sws, "orderBy", jcols))
          })

#' rowsBetween
#'
#' Defines the frame boundaries, from \code{start} (inclusive) to \code{end}
#' (inclusive).
#'
#' Both \code{start} and \code{end} are relative positions from the current
#' row. For example, "0" means "current row", while "-1" means the row before
#' the current row, and "5" means the fifth row after the current row.
#'
#' A row based boundary is based on the position of the row within the
#' partition. An offset indicates the number of rows above or below the current
#' row, the frame for the current row starts or ends. For instance, given a row
#' based sliding frame with a lower bound offset of -1 and a upper bound offset
#' of +2. The frame for row with index 5 would range from index 4 to index 6.
#'
#' @param x a WindowSpec
#' @param start boundary start, inclusive.
#'              The frame is unbounded if this is the minimum long value.
#' @param end boundary end, inclusive.
#'            The frame is unbounded if this is the maximum long value.
#'
#' @return a WindowSpec
#' @export
#' @rdname rowsBetween
#' @aliases rowsBetween,WindowSpec,numeric,numeric-method
#' @name rowsBetween
#' @family windowspec_method
#' @examples
#' \dontrun{
#'   id <- c(rep(1, 3), rep(2, 3), 3)
#'   desc <- c('New', 'New', 'Good', 'New', 'Good', 'Good', 'New')
#'   df <- data.frame(id, desc)
#'   df <- createDataFrame(df)
#'   w1 <- orderBy(windowPartitionBy('desc'), df$id)
#'   w2 <- rowsBetween(w1, 0, 3)
#'   df1 <- withColumn(df, "sum", over(sum(df$id), w2))
#'   head(df1)
#' }
#' @note rowsBetween since 2.0.0
setMethod("rowsBetween",
          signature(x = "WindowSpec", start = "numeric", end = "numeric"),
          function(x, start, end) {
            # "start" and "end" should be long, due to serde limitation,
            # limit "start" and "end" as integer now
            windowSpec(call_method(x@sws, "rowsBetween",
                                   as.integer(start), as.integer(end)))
          })

#' rangeBetween
#'
#' Defines the frame boundaries, from \code{start} (inclusive) to \code{end}
#' (inclusive).
#'
#' Both \code{start} and \code{end} are relative from the current row. For
#' example, "0" means "current row", while "-1" means one off before the
#' current row, and "5" means the five off after the current row.
#'
#' We recommend users use \code{Window.unboundedPreceding},
#' \code{Window.unboundedFollowing}, and \code{Window.currentRow} to specify
#' special boundary values, rather than using long values directly.
#'
#' A range-based boundary is based on the actual value of the ORDER BY
#' expression(s). An offset is used to alter the value of the ORDER BY
#' expression, for instance if the current ORDER BY expression has a value of
#' 10 and the lower bound offset is -3, the resulting lower bound for the
#' current row will be 10 - 3 = 7. This however puts a number of constraints
#' on the ORDER BY expressions: there can be only one expression and this
#' expression must have a numerical data type. An exception can be made when
#' the offset is unbounded, because no value modification is needed, in this
#' case multiple and non-numeric ORDER BY expression are allowed.
#'
#' @param x a WindowSpec
#' @param start boundary start, inclusive.
#'              The frame is unbounded if this is the minimum long value.
#' @param end boundary end, inclusive.
#'            The frame is unbounded if this is the maximum long value.
#'
#' @export
#' @return a WindowSpec
#' @rdname rangeBetween
#' @aliases rangeBetween,WindowSpec,numeric,numeric-method
#' @name rangeBetween
#' @family windowspec_method
#' @examples
#' \dontrun{
#'   id <- c(rep(1, 3), rep(2, 3), 3)
#'   desc <- c('New', 'New', 'Good', 'New', 'Good', 'Good', 'New')
#'   df <- data.frame(id, desc)
#'   df <- createDataFrame(df)
#'   w1 <- orderBy(windowPartitionBy('desc'), df$id)
#'   w2 <- rangeBetween(w1, 0, 3)
#'   df1 <- withColumn(df, "sum", over(sum(df$id), w2))
#'   head(df1)
#' }
#' @note rangeBetween since 2.0.0
setMethod("rangeBetween",
          signature(x = "WindowSpec", start = "numeric", end = "numeric"),
          function(x, start, end) {
            # "start" and "end" should be long, due to serde limitation,
            # limit "start" and "end" as integer now
            windowSpec(call_method(x@sws, "rangeBetween",
                                   as.integer(start), as.integer(end)))
          })

# Note that over is a method of Column class, but it is placed here to
# avoid Roxygen circular-dependency between class Column and WindowSpec.

#' over
#'
#' Define a windowing column.
#'
#' @param x a Column, usually one returned by window function(s).
#' @param window a WindowSpec object. Can be created by \code{windowPartitionBy}
#'        or \code{windowOrderBy} and configured by other WindowSpec methods.
#'
#' @export
#' @rdname over
#' @name over
#' @aliases over,Column,WindowSpec-method
#' @family colum_func
#' @examples
#' \dontrun{
#'   df <- createDataFrame(mtcars)
#'
#'   # Partition by am (transmission) and order by hp (horsepower)
#'   ws <- orderBy(windowPartitionBy("am"), "hp")
#'
#'   # Rank on hp within each partition
#'   out <- select(df, over(rank(), ws), df$hp, df$am)
#'
#'   # Lag mpg values by 1 row on the partition-and-ordered table
#'   out <- select(df, over(lead(df$mpg), ws), df$mpg, df$hp, df$am)
#' }
#' @note over since 2.0.0
setMethod("over",
          signature(x = "Column", window = "WindowSpec"),
          function(x, window) {
            new("Column", call_method(x@jc, "over", window@sws))
          })

#' windowPartitionBy
#'
#' Creates a WindowSpec with the partitioning defined.
#'
#' @param col A column name or Column by which rows are partitioned to
#'            windows.
#' @param ... Optional column names or Columns in addition to col, by
#'            which rows are partitioned to windows.
#'
#' @export
#' @rdname windowPartitionBy
#' @name windowPartitionBy
#' @aliases windowPartitionBy,character-method
#' @examples
#' \dontrun{
#'   ws <- orderBy(windowPartitionBy("key1", "key2"), "key3")
#'   df1 <- select(df, over(lead("value", 1), ws))
#'
#'   ws <- orderBy(windowPartitionBy(df$key1, df$key2), df$key3)
#'   df1 <- select(df, over(lead("value", 1), ws))
#' }
#' @note windowPartitionBy(character) since 2.0.0
setMethod("windowPartitionBy",
          signature(col = "character"),
          function(col, ...) {
            windowSpec(
              call_static("org.apache.spark.sql.expressions.Window",
                          "partitionBy",
                          col,
                          list(...)))
          })

#' @export
#' @rdname windowPartitionBy
#' @name windowPartitionBy
#' @aliases windowPartitionBy,Column-method
#' @note windowPartitionBy(Column) since 2.0.0
setMethod("windowPartitionBy",
          signature(col = "Column"),
          function(col, ...) {
            jcols <- lapply(list(col, ...), function(c) {
              c@jc
            })
            windowSpec(
              call_static("org.apache.spark.sql.expressions.Window",
                          "partitionBy",
                          jcols))
          })

#' windowOrderBy
#'
#' Creates a WindowSpec with the ordering defined.
#'
#' @param col A column name or Column by which rows are ordered within
#'            windows.
#' @param ... Optional column names or Columns in addition to col, by
#'            which rows are ordered within windows.
#'
#' @export
#' @rdname windowOrderBy
#' @name windowOrderBy
#' @aliases windowOrderBy,character-method
#' @examples
#' \dontrun{
#'   ws <- windowOrderBy("key1", "key2")
#'   df1 <- select(df, over(lead("value", 1), ws))
#'
#'   ws <- windowOrderBy(df$key1, df$key2)
#'   df1 <- select(df, over(lead("value", 1), ws))
#' }
#' @note windowOrderBy(character) since 2.0.0
setMethod("windowOrderBy",
          signature(col = "character"),
          function(col, ...) {
            windowSpec(
              call_static("org.apache.spark.sql.expressions.Window",
                          "orderBy",
                          col,
                          list(...)))
          })

#' @export
#' @rdname windowOrderBy
#' @name windowOrderBy
#' @aliases windowOrderBy,Column-method
#' @note windowOrderBy(Column) since 2.0.0
setMethod("windowOrderBy",
          signature(col = "Column"),
          function(col, ...) {
            jcols <- lapply(list(col, ...), function(c) {
              c@jc
            })
            windowSpec(
              call_static("org.apache.spark.sql.expressions.Window",
                          "orderBy",
                          jcols))
          })
