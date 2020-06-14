#' Computes a pair-wise frequency table of the given columns
#'
#' Computes a pair-wise frequency table of the given columns. Also known as a contingency
#' table. The number of distinct values for each column should be less than 1e4. At most 1e6
#' non-zero pair frequencies will be returned.
#'
#' @param x a spark_tbl
#' @param col1 name of the first column. Distinct items will make the first item of each row.
#' @param col2 name of the second column. Distinct items will make the column names of the output.
#' @return a local R data.frame representing the contingency table. The first column of each row
#'         will be the distinct values of \code{col1} and the column names will be the distinct
#'         values of \code{col2}. The name of the first column will be "\code{col1}_\code{col2}".
#'         Pairs that have no occurrences will have zero as their counts.
#'
#' @rdname crosstab
#' @name crosstab
#' @aliases crosstab,spark_tbl,character,character-method
#' @family stat functions
#' @examples
#' \dontrun{
#' df <- read.json("/path/to/file.json")
#' ct <- crosstab(df, "title", "gender")
#' }
#' @note crosstab since 1.5.0
crosstab <- function(x, col1, col2) {
  stopifnot(inherits(x, "spark_tbl"))
  statFunctions <- call_method(attr(x, "jc"), "stat")
  sct <- call_method(statFunctions, "crosstab", col1, col2)
  collect(new_spark_tbl(sct))
}

#' Covariance on a Spark DataFrame
#'
#' @details
#' \code{cov}: When applied to spark_tbl, this calculates the sample
#' covariance of two numerical columns of \emph{one} spark_tbl.
#'
#' @param x a \code{spark_tbl}
#' @param colName1 the name of the first column
#' @param colName2 the name of the second column
#' @return The covariance of the two columns.
#'
#' @aliases cov
#' @family stat functions
#' @examples
#'
#' \dontrun{
#' cov(df, "mpg", "hp")
#' cov(df, df$mpg, df$hp)}
#' @note cov since 1.6.0
covariance <- function(x, colName1, colName2) {
  stopifnot(inherits(x, "spark_tbl"))
  stopifnot(class(colName1) == "character" && class(colName2) == "character")
  statFunctions <- call_method(attr(x, "jc"), "stat")
  call_method(statFunctions, "cov", colName1, colName2)
}

#' Calculates the correlation of two columns of a spark_tbl.
#' Currently only supports the Pearson Correlation Coefficient.
#' For Spearman Correlation, consider using RDD methods found in MLlib's Statistics.
#'
#' @param colName1 the name of the first column
#' @param colName2 the name of the second column
#' @param method Optional. A character specifying the method for calculating the correlation.
#'               only "pearson" is allowed now.
#' @return The Pearson Correlation Coefficient as a Double.
#'
#' @name corr
#' @aliases corr
#' @family stat functions
#' @examples
#'
#' \dontrun{
#' corr(df, "mpg", "hp")
#' corr(df, "mpg", "hp", method = "pearson")}
#' @note corr since 1.6.0
correlation <- function(x, colName1, colName2, method = "pearson") {
  stopifnot(inherits(x, "spark_tbl"))
  stopifnot(class(colName1) == "character" && class(colName2) == "character")
  statFunctions <- call_method(attr(x, "jc"), "stat")
  call_method(statFunctions, "corr", colName1, colName2, method)
}


#' Finding frequent items for columns, possibly with false positives
#'
#' Finding frequent items for columns, possibly with false positives.
#' Using the frequent element count algorithm described in
#' \url{https://doi.org/10.1145/762471.762473}, proposed by Karp, Schenker, and Papadimitriou.
#'
#' @param x A spark_tbl.
#' @param cols A vector column names to search frequent items in.
#' @param support (Optional) The minimum frequency for an item to be considered \code{frequent}.
#'                Should be greater than 1e-4. Default support = 0.01.
#' @return a local R data.frame with the frequent items in each column
#'
#' @rdname freqItems
#' @name freqItems
#' @aliases freqItems,spark_tbl,character-method
#' @family stat functions
#' @examples
#' \dontrun{
#' df <- read.json("/path/to/file.json")
#' fi = freqItems(df, c("title", "gender"))
#' }
#' @note freqItems since 1.6.0
freqItems <- function(x, cols, support = 0.01) {
  stopifnot(inherits(x, "spark_tbl"))
  statFunctions <- call_method(attr(x, "jc"), "stat")
  sct <- call_method(statFunctions, "freqItems", as.list(cols), support)
  collect(new_spark_tbl(sct))
}

#' Calculates the approximate quantiles of numerical columns of a spark_tbl
#'
#' Calculates the approximate quantiles of numerical columns of a spark_tbl.
#' The result of this algorithm has the following deterministic bound:
#' If the spark_tbl has N elements and if we request the quantile at probability p up to
#' error err, then the algorithm will return a sample x from the spark_tbl so that the
#' *exact* rank of x is close to (p * N). More precisely,
#'   floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).
#' This method implements a variation of the Greenwald-Khanna algorithm (with some speed
#' optimizations). The algorithm was first present in [[https://doi.org/10.1145/375663.375670
#' Space-efficient Online Computation of Quantile Summaries]] by Greenwald and Khanna.
#' Note that NA values will be ignored in numerical columns before calculation. For
#'   columns only containing NA values, an empty list is returned.
#'
#' @param x A spark_tbl.
#' @param cols A single column name, or a list of names for multiple columns.
#' @param probabilities A list of quantile probabilities. Each number must belong to [0, 1].
#'                      For example 0 is the minimum, 0.5 is the median, 1 is the maximum.
#' @param relativeError The relative target precision to achieve (>= 0). If set to zero,
#'                      the exact quantiles are computed, which could be very expensive.
#'                      Note that values greater than 1 are accepted but give the same result as 1.
#' @return The approximate quantiles at the given probabilities. If the input is a single column
#'         name, the output is a list of approximate quantiles in that column; If the input is
#'         multiple column names, the output should be a list, and each element in it is a list of
#'         numeric values which represents the approximate quantiles in corresponding column.
#'
#' @rdname approxQuantile
#' @name approxQuantile
#' @aliases approxQuantile,spark_tbl,character,numeric,numeric-method
#' @family stat functions
#' @examples
#' \dontrun{
#' df <- read.json("/path/to/file.json")
#' quantiles <- approxQuantile(df, "key", c(0.5, 0.8), 0.0)
#' }
#' @note approxQuantile since 2.0.0
approxQuantile <- function(x, cols, probabilities, relativeError) {
  stopifnot(inherits(x, "spark_tbl"))
  statFunctions <- call_method(attr(x, "jc"), "stat")
  quantiles <- call_method(statFunctions, "approxQuantile", as.list(cols),
                           as.list(probabilities), relativeError)
  if (length(cols) == 1) {
    quantiles[[1]]
  } else {
    quantiles
  }
}

#' Returns a stratified sample without replacement
#'
#' Returns a stratified sample without replacement based on the fraction given on each
#' stratum.
#'
#' @param x A spark_tbl
#' @param col column that defines strata
#' @param fractions A named list giving sampling fraction for each stratum. If a stratum is
#'                  not specified, we treat its fraction as zero.
#' @param seed random seed
#' @return A new spark_tbl that represents the stratified sample
#'
#' @rdname sampleBy
#' @aliases sampleBy,spark_tbl,character,list,numeric-method
#' @name sampleBy
#' @family stat functions
#' @examples
#'\dontrun{
#' df <- read.json("/path/to/file.json")
#' sample <- sampleBy(df, "key", fractions, 36)
#' }
#' @note sampleBy since 1.6.0
sampleBy <- function(x, col, fractions, seed) {
  stopifnot(inherits(x, "spark_tbl"))
  fractionsEnv <- convertNamedListToEnv(fractions)

  statFunctions <- call_method(attr(x, "jc"), "stat")
  # Seed is expected to be Long on Scala side, here convert it to an integer
  # due to SerDe limitation now.
  sdf <- call_method(statFunctions, "sampleBy", col, fractionsEnv, as.integer(seed))
  new_spark_tbl(sdf)
}
