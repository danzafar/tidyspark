#' #' @export
#' factorial <- function(...) {
#'   UseMethod("factorial")
#' }
#'
#' #' @importFrom base factorial
#' factorial.default <- function(...) {
#'   base::factorial(...)
#' }
