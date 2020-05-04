#' A general vectorised if
#'
#' This function allows you to vectorise multiple [if_else()]
#' statements. It is an R equivalent of the SQL `CASE WHEN` statement.
#' If no cases match, `NA` is returned.
#'
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> A sequence of two-sided formulas. The left hand side (LHS)
#'   determines which values match this case. The right hand side (RHS)
#'   provides the replacement value.
#'
#'   The LHS must evaluate to a logical vector. The RHS does not need to be
#'   logical, but all RHSs must evaluate to the same type of vector.
#'
#'   Both LHS and RHS may have the same length of either 1 or `n`. The
#'   value of `n` must be consistent across all cases. The case of
#'   `n == 0` is treated as a variant of `n != 1`.
#'
#'   `NULL` inputs are ignored.
#' @export
#' @return A vector of length 1 or `n`, matching the length of the logical
#'   input or output vectors, with the type (and attributes) of the first
#'   RHS. Inconsistent lengths or types will generate an error.
#' @examples
#' x <- 1:50
#' case_when(
#'   x %% 35 == 0 ~ "fizz buzz",
#'   x %% 5 == 0 ~ "fizz",
#'   x %% 7 == 0 ~ "buzz",
#'   TRUE ~ as.character(x)
#' )
#'
#' # Like an if statement, the arguments are evaluated in order, so you must
#' # proceed from the most specific to the most general. This won't work:
#' case_when(
#'   TRUE ~ as.character(x),
#'   x %%  5 == 0 ~ "fizz",
#'   x %%  7 == 0 ~ "buzz",
#'   x %% 35 == 0 ~ "fizz buzz"
#' )
#'
#' # If none of the cases match, NA is used:
#' case_when(
#'   x %%  5 == 0 ~ "fizz",
#'   x %%  7 == 0 ~ "buzz",
#'   x %% 35 == 0 ~ "fizz buzz"
#' )
#'
#' # Note that NA values in the vector x do not get special treatment. If you want
#' # to explicitly handle NA values you can use the `is.na` function:
#' x[2:4] <- NA_real_
#' case_when(
#'   x %% 35 == 0 ~ "fizz buzz",
#'   x %% 5 == 0 ~ "fizz",
#'   x %% 7 == 0 ~ "buzz",
#'   is.na(x) ~ "nope",
#'   TRUE ~ as.character(x)
#' )
#'
#' # All RHS values need to be of the same type. Inconsistent types will throw an error.
#' # This applies also to NA values used in RHS: NA is logical, use
#' # typed values like NA_real_, NA_complex, NA_character_, NA_integer_ as appropriate.
#' case_when(
#'   x %% 35 == 0 ~ NA_character_,
#'   x %% 5 == 0 ~ "fizz",
#'   x %% 7 == 0 ~ "buzz",
#'   TRUE ~ as.character(x)
#' )
#' case_when(
#'   x %% 35 == 0 ~ 35,
#'   x %% 5 == 0 ~ 5,
#'   x %% 7 == 0 ~ 7,
#'   TRUE ~ NA_real_
#' )
#'
#' # case_when() evaluates all RHS expressions, and then constructs its
#' # result by extracting the selected (via the LHS expressions) parts.
#' # In particular NaN are produced in this case:
#' y <- seq(-2, 2, by = .5)
#' case_when(
#'   y >= 0 ~ sqrt(y),
#'   TRUE   ~ y
#' )
#'
#' # This throws an error as NA is logical not numeric
#' \dontrun{
#' case_when(
#'   x %% 35 == 0 ~ 35,
#'   x %% 5 == 0 ~ 5,
#'   x %% 7 == 0 ~ 7,
#'   TRUE ~ NA
#' )
#' }
#'
#' # case_when is particularly useful inside mutate when you want to
#' # create a new variable that relies on a complex combination of existing
#' # variables
#' starwars %>%
#'   select(name:mass, gender, species) %>%
#'   mutate(
#'     type = case_when(
#'       height > 200 | mass > 200 ~ "large",
#'       species == "Droid"        ~ "robot",
#'       TRUE                      ~ "other"
#'     )
#'   )
#'
#'
#' # `case_when()` is not a tidy eval function. If you'd like to reuse
#' # the same patterns, extract the `case_when()` call in a normal
#' # function:
#' case_character_type <- function(height, mass, species) {
#'   case_when(
#'     height > 200 | mass > 200 ~ "large",
#'     species == "Droid"        ~ "robot",
#'     TRUE                      ~ "other"
#'   )
#' }
#'
#' case_character_type(150, 250, "Droid")
#' case_character_type(150, 150, "Droid")
#'
#' # Such functions can be used inside `mutate()` as well:
#' starwars %>%
#'   mutate(type = case_character_type(height, mass, species)) %>%
#'   pull(type)
#'
#' # `case_when()` ignores `NULL` inputs. This is useful when you'd
#' # like to use a pattern only under certain conditions. Here we'll
#' # take advantage of the fact that `if` returns `NULL` when there is
#' # no `else` clause:
#' case_character_type <- function(height, mass, species, robots = TRUE) {
#'   case_when(
#'     height > 200 | mass > 200      ~ "large",
#'     if (robots) species == "Droid" ~ "robot",
#'     TRUE                           ~ "other"
#'   )
#' }
#'
#' starwars %>%
#'   mutate(type = case_character_type(height, mass, species, robots = FALSE)) %>%
#'   pull(type)
#'
#'
#'

case_when <- function(...) {
  UseMethod("case_when")
}
case_when.default <- function(...) {
  dplyr:::case_when(...)
}
case_when.Column <- function (...) {
  fs <- dplyr:::compact_null(rlang::list2(...))
  n <- length(fs)
  if (n == 0) {
    abort("No cases provided")
  }
  query <- vector("list", n)
  value <- vector("list", n)
  default_env <- rlang::caller_env()
  quos_pairs <- purrr::map2(fs, seq_along(fs), dplyr:::validate_formula,
                     default_env, current_env())
  for (i in seq_len(n)) {
    pair <- quos_pairs[[i]]
    query[[i]] <- rlang::eval_tidy(pair$lhs, env = default_env)
    value[[i]] <- rlang::eval_tidy(pair$rhs, env = default_env)
    if (!is.logical(query[[i]])) {
      #dplyr:::abort_case_when_logical(pair$lhs, i, query[[i]])
      message('Working on this')
    }
  }
  # not need if only called from a data_frame or data_frame like context
  #m <- dplyr:::validate_case_when_length(query, value, fs)
  #out <- value[[1]][rep(NA_integer_, m)]


  out <- lit(value[[1]])
  replaced <- lit(FALSE)
  for (i in seq_len(n)) {
    # out <- replace_with(out, query[[i]] & !replaced, value[[i]],
    #                     NULL)
    out <- ifelse(lit(query[[i]]) & !replaced, value[[i]], out)
    replaced <- replaced | (lit(query[[i]]) & !is.na(lit(query[[i]])))

  }
  out
}
