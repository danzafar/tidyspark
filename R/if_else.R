#' @export
if_else <- function(...) {
  UseMethod("if_else")
}

# grab_groups_from_mask <- function(env = parent.frame(), name = ".tbl") {
#   .data <- env$.top_env[[name]]
#   if (is.null(.data)) stop("Was not able to find ", name, " in the .top_env")
#   attr(env$.top_env[[name]], ".groups")
# }

#' @export
if_else.Column <- function(condition, yes, no) {

  condition <- condition@jc

  if (is_agg_expr(yes) | is_agg_expr(no)) {
    stop("tidyspark is not yet sophisticated enough to process aggregates",
         "for the 'yes' and 'no' arguments in 'if_else'. As a workaround,",
         "you can create a new column with this value and then reference that",
         "so 'if_else(.p, max(a), b)' => 'if_else(.p, new_col, b)'")
  }

  # parse the predicate to see if the expressions are grouped
  eq_expr <- call_method(call_method(condition, "expr"), "children")
  first <- spark_class(call_method(eq_expr, "head"))
  second <- spark_class(call_method(
    call_method(eq_expr, "tail"),
    "head"))
  predicate_agg <- grepl("expressions\\.aggregate", c(first, second))

  agg_error <- if (any(predicate_agg)) T
  else {
    # if passed from case_when it has a different architecture
    eq_expr_cw <- call_method(call_method(eq_expr, "head"), "children")
    if (spark_class(eq_expr_cw, T) != "Nil$") {
      first_cw <- spark_class(call_method(eq_expr_cw, "head"))
      second_cw <- spark_class(call_method(
        call_method(eq_expr_cw, "tail"),
        "head"))
      any(grepl("expressions\\.aggregate", c(first_cw, second_cw)))
    } else F
  }

  if (agg_error) {
    stop("tidyspark is not yet sophisticated enough to process aggregates",
         "in the predicate given to if_else or case_when. As a workaround,",
         "you can create a new column with this value and then reference that",
         "so 'if_else(max(a) > b, c, d)' => 'if_else(new_col > b, c, d)'")
  }


  if (inherits(yes, "Column")) yes <- yes@jc
  if (inherits(no, "Column")) no <- no@jc

  jc <- call_method(call_static("org.apache.spark.sql.functions",
                                "when", condition, yes), "otherwise", no)
  new("Column", jc)
}

if_else.default <- function(...) dplyr::if_else(...)


#' A general vectorised if
#'
#' This function allows you to vectorise multiple [if_else()]
#' statements. It is an R equivalent of the SQL `CASE WHEN` statement.
#' If no cases match, `NA` is returned.
#'
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> A sequence of two-sided
#'   formulas. The left hand side (LHS) determines which values match this case.
#'   The right hand side (RHS) provides the replacement value.
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
#' @export
case_when <- function(...) {
  fs <- dplyr:::compact_null(rlang::list2(...))
  if (length(fs) == 0) {
    abort("No cases provided")
  }

  quos_pairs <- mapply(function(x, y) {
    dplyr:::validate_formula(x, y, default_env, current_env())
  }, fs, seq_along(fs),
  SIMPLIFY = F)

  default_env <- rlang::caller_env()
  classes <- lapply(quos_pairs, function(x)
    rlang::eval_tidy(x$lhs, env = default_env))
  is_Column <- sapply(classes, function(x) inherits(x, "Column"))

  if (any(is_Column)) case_when.Column(...)
  else dplyr:::case_when(...)

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

  quos_pairs <- mapply(function(x, y) {
    dplyr:::validate_formula(x, y, default_env, current_env())},
                       fs, seq_along(fs),
    SIMPLIFY = F)

  for (i in seq_len(n)) {
    pair <- quos_pairs[[i]]
    query[[i]] <- rlang::eval_tidy(pair$lhs, env = default_env)
    value[[i]] <- rlang::eval_tidy(pair$rhs, env = default_env)
    if (!is.logical(query[[i]])) {
      allowed <- c("GreaterThan", "LessThan", "GreaterThanOrEqual",
                   "LessThanOrEqual", "EqualTo", "EqualNullSafe", "Not")
      class_in <- spark_class(query[[i]], T)
      if (!(class_in %in% allowed)) {
        stop("LHS of case ", i, " must be a Column expression class matching ",
             "one of: \n'", paste0(allowed, collapse = "', '"),
             "'. Yours is: '", class_in, "'.")
      }

    }
  }

  out <- lit(value[[1]])
  replaced <- lit(FALSE)
  for (i in seq_len(n)) {
    out <- if_else(lit(query[[i]]) & !replaced, value[[i]], out)
    replaced <- replaced | (lit(query[[i]]) & !is.na(lit(query[[i]])))
  }
  out
}
