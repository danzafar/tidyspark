
# simple dplyr functions -------------------------------------------------------

check_na_matches <- function (na_matches) {
    na_matches <- match.arg(na_matches, choices = c("na", "never"))
    accept_na_match <- (na_matches == "na")
    accept_na_match
}

match_vars <- function (vars, data) {
  if (is.numeric(vars))
    return(vars)
  match(vars, names(data))
}

compact_null <- function (x) {
  Filter(function(elt) !is.null(elt), x)
}

# for deparse names ------------------------------------------------------------

map <- function (.x, .f, ...) {
  lapply(.x, .f, ...)
}

map_if <- function (.x, .p, .f, ...) {
  matches <- probe(.x, .p)
  .x[matches] <- map(.x[matches], .f, ...)
  .x
}

map_chr <- function (.x, .f, ...) {
  map_mold(.x, .f, character(1), ...)
}

map_mold <- function (.x, .f, .mold, ...) {
  out <- vapply(.x, .f, .mold, ..., USE.NAMES = FALSE)
  names(out) <- names(.x)
  out
}

deparse_names <- function(x) {
  x <- map_if(x, rlang::is_quosure, rlang::quo_squash)
  x <- map_if(x, rlang::is_bare_formula, rlang::f_rhs)
  map_chr(x, deparse)
}

# for join_vars ----------------------------------------------------------------

get_by_aux <- function (names, by) {
  if (length(by) > 0) {
    by <- match(by, names)
    aux <- seq_along(names)[-by]
  }
  else {
    by <- integer()
    aux <- seq_along(names)
  }
  list(by = by, aux = aux)
}

get_join_var_idx <- function (x_names, y_names, by) {
  x_idx <- get_by_aux(x_names, by$x)
  y_idx <- get_by_aux(y_names, by$y)
  list(x = x_idx, y = y_idx)
}

add_suffixes <- function (x, y, suffix) {
  if (identical(suffix, "")) {
    return(x)
  }
  out <- rlang::rep_along(x, na_chr)
  for (i in seq_along(x)) {
    nm <- x[[i]]
    while (nm %in% y || nm %in% out) {
      nm <- paste0(nm, suffix)
    }
    out[[i]] <- nm
  }
  out
}

join_vars <- function (x_names, y_names, by,
                       suffix = list(x = ".x", y = ".y")) {
  idx <- get_join_var_idx(x_names, y_names, by)
  x_names_by <- x_names[idx$x$by]
  x_names_aux <- x_names[idx$x$aux]
  y_names_aux <- y_names[idx$y$aux]
  x_new <- x_names
  x_new[idx$x$aux] <- add_suffixes(x_names_aux, c(x_names_by,
                                                  y_names_aux), suffix$x)
  y_new <- add_suffixes(y_names_aux, x_names, suffix$y)
  x_x <- seq_along(x_names)
  x_y <- idx$y$by[match(x_names, by$x)]
  y_x <- rlang::rep_along(idx$y$aux, NA)
  y_y <- seq_along(idx$y$aux)
  ret <- list(alias = c(x_new, y_new), x = c(x_x, y_x), y = c(x_y,
                                                              y_y))
  ret$idx <- idx
  ret
}

# for validate_formuala --------------------------------------------------------

`%||%` <- function (x, y) {
  if (rlang::is_null(x)) y
  else x
}

validate_formula <- function (x, i, default_env, dots_env) {
  if (rlang::is_quosure(x)) {
    default_env <- rlang::quo_get_env(x)
    x <- rlang::quo_get_expr(x)
  }
  if (!rlang::is_formula(x)) {
    arg <- substitute(...(), dots_env)[[1]]
    abort("Case ", i, " (", arg, ") must be a two-sided formula")
  }
  if (rlang::is_null(rlang::f_lhs(x))) {
    abort("formulas must be two-sided")
  }
  env <- rlang::f_env(x) %||% default_env
  list(lhs = rlang::new_quosure(rlang::f_lhs(x), env),
       rhs = rlang::new_quosure(rlang::f_rhs(x), env))
}

# check_suffix -----------------------------------------------------------------

friendly_type_of <- function(x) {
  if (is.object(x)) {
    sprintf("a `%s` object", fmt_classes(x))
  } else {
    as_friendly_type(typeof(x))
  }
}

as_friendly_type <- function(type) {
  switch(type,
         logical = "a logical vector",
         integer = "an integer vector",
         numeric = ,
         double = "a double vector",
         complex = "a complex vector",
         character = "a character vector",
         raw = "a raw vector",
         string = "a string",
         list = "a list",
         NULL = "NULL",
         environment = "an environment",
         externalptr = "a pointer",
         weakref = "a weak reference",
         S4 = "an S4 object",
         name = ,
         symbol = "a symbol",
         language = "a call",
         pairlist = "a pairlist node",
         expression = "an expression vector",
         quosure = "a quosure",
         formula = "a formula",
         char = "an internal string",
         promise = "an internal promise",
         ... = "an internal dots object",
         any = "an internal `any` object",
         bytecode = "an internal bytecode object",
         primitive = ,
         builtin = ,
         special = "a primitive function",
         closure = "a function",
         type
  )
}

bad_args <- function(header, ...) {
  text <- paste0(...)
  if (!rlang::is_null(header))
    text <- paste0(header, " ", text)
  stop(text)
}

check_suffix <- function(x) {
  if (!is.character(x) || length(x) != 2) {
    browser
    bad_args("suffix", "must be a character vector of length 2, ",
             "not ", friendly_type_of(x), " of length ", length(x))
  }
  if (any(is.na(x))) {
    bad_args("suffix", "can't be NA")
  }
  if (all(x == "")) {
    bad_args("suffix", "can't be empty string for both `x` and `y` suffixes")
  }
  list(x = x[[1]], y = x[[2]])
}

# bad_eq_ops <- function (named_calls, ..., .envir = parent.frame()) {
#     glubort(fmt_wrong_eq_ops(named_calls), ..., .envir = .envir)
# }
#
# check_valid_names <- function (names, warn_only = FALSE) {
#     invisible(.Call(`_dplyr_check_valid_names`, names, warn_only))
# }
#
# # copied from https://github.com/tidyverse/dplyr/blob/master/R/error.R
# check_pkg <- function(name, reason, install = TRUE) {
#   if (is_installed(name)) {
#     return(invisible(TRUE))
#   }
#
#   glubort(NULL, "The {name} package is required to {reason}.",
#     if (install) '\nPlease install it with `install.packages("{name}")`' else ""
#   )
# }
#
# # ngettext() does extra work, this function is a simpler version
# ntext <- function(n, msg1, msg2) {
#   if (n == 1) msg1 else msg2
# }
#
# bad <- function(..., .envir = parent.frame()) {
#   glubort(NULL, ..., .envir = parent.frame())
# }
#
# bad_args <- function(args, ..., .envir = parent.frame()) {
#   glubort(fmt_args(args), ..., .envir = .envir)
# }
#
# bad_pos_args <- function(pos_args, ..., .envir = parent.frame()) {
#   glubort(fmt_pos_args(pos_args), ..., .envir = .envir)
# }
#
# bad_calls <- function(calls, ..., .envir = parent.frame()) {
#   glubort(fmt_calls(calls), ..., .envir = .envir)
# }
#
# glubort <- function(header, ..., .envir = parent.frame(), .abort = abort) {
#   text <- glue(..., .envir = .envir)
#   if (!is_null(header)) text <- paste0(header, " ", text)
#   .abort(text)
# }
#
# fmt_args <- function(x) {
#   x <- parse_args(x)
#   fmt_obj(x)
# }
#
# fmt_pos_args <- function(x) {
#   args <- ntext(length(x), "Argument", "Arguments")
#   glue("{args} {fmt_comma(x)}")
# }
#
# fmt_calls <- function(...) {
#   x <- parse_named_call(...)
#   fmt_obj(x)
# }
#
# fmt_named_calls <- function(...) {
#   x <- parse_named_call(...)
#   fmt_named(x)
# }
#
# fmt_wrong_eq_ops <- function(...) {
#   x <- parse_named_call(...)
#   fmt_items(
#     paste0("Did you mean ", fmt_obj1(paste0(names2(x), " == ", x)), "?"),
#     bullet = "*"
#   )
# }
#
# fmt_cols <- function(x) {
#   cols <- ntext(length(x), "Column", "Columns")
#   glue("{cols} {fmt_obj(x)}")
# }
#
# fmt_measures <- function(x) {
#   measures <- ntext(length(x), "Measure", "Measures")
#   glue("{measures} {fmt_obj(x)}")
# }
#
# fmt_named <- function(x) {
#   fmt_comma(paste0(fmt_obj1(names2(x)), " = ", x))
# }
#
# fmt_obj <- function(x) {
#   fmt_comma(fmt_obj1(x))
# }
#
# fmt_obj1 <- function(x) {
#   paste0("`", x, "`")
# }
#
# fmt_classes <- function(x) {
#   paste(class(x), collapse = "/")
# }
#
# fmt_dims <- function(x) {
#   paste0("[", paste0(x, collapse = " x "), "]")
# }
#
# fmt_comma <- function(..., .max = 6) {
#   x <- paste0(...)
#   if (length(x) > .max) {
#     length(x) <- .max
#     x[[.max]] <- "..."
#   }
#
#   commas(x)
# }
#
# fmt_items <- function(x, bullet = "-", .max = 6) {
#   if (length(x) > .max) {
#     more <- glue("({length(x) - (.max - 1)} more)")
#     length(x) <- .max
#     x[.max] <- more
#   }
#
#   paste0(glue("{bullet} {x}"), collapse = "\n")
# }
#
# parse_args <- function(x) {
#   # convert single formula to list of length 1
#   x <- unlist(list(x), recursive = FALSE)
#   is_fml <- map_lgl(x, is_formula)
#   x[is_fml] <- map_chr(map(x[is_fml], "[[", 2), as_string)
#   unlist(x)
# }
#
# parse_named_call <- function(x) {
#   map_chr(x, quo_text)
# }
#
#

