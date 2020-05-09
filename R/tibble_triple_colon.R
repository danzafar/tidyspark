
glimpse.tbl <- function (x, width = NULL, ...) {
  width <- tibble_glimpse_width(width)
  if (!is.finite(width)) {
    abort(error_glimpse_infinite_width())
  }
  cat_line("Observations: ", big_mark(nrow(x)))
  rows <- as.integer(width/3)
  df <- as.data.frame(head(x, rows))
  cat_line("Variables: ", big_mark(ncol(df)))
  summary <- tbl_sum(x)
  brief_summary <- summary[-1]
  if (has_length(brief_summary)) {
    cat_line(names(brief_summary), ": ", brief_summary)
  }
  if (ncol(df) == 0)
    return(invisible(x))
  var_types <- map_chr(map(df, new_pillar_type), format)
  ticked_names <- format(new_pillar_title(tick_if_needed(names(df))))
  var_names <- paste0("$ ", justify(ticked_names, right = FALSE),
                      " ", var_types, " ")
  data_width <- width - crayon::col_nchar(var_names) - 2
  formatted <- map_chr(df, function(x) collapse(format_v(x)))
  truncated <- str_trunc(formatted, data_width)
  cat_line(var_names, truncated)
  invisible(x)
}
