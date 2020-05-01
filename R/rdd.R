

RDD <- R6::R6Class("RDD", list(
  env = NULL,
  jrdd = NULL,
  initialize = function(jrdd, serializedMode,
                        isCached, isCheckpointed) {
    stopifnot(class(serializedMode) == "character")
    stopifnot(serializedMode %in% c("byte", "string", "row"))

    self$env <- new.env()
    self$env$isCached <- isCached
    self$env$isCheckpointed <- isCheckpointed
    self$env$serializedMode <- serializedMode

    self$jrdd <- jrdd
    self

  },
  print = function() {
    cat("<tidyspark RDD>\n")
    cat(paste0(call_method(self$jrdd, "toString"), "\n"))
    invisible(self)
  }
)
)
