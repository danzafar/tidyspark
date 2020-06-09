approxQuantile <- function(x, cols, probabilities, relativeError) {
  sdf <- attr(x, "jc")
    statFunctions <- call_method(sdf, "stat")
    quantiles <- call_method(statFunctions, "approxQuantile",
                             as.list(cols), as.list(probabilities), relativeError)
    if (length(cols) == 1) {
      quantiles[[1]]
    }
    else {
      quantiles
    }
}

predict_internal <- function(object, newData) {
    new_spark_tbl(call_method(object@jobj, "transform", newData@sdf))
}

