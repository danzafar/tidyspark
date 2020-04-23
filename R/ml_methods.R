# GLM
setMethod("summary", signature(x = "GeneralizedLinearRegressionModel") <- function(object, ...) {
  jobj <- object@jobj
  is.loaded <- callJMethod(jobj, "isLoaded")
  features <- callJMethod(jobj, "rFeatures")
  coefficients <- callJMethod(jobj, "rCoefficients")
  dispersion <- callJMethod(jobj, "rDispersion")
  null.deviance <- callJMethod(jobj, "rNullDeviance")
  deviance <- callJMethod(jobj, "rDeviance")
  df.null <- callJMethod(jobj, "rResidualDegreeOfFreedomNull")
  df.residual <- callJMethod(jobj, "rResidualDegreeOfFreedom")
  iter <- callJMethod(jobj, "rNumIterations")
  family <- callJMethod(jobj, "rFamily")
  aic <- callJMethod(jobj, "rAic")
  if (family == "tweedie" && aic == 0)
    aic <- NA
  deviance.resid <- if (is.loaded) {
    NULL
  }
  else {
    dataFrame(callJMethod(jobj, "rDevianceResiduals"))
  }
  if (length(features) == length(coefficients)) {
    coefficients <- matrix(unlist(coefficients), ncol = 1)
    colnames(coefficients) <- c("Estimate")
    rownames(coefficients) <- unlist(features)
  }
  else {
    coefficients <- matrix(unlist(coefficients), ncol = 4)
    colnames(coefficients) <- c("Estimate", "Std. Error",
                                "t value", "Pr(>|t|)")
    rownames(coefficients) <- unlist(features)
  }
  ans <- list(deviance.resid = deviance.resid, coefficients = coefficients,
              dispersion = dispersion, null.deviance = null.deviance,
              deviance = deviance, df.null = df.null, df.residual = df.residual,
              aic = aic, iter = iter, family = family, is.loaded = is.loaded)
  class(ans) <- "summary.GeneralizedLinearRegressionModel"
  ans
})

# Logit

# Descision Trees

# RF

# Survival

# Isotonic

# MLP

# Predict

setMethod('predict', signature = 'GeneralizedLinearRegressionModel') <- function(object, newData) {
  new_spark_tbl(callJMethod(object@jobj, "transform", newData@sdf))
}
# GBT
