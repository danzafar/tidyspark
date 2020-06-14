#' @include mllib_utils.R

# Generalized Linear Regression ----------
#' @title S4 class that represents a generalized linear model
#'
#' @param jobj a Java object reference to the backing Scala GeneralizedLinearRegressionWrapper
#' @note GeneralizedLinearRegressionModel since 2.0.0
setClass("GeneralizedLinearRegressionModel", representation(jobj = "jobj"))

#' Generalized Linear Models
#'
#' Fits generalized linear model against a spark_tbl.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write_ml}/\code{read_ml} to save/load fitted models.
#'
#' @param data a spark_tbl for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param family a description of the error distribution and link function to be used in the model.
#'               This can be a character string naming a family function, a family function or
#'               the result of a call to a family function. Refer R family at
#'               \url{https://stat.ethz.ch/R-manual/R-devel/library/stats/html/family.html}.
#'               Currently these families are supported: \code{binomial}, \code{gaussian},
#'               \code{Gamma}, \code{poisson} and \code{tweedie}.
#'
#'               Note that there are two ways to specify the tweedie family.
#'               \itemize{
#'                \item Set \code{family = "tweedie"} and specify the var.power and link.power;
#'                \item When package \code{statmod} is loaded, the tweedie family is specified
#'                using the family definition therein, i.e., \code{tweedie(var.power, link.power)}.
#'               }
#' @param tol positive convergence tolerance of iterations.
#' @param maxIter integer giving the maximal number of IRLS iterations.
#' @param weightCol the weight column name. If this is not set or \code{NULL}, we treat all instance
#'                  weights as 1.0.
#' @param regParam regularization parameter for L2 regularization.
#' @param var.power the power in the variance function of the Tweedie distribution which provides
#'                      the relationship between the variance and mean of the distribution. Only
#'                      applicable to the Tweedie family.
#' @param link.power the index in the power link function. Only applicable to the Tweedie family.
#' @param stringIndexerOrderType how to order categories of a string feature column. This is used to
#'                               decide the base level of a string feature as the last category
#'                               after ordering is dropped when encoding strings. Supported options
#'                               are "frequencyDesc", "frequencyAsc", "alphabetDesc", and
#'                               "alphabetAsc". The default value is "frequencyDesc". When the
#'                               ordering is set to "alphabetDesc", this drops the same category
#'                               as R when encoding strings.
#' @param offsetCol the offset column name. If this is not set or empty, we treat all instance
#'                  offsets as 0.0. The feature specified as offset has a constant coefficient of
#'                  1.0.
#' @param ... additional arguments passed to the method.
#' @aliases ml_glm,spark_tbl,formula-method
#' @return \code{ml_glm} returns a fitted generalized linear model.
#' @rdname ml_glm
#' @name ml_glm
#' @importFrom stats gaussian
#' @examples
#' \dontrun{
#' spark_session()
#' t <- as.data.frame(Titanic, stringsAsFactors = FALSE)
#' df <- spark_tbl(t)
#' model <- ml_glm(df, Freq ~ Sex + Age, family = "gaussian")
#' summary(model)
#' }
#' @export
ml_glm <- function(data, formula, family = "gaussian", tol = 1e-06,
                   maxIter = 25, weightCol = NULL, regParam = 0, var.power = 0,
                   link.power = 1 - var.power, stringIndexerOrderType = c("frequencyDesc",
                                                                          "frequencyAsc", "alphabetDesc", "alphabetAsc"), offsetCol = NULL) {
  stringIndexerOrderType <- match.arg(stringIndexerOrderType)
  if (is.character(family)) {
    if (tolower(family) == "tweedie") {
      family <- list(family = "tweedie", link = NULL)
    }
    else {
      family <- get(family, mode = "function", envir = parent.frame())
    }
  }
  if (is.function(family)) {
    family <- family()
  }
  if (is.null(family$family)) {
    print(family)
    stop("'family' not recognized")
  }
  if (tolower(family$family) == "tweedie" && !is.null(family$variance)) {
    var.power <- log(family$variance(exp(1)))
    link.power <- log(family$linkfun(exp(1)))
    family <- list(family = "tweedie", link = NULL)
  }
  formula <- paste(deparse(formula), collapse = "")
  if (!is.null(weightCol) && weightCol == "") {
    weightCol <- NULL
  }
  else if (!is.null(weightCol)) {
    weightCol <- as.character(weightCol)
  }
  if (!is.null(offsetCol)) {
    offsetCol <- as.character(offsetCol)
    if (nchar(offsetCol) == 0) {
      offsetCol <- NULL
    }
  }

  jobj <- call_static("org.apache.spark.ml.r.GeneralizedLinearRegressionWrapper",
                      "fit", formula, attr(data, "jc"), tolower(family$family),
                      family$link, tol, as.integer(maxIter), weightCol,
                      regParam, as.double(var.power), as.double(link.power),
                      stringIndexerOrderType, offsetCol)
  new("GeneralizedLinearRegressionModel", jobj = jobj)
}

#' @param object a fitted generalized linear model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list of components includes at least the \code{coefficients} (coefficients matrix,
#'         which includes coefficients, standard error of coefficients, t value and p value),
#'         \code{null.deviance} (null/residual degrees of freedom), \code{aic} (AIC)
#'         and \code{iter} (number of iterations IRLS takes). If there are collinear columns in
#'         the data, the coefficients matrix only provides coefficients.
#' @rdname ml_glm
#' @note summary(GeneralizedLinearRegressionModel) since 2.0.0
setMethod("summary", signature(object = "GeneralizedLinearRegressionModel"),
          function(object) {
            jobj <- object@jobj
            is.loaded <- call_method(jobj, "isLoaded")
            features <- call_method(jobj, "rFeatures")
            coefficients <- call_method(jobj, "rCoefficients")
            dispersion <- call_method(jobj, "rDispersion")
            null.deviance <- call_method(jobj, "rNullDeviance")
            deviance <- call_method(jobj, "rDeviance")
            df.null <- call_method(jobj, "rResidualDegreeOfFreedomNull")
            df.residual <- call_method(jobj, "rResidualDegreeOfFreedom")
            iter <- call_method(jobj, "rNumIterations")
            family <- call_method(jobj, "rFamily")
            aic <- call_method(jobj, "rAic")
            if (family == "tweedie" && aic == 0) aic <- NA
            deviance.resid <- if (is.loaded) {
              NULL
            } else {
              new_spark_tbl(call_method(jobj, "rDevianceResiduals"))
            }
            # If the underlying WeightedLeastSquares using "normal" solver, we can provide
            # coefficients, standard error of coefficients, t value and p value. Otherwise,
            # it will be fitted by local "l-bfgs", we can only provide coefficients.
            if (length(features) == length(coefficients)) {
              coefficients <- matrix(unlist(coefficients), ncol = 1)
              colnames(coefficients) <- c("Estimate")
              rownames(coefficients) <- unlist(features)
            } else {
              coefficients <- matrix(unlist(coefficients), ncol = 4)
              colnames(coefficients) <- c("Estimate", "Std. Error", "t value", "Pr(>|t|)")
              rownames(coefficients) <- unlist(features)
            }
            ans <- list(deviance.resid = deviance.resid, coefficients = coefficients,
                        dispersion = dispersion, null.deviance = null.deviance,
                        deviance = deviance, df.null = df.null, df.residual = df.residual,
                        aic = aic, iter = iter, family = family, is.loaded = is.loaded)
            class(ans) <- "summary.GeneralizedLinearRegressionModel"
            ans
          })

#' @rdname ml_glm
#' @param x summary object of fitted generalized linear model returned by \code{summary} function.
#' @note print.summary.GeneralizedLinearRegressionModel since 2.0.0
print.summary.GeneralizedLinearRegressionModel <- function(x, ...) {
  if (x$is.loaded) {
    cat("\nSaved-loaded model does not support output 'Deviance Residuals'.\n")
  } else {
    x$deviance.resid <- setNames(unlist(approxQuantile(x$deviance.resid, "devianceResiduals",
                                                       c(0.0, 0.25, 0.5, 0.75, 1.0), 0.01)), c("Min", "1Q", "Median", "3Q", "Max"))
    x$deviance.resid <- zapsmall(x$deviance.resid, 5L)
    cat("\nDeviance Residuals: \n")
    cat("(Note: These are approximate quantiles with relative error <= 0.01)\n")
    print.default(x$deviance.resid, digits = 5L, na.print = "", print.gap = 2L)
  }

  cat("\nCoefficients:\n")
  print.default(x$coefficients, digits = 5L, na.print = "", print.gap = 2L)

  cat("\n(Dispersion parameter for ", x$family, " family taken to be ", format(x$dispersion),
      ")\n\n", apply(cbind(paste(format(c("Null", "Residual"), justify = "right"), "deviance:"),
                           format(unlist(x[c("null.deviance", "deviance")]), digits = 5L),
                           " on", format(unlist(x[c("df.null", "df.residual")])), " degrees of freedom\n"),
                     1L, paste, collapse = " "), sep = "")
  cat("AIC: ", format(x$aic, digits = 4L), "\n\n",
      "Number of Fisher Scoring iterations: ", x$iter, "\n\n", sep = "")
  invisible(x)
}

#' @param newData a spark_tbl for testing.
#' @return \code{predict} returns a spark_tbl containing predicted labels in a column named
#'         "prediction".
#' @rdname ml_glm
#' @note predict(GeneralizedLinearRegressionModel) since 1.5.0
setMethod("predict", signature(object = "GeneralizedLinearRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname ml_glm
#' @note write_ml(GeneralizedLinearRegressionModel, character) since 2.0.0
setMethod("write_ml", signature(object = "GeneralizedLinearRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

# Isotonic Regression ----------

#' S4 class that represents an IsotonicRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala IsotonicRegressionModel
#' @note IsotonicRegressionModel since 2.1.0
setClass("IsotonicRegressionModel", representation(jobj = "jobj"))


#' Isotonic Regression Model
#'
#' Fits an Isotonic Regression model against a spark_tbl, similarly to R's isoreg().
#' Users can print, make predictions on the produced model and save the model to the input path.
#'
#' @param data spark_tbl for training.
#' @param formula A symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param isotonic Whether the output sequence should be isotonic/increasing (TRUE) or
#'                 antitonic/decreasing (FALSE).
#' @param featureIndex The index of the feature if \code{featuresCol} is a vector column
#'                     (default: 0), no effect otherwise.
#' @param weightCol The weight column name.
#' @param ... additional arguments passed to the method.
#' @return \code{ml_isotonic_regression} returns a fitted Isotonic Regression model.
#' @rdname ml_isoreg
#' @aliases ml_isoreg,spark_tbl,formula-method
#' @name ml_isoreg
#' @examples
#' \dontrun{
#' spark_session()
#' data <- list(list(7.0, 0.0), list(5.0, 1.0), list(3.0, 2.0),
#'         list(5.0, 3.0), list(1.0, 4.0))
#' df <- spark_tbl(data %>% setNames("label", "feature"))
#' model <- ml_isotonic_regression(df, label ~ feature, isotonic = FALSE)
#' # return model boundaries and prediction as lists
#' result <- summary(model, df)
#' }
ml_isoreg <- function(data, formula, isotonic = TRUE, featureIndex = 0,
                      weightCol = NULL) {
  formula <- paste(deparse(formula), collapse = "")
  if (!is.null(weightCol) && weightCol == "") {
    weightCol <- NULL
  }
  else if (!is.null(weightCol)) {
    weightCol <- as.character(weightCol)
  }
  jobj <- call_static("org.apache.spark.ml.r.IsotonicRegressionWrapper",
                      "fit", attr(data, "jc"), formula, as.logical(isotonic), as.integer(featureIndex),
                      weightCol)
  new("IsotonicRegressionModel", jobj = jobj)
}

#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes model's \code{boundaries} (boundaries in increasing order)
#'         and \code{predictions} (predictions associated with the boundaries at the same index).
#' @rdname ml_isoreg
#' @aliases summary,IsotonicRegressionModel-method
#' @note summary(IsotonicRegressionModel) since 2.1.0
setMethod("summary", signature(object = "IsotonicRegressionModel"),
          function(object) {
            jobj <- object@jobj
            boundaries <- call_method(jobj, "boundaries")
            predictions <- call_method(jobj, "predictions")
            list(boundaries = boundaries, predictions = predictions)
          })

#' @param object a fitted IsotonicRegressionModel.
#' @param newData spark_tbl for testing.
#' @return \code{predict} returns a spark_tbl containing predicted values.
#' @rdname ml_isoreg
#' @aliases predict,IsotonicRegressionModel,spark_tbl-method
#' @note predict(IsotonicRegressionModel) since 2.1.0
setMethod("predict", signature(object = "IsotonicRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname ml_isoreg
#' @aliases write_ml,IsotonicRegressionModel,character-method
#' @note write_ml(IsotonicRegression, character) since 2.1.0
setMethod("write_ml", signature(object = "IsotonicRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

# Survival Analysis --------------

#' S4 class that represents a AFTSurvivalRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala AFTSurvivalRegressionWrapper
#' @note AFTSurvivalRegressionModel since 2.0.0
setClass("AFTSurvivalRegressionModel", representation(jobj = "jobj"))


#' Accelerated Failure Time (AFT) Survival Regression Model
#'
#' \code{ml_survival_regression} fits an accelerated failure time (AFT) survival regression model on
#' a spark_tbl. Users can call \code{summary} to get a summary of the fitted AFT model,
#' \code{predict} to make predictions on new data, and \code{write_ml}/\code{read_ml} to
#' save/load fitted models.
#'
#' @param data a spark_tbl for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', ':', '+', and '-'.
#'                Note that operator '.' is not supported currently.
#' @param aggregationDepth The depth for treeAggregate (greater than or equal to 2). If the
#'                         dimensions of features or the number of partitions are large, this
#'                         param could be adjusted to a larger size. This is an expert parameter.
#'                         Default value should be good for most cases.
#' @param stringIndexerOrderType how to order categories of a string feature column. This is used to
#'                               decide the base level of a string feature as the last category
#'                               after ordering is dropped when encoding strings. Supported options
#'                               are "frequencyDesc", "frequencyAsc", "alphabetDesc", and
#'                               "alphabetAsc". The default value is "frequencyDesc". When the
#'                               ordering is set to "alphabetDesc", this drops the same category
#'                               as R when encoding strings.
#' @param ... additional arguments passed to the method.
#' @return \code{ml_survival_regression} returns a fitted AFT survival regression model.
#' @seealso survival: \url{https://cran.r-project.org/package=survival}
#' @rdname ml_survreg
#' @examples
#' \dontrun{
#' df <- spark_tbl(ovarian)
#' model <- ml_survival_regression(df, Surv(futime, fustat) ~ ecog_ps + rx)
#'
#' # get a summary of the model
#' summary(model)
#' }
ml_survreg <- function(data, formula, aggregationDepth = 2,
                       stringIndexerOrderType = c("frequencyDesc", "frequencyAsc",
                                                  "alphabetDesc", "alphabetAsc")) {
  stringIndexerOrderType <- match.arg(stringIndexerOrderType)
  formula <- paste(deparse(formula), collapse = "")
  jobj <- call_static("org.apache.spark.ml.r.AFTSurvivalRegressionWrapper",
                      "fit", formula, attr(data, "jc"), as.integer(aggregationDepth),
                      stringIndexerOrderType)
  new("AFTSurvivalRegressionModel", jobj = jobj)
}

#' @param object a fitted AFT survival regression model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes the model's \code{coefficients} (features, coefficients,
#'         intercept and log(scale)).
#' @rdname ml_survreg
#' @note summary(AFTSurvivalRegressionModel) since 2.0.0
setMethod("summary", signature(object = "AFTSurvivalRegressionModel"),
          function(object) {
            jobj <- object@jobj
            features <- call_method(jobj, "rFeatures")
            coefficients <- call_method(jobj, "rCoefficients")
            coefficients <- as.matrix(unlist(coefficients))
            colnames(coefficients) <- c("Value")
            rownames(coefficients) <- unlist(features)
            list(coefficients = coefficients)
          })

#  Makes predictions from an AFT survival regression model or a model produced by
#  ml_survreg, similarly to R package survival's predict.

#' @param newData a spark_tbl for testing.
#' @return \code{predict} returns a spark_tbl containing predicted values
#'         on the original scale of the data (mean predicted value at scale = 1.0).
#' @rdname ml_survreg
#' @note predict(AFTSurvivalRegressionModel) since 2.0.0
setMethod("predict", signature(object = "AFTSurvivalRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#' @rdname ml_survreg
#' @note write_ml(AFTSurvivalRegressionModel, character) since 2.0.0
#' @seealso \link{write_ml}
setMethod("write_ml", signature(object = "AFTSurvivalRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })
