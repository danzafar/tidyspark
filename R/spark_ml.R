# Transformers
ml_glm <- function(data, formula, ...) {
  {
    .local <- function(data, formula, family = gaussian, tol = 1e-06,
                       maxIter = 25, weightCol = NULL, regParam = 0, var.power = 0,
                       link.power = 1 - var.power, stringIndexerOrderType = c("frequencyDesc",
                                                                              "frequencyAsc", "alphabetDesc", "alphabetAsc"), offsetCol = NULL)
    {
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
    .local(data, formula, ...)
  }
}

# Decision tree regression

ml_decision_tree <- function(data, formula, ...) {
  .local <- function (data, formula, type = c("regression",
                                              "classification"), maxDepth = 5, maxBins = 32, impurity = NULL,
                      seed = NULL, minInstancesPerNode = 1, minInfoGain = 0,
                      checkpointInterval = 10, maxMemoryInMB = 256, cacheNodeIds = FALSE,
                      handleInvalid = c("error", "keep", "skip"))
  {
    type <- match.arg(type)
    formula <- paste(deparse(formula), collapse = "")
    if (!is.null(seed)) {
      seed <- as.character(as.integer(seed))
    }
    switch(type, regression = {
      if (is.null(impurity)) impurity <- "variance"
      impurity <- match.arg(impurity, "variance")
      jobj <- call_static("org.apache.spark.ml.r.DecisionTreeRegressorWrapper",
                          "fit", attr(data, "jc"), formula, as.integer(maxDepth),
                          as.integer(maxBins), impurity, as.integer(minInstancesPerNode),
                          as.numeric(minInfoGain), as.integer(checkpointInterval),
                          seed, as.integer(maxMemoryInMB), as.logical(cacheNodeIds))
      new("DecisionTreeRegressionModel", jobj = jobj)
    }, classification = {
      handleInvalid <- match.arg(handleInvalid)
      if (is.null(impurity)) impurity <- "gini"
      impurity <- match.arg(impurity, c("gini", "entropy"))
      jobj <- call_static("org.apache.spark.ml.r.DecisionTreeClassifierWrapper",
                          "fit", attr(data, "jc"), formula, as.integer(maxDepth),
                          as.integer(maxBins), impurity, as.integer(minInstancesPerNode),
                          as.numeric(minInfoGain), as.integer(checkpointInterval),
                          seed, as.integer(maxMemoryInMB), as.logical(cacheNodeIds),
                          handleInvalid)
      new("DecisionTreeClassificationModel", jobj = jobj)
    })
  }
  .local(data, formula, ...)
}

# Random Forest

ml_random_forest <- function(data, formula, ...) {
  .local <- function(data, formula, type = c("regression",
                                              "classification"), maxDepth = 5, maxBins = 32, numTrees = 20,
                      impurity = NULL, featureSubsetStrategy = "auto", seed = NULL,
                      subsamplingRate = 1, minInstancesPerNode = 1, minInfoGain = 0,
                      checkpointInterval = 10, maxMemoryInMB = 256, cacheNodeIds = FALSE,
                      handleInvalid = c("error", "keep", "skip"))
  {
    type <- match.arg(type)
    formula <- paste(deparse(formula), collapse = "")
    if (!is.null(seed)) {
      seed <- as.character(as.integer(seed))
    }
    switch(type, regression = {
      if (is.null(impurity)) impurity <- "variance"
      impurity <- match.arg(impurity, "variance")
      jobj <- call_static("org.apache.spark.ml.r.RandomForestRegressorWrapper",
                          "fit", attr(data, "jc"), formula, as.integer(maxDepth),
                          as.integer(maxBins), as.integer(numTrees), impurity,
                          as.integer(minInstancesPerNode), as.numeric(minInfoGain),
                          as.integer(checkpointInterval), as.character(featureSubsetStrategy),
                          seed, as.numeric(subsamplingRate), as.integer(maxMemoryInMB),
                          as.logical(cacheNodeIds))
      new("RandomForestRegressionModel", jobj = jobj)
    }, classification = {
      handleInvalid <- match.arg(handleInvalid)
      if (is.null(impurity)) impurity <- "gini"
      impurity <- match.arg(impurity, c("gini", "entropy"))
      jobj <- call_static("org.apache.spark.ml.r.RandomForestClassifierWrapper",
                          "fit", attr(data, "jc"), formula, as.integer(maxDepth),
                          as.integer(maxBins), as.integer(numTrees), impurity,
                          as.integer(minInstancesPerNode), as.numeric(minInfoGain),
                          as.integer(checkpointInterval), as.character(featureSubsetStrategy),
                          seed, as.numeric(subsamplingRate), as.integer(maxMemoryInMB),
                          as.logical(cacheNodeIds), handleInvalid)
      new("RandomForestClassificationModel", jobj = jobj)
    })
  }
  .local(data, formula, ...)
}

# gradient boosted trees

ml_gbt <- function(data, formula, ...)
{
  .local <- function (data, formula, type = c("regression",
                                              "classification"), maxDepth = 5, maxBins = 32, maxIter = 20,
                      stepSize = 0.1, lossType = NULL, seed = NULL, subsamplingRate = 1,
                      minInstancesPerNode = 1, minInfoGain = 0, checkpointInterval = 10,
                      maxMemoryInMB = 256, cacheNodeIds = FALSE, handleInvalid = c("error",
                                                                                   "keep", "skip"))
  {
    type <- match.arg(type)
    formula <- paste(deparse(formula), collapse = "")
    if (!is.null(seed)) {
      seed <- as.character(as.integer(seed))
    }
    switch(type, regression = {
      if (is.null(lossType)) lossType <- "squared"
      lossType <- match.arg(lossType, c("squared", "absolute"))
      jobj <- call_static("org.apache.spark.ml.r.GBTRegressorWrapper",
                          "fit", attr(data, "jc"), formula, as.integer(maxDepth),
                          as.integer(maxBins), as.integer(maxIter), as.numeric(stepSize),
                          as.integer(minInstancesPerNode), as.numeric(minInfoGain),
                          as.integer(checkpointInterval), lossType, seed,
                          as.numeric(subsamplingRate), as.integer(maxMemoryInMB),
                          as.logical(cacheNodeIds))
      new("GBTRegressionModel", jobj = jobj)
    }, classification = {
      handleInvalid <- match.arg(handleInvalid)
      if (is.null(lossType)) lossType <- "logistic"
      lossType <- match.arg(lossType, "logistic")
      jobj <- call_static("org.apache.spark.ml.r.GBTClassifierWrapper",
                          "fit", attr(data, "jc"), formula, as.integer(maxDepth),
                          as.integer(maxBins), as.integer(maxIter), as.numeric(stepSize),
                          as.integer(minInstancesPerNode), as.numeric(minInfoGain),
                          as.integer(checkpointInterval), lossType, seed,
                          as.numeric(subsamplingRate), as.integer(maxMemoryInMB),
                          as.logical(cacheNodeIds), handleInvalid)
      new("GBTClassificationModel", jobj = jobj)
    })
  }
  .local(data, formula, ...)
}

ml_survival_regression <- function(data, formula, ...) {
  .local <- function(data, formula, aggregationDepth = 2,
                      stringIndexerOrderType = c("frequencyDesc", "frequencyAsc",
                                                 "alphabetDesc", "alphabetAsc"))
  {
    stringIndexerOrderType <- match.arg(stringIndexerOrderType)
    formula <- paste(deparse(formula), collapse = "")
    jobj <- call_static("org.apache.spark.ml.r.AFTSurvivalRegressionWrapper",
                        "fit", formula, attr(data, "jc"), as.integer(aggregationDepth),
                        stringIndexerOrderType)
    new("AFTSurvivalRegressionModel", jobj = jobj)
  }
  .local(data, formula, ...)
}

ml_isotonic_regression <- function(data, formula, ...) {
  .local <- function(data, formula, isotonic = TRUE, featureIndex = 0,
                      weightCol = NULL)
  {
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
  .local(data, formula, ...)
}
