# Transformers
ml_glm <- function(data, formula, family = gaussian, tol = 1e-06,
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


# logistic regression

ml_logit <- function(data, formula, regParam = 0, elasticNetParam = 0,
                      maxIter = 100, tol = 1e-06, family = "auto", standardization = TRUE,
                      thresholds = 0.5, weightCol = NULL, aggregationDepth = 2,
                      lowerBoundsOnCoefficients = NULL, upperBoundsOnCoefficients = NULL,
                      lowerBoundsOnIntercepts = NULL, upperBoundsOnIntercepts = NULL,
                      handleInvalid = c("error", "keep", "skip")) {
    formula <- paste(deparse(formula), collapse = "")
    row <- 0
    col <- 0
    if (!is.null(weightCol) && weightCol == "") {
      weightCol <- NULL
    }
    else if (!is.null(weightCol)) {
      weightCol <- as.character(weightCol)
    }
    if (!is.null(lowerBoundsOnIntercepts)) {
      lowerBoundsOnIntercepts <- as.array(lowerBoundsOnIntercepts)
    }
    if (!is.null(upperBoundsOnIntercepts)) {
      upperBoundsOnIntercepts <- as.array(upperBoundsOnIntercepts)
    }
    if (!is.null(lowerBoundsOnCoefficients)) {
      if (class(lowerBoundsOnCoefficients) != "matrix") {
        stop("lowerBoundsOnCoefficients must be a matrix.")
      }
      row <- nrow(lowerBoundsOnCoefficients)
      col <- ncol(lowerBoundsOnCoefficients)
      lowerBoundsOnCoefficients <- as.array(as.vector(lowerBoundsOnCoefficients))
    }
    if (!is.null(upperBoundsOnCoefficients)) {
      if (class(upperBoundsOnCoefficients) != "matrix") {
        stop("upperBoundsOnCoefficients must be a matrix.")
      }
      if (!is.null(lowerBoundsOnCoefficients) && (row !=
                                                  nrow(upperBoundsOnCoefficients) || col != ncol(upperBoundsOnCoefficients))) {
        stop(paste0("dimension of upperBoundsOnCoefficients ",
                    "is not the same as lowerBoundsOnCoefficients",
                    sep = ""))
      }
      if (is.null(lowerBoundsOnCoefficients)) {
        row <- nrow(upperBoundsOnCoefficients)
        col <- ncol(upperBoundsOnCoefficients)
      }
      upperBoundsOnCoefficients <- as.array(as.vector(upperBoundsOnCoefficients))
    }
    handleInvalid <- match.arg(handleInvalid)
    jobj <- call_static("org.apache.spark.ml.r.LogisticRegressionWrapper",
                        "fit", attr(data, "jc"), formula, as.numeric(regParam), as.numeric(elasticNetParam),
                        as.integer(maxIter), as.numeric(tol), as.character(family),
                        as.logical(standardization), as.array(thresholds),
                        weightCol, as.integer(aggregationDepth), as.integer(row),
                        as.integer(col), lowerBoundsOnCoefficients, upperBoundsOnCoefficients,
                        lowerBoundsOnIntercepts, upperBoundsOnIntercepts,
                        handleInvalid)
    new("LogisticRegressionModel", jobj = jobj)
}


# Decision tree regression

ml_decision_tree <- function (data, formula, type = c("regression",
                                              "classification"), maxDepth = 5, maxBins = 32, impurity = NULL,
                      seed = NULL, minInstancesPerNode = 1, minInfoGain = 0,
                      checkpointInterval = 10, maxMemoryInMB = 256, cacheNodeIds = FALSE,
                      handleInvalid = c("error", "keep", "skip")) {
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


# Random Forest

ml_random_forest <- function(data, formula, type = c("regression",
                                              "classification"), maxDepth = 5, maxBins = 32, numTrees = 20,
                      impurity = NULL, featureSubsetStrategy = "auto", seed = NULL,
                      subsamplingRate = 1, minInstancesPerNode = 1, minInfoGain = 0,
                      checkpointInterval = 10, maxMemoryInMB = 256, cacheNodeIds = FALSE,
                      handleInvalid = c("error", "keep", "skip")) {
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


# gradient boosted trees

ml_gbt <- function(data, formula,
                   type = c("regression", "classification"),
                   maxDepth = 5, maxBins = 32, maxIter = 20,
                   stepSize = 0.1, lossType = NULL, seed = NULL, subsamplingRate = 1,
                   minInstancesPerNode = 1, minInfoGain = 0, checkpointInterval = 10,
                   maxMemoryInMB = 256, cacheNodeIds = FALSE,
                   handleInvalid = c("error",
                                     "keep", "skip")) {
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


ml_survival_regression <- function(data, formula, aggregationDepth = 2,
                      stringIndexerOrderType = c("frequencyDesc", "frequencyAsc",
                                                 "alphabetDesc", "alphabetAsc")) {
    stringIndexerOrderType <- match.arg(stringIndexerOrderType)
    formula <- paste(deparse(formula), collapse = "")
    jobj <- call_static("org.apache.spark.ml.r.AFTSurvivalRegressionWrapper",
                        "fit", formula, attr(data, "jc"), as.integer(aggregationDepth),
                        stringIndexerOrderType)
    new("AFTSurvivalRegressionModel", jobj = jobj)
}

ml_isotonic_regression <- function(data, formula, isotonic = TRUE, featureIndex = 0,
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

ml_mlp <- function (data, formula, layers, blockSize = 128,
                    solver = "l-bfgs", maxIter = 100, tol = 1e-06, stepSize = 0.03,
                    seed = NULL, initialWeights = NULL, handleInvalid = c("error",
                                                                          "keep", "skip"))
{
  formula <- paste(deparse(formula), collapse = "")
  if (is.null(layers)) {
    stop("layers must be a integer vector with length > 1.")
  }
  layers <- as.integer(na.omit(layers))
  if (length(layers) <= 1) {
    stop("layers must be a integer vector with length > 1.")
  }
  if (!is.null(seed)) {
    seed <- as.character(as.integer(seed))
  }
  if (!is.null(initialWeights)) {
    initialWeights <- as.array(as.numeric(na.omit(initialWeights)))
  }
  handleInvalid <- match.arg(handleInvalid)
  jobj <- call_static("org.apache.spark.ml.r.MultilayerPerceptronClassifierWrapper",
                      "fit", attr(data, "jc"), formula, as.integer(blockSize),
                      as.array(layers), as.character(solver), as.integer(maxIter),
                      as.numeric(tol), as.numeric(stepSize), seed, initialWeights,
                      handleInvalid)
  new("MultilayerPerceptronClassificationModel", jobj = jobj)
}
