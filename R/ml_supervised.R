




# Tree Models ---------------------------

#' S4 class that represents a GBTRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala GBTRegressionModel
#' @note GBTRegressionModel since 2.1.0
setClass("GBTRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a GBTClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala GBTClassificationModel
#' @note GBTClassificationModel since 2.1.0
setClass("GBTClassificationModel", representation(jobj = "jobj"))

#' S4 class that represents a RandomForestRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala RandomForestRegressionModel
#' @note RandomForestRegressionModel since 2.1.0
setClass("RandomForestRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a RandomForestClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala RandomForestClassificationModel
#' @note RandomForestClassificationModel since 2.1.0
setClass("RandomForestClassificationModel", representation(jobj = "jobj"))

#' S4 class that represents a DecisionTreeRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala DecisionTreeRegressionModel
#' @note DecisionTreeRegressionModel since 2.3.0
setClass("DecisionTreeRegressionModel", representation(jobj = "jobj"))

#' S4 class that represents a DecisionTreeClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala DecisionTreeClassificationModel
#' @note DecisionTreeClassificationModel since 2.3.0
setClass("DecisionTreeClassificationModel", representation(jobj = "jobj"))


#' Decision Tree Model for Regression and Classification
#'
#' \code{ml_decision_tree} fits a Decision Tree Regression model or Classification model on
#' a spark_tbl. Users can call \code{summary} to get a summary of the fitted Decision Tree
#' model, \code{predict} to make predictions on new data, and \code{write_ml}/\code{read_ml} to
#' save/load fitted models.
#' For more details, see
# nolint start
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-regression}{
#' Decision Tree Regression} and
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-classifier}{
#' Decision Tree Classification}
# nolint end
#'
#' @param data a spark_tbl for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', ':', '+', and '-'.
#' @param type type of model, one of "regression" or "classification", to fit
#' @param maxDepth Maximum depth of the tree (>= 0).
#' @param maxBins Maximum number of bins used for discretizing continuous features and for choosing
#'                how to split on features at each node. More bins give higher granularity. Must be
#'                >= 2 and >= number of categories in any categorical feature.
#' @param impurity Criterion used for information gain calculation.
#'                 For regression, must be "variance". For classification, must be one of
#'                 "entropy" and "gini", default is "gini".
#' @param seed integer seed for random number generation.
#' @param minInstancesPerNode Minimum number of instances each child must have after split.
#' @param minInfoGain Minimum information gain for a split to be considered at a tree node.
#' @param checkpointInterval Param for set checkpoint interval (>= 1) or disable checkpoint (-1).
#'                           Note: this setting will be ignored if the checkpoint directory is not
#'                           set.
#' @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation.
#' @param cacheNodeIds If FALSE, the algorithm will pass trees to executors to match instances with
#'                     nodes. If TRUE, the algorithm will cache node IDs for each instance. Caching
#'                     can speed up training of deeper trees. Users can set how often should the
#'                     cache be checkpointed or disable it by setting checkpointInterval.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type in classification model.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @aliases ml_decision_tree,spark_tbl,formula-method
#' @return \code{ml_decision_tree} returns a fitted Decision Tree model.
#' @rdname ml_decision_tree
#' @name ml_decision_tree
#' @examples
#' \dontrun{
#' # fit a Decision Tree Regression Model
#' df <- spark_tbl(longley)
#' model <- ml_decision_tree(df, Employed ~ ., type = "regression", maxDepth = 5, maxBins = 16)
#'
#' # get the summary of the model
#' summary(model)
#' }
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


#' Random Forest Model for Regression and Classification
#'
#' \code{ml_random_forest} fits a Random Forest Regression model or Classification model on
#' a spark_tbl. Users can call \code{summary} to get a summary of the fitted Random Forest
#' model, \code{predict} to make predictions on new data, and \code{write_ml}/\code{read_ml} to
#' save/load fitted models.
#' For more details, see
# nolint start
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-regression}{
#' Random Forest Regression} and
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-classifier}{
#' Random Forest Classification}
# nolint end
#'
#' @param data a spark_tbl for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', ':', '+', and '-'.
#' @param type type of model, one of "regression" or "classification", to fit
#' @param maxDepth Maximum depth of the tree (>= 0).
#' @param maxBins Maximum number of bins used for discretizing continuous features and for choosing
#'                how to split on features at each node. More bins give higher granularity. Must be
#'                >= 2 and >= number of categories in any categorical feature.
#' @param numTrees Number of trees to train (>= 1).
#' @param impurity Criterion used for information gain calculation.
#'                 For regression, must be "variance". For classification, must be one of
#'                 "entropy" and "gini", default is "gini".
#' @param featureSubsetStrategy The number of features to consider for splits at each tree node.
#'                              Supported options: "auto" (choose automatically for task: If
#'                                                 numTrees == 1, set to "all." If numTrees > 1
#'                                                 (forest), set to "sqrt" for classification and
#'                                                 to "onethird" for regression),
#'                                                 "all" (use all features),
#'                                                 "onethird" (use 1/3 of the features),
#'                                                 "sqrt" (use sqrt(number of features)),
#'                                                 "log2" (use log2(number of features)),
#'                                                 "n": (when n is in the range (0, 1.0], use
#'                                                 n * number of features. When n is in the range
#'                                                 (1, number of features), use n features).
#'                                                 Default is "auto".
#' @param seed integer seed for random number generation.
#' @param subsamplingRate Fraction of the training data used for learning each decision tree, in
#'                        range (0, 1].
#' @param minInstancesPerNode Minimum number of instances each child must have after split.
#' @param minInfoGain Minimum information gain for a split to be considered at a tree node.
#' @param checkpointInterval Param for set checkpoint interval (>= 1) or disable checkpoint (-1).
#'                           Note: this setting will be ignored if the checkpoint directory is not
#'                           set.
#' @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation.
#' @param cacheNodeIds If FALSE, the algorithm will pass trees to executors to match instances with
#'                     nodes. If TRUE, the algorithm will cache node IDs for each instance. Caching
#'                     can speed up training of deeper trees. Users can set how often should the
#'                     cache be checkpointed or disable it by setting checkpointInterval.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type in classification model.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @aliases ml_random_forest,spark_tbl,formula-method
#' @return \code{ml_random_forest} returns a fitted Random Forest model.
#' @rdname ml_random_forest
#' @name ml_random_forest
#' @examples
#' \dontrun{
#' # fit a Random Forest Regression Model
#' df <- spark_tbl(longley)
#' model <- ml_random_forest(df, Employed ~ ., type = "regression", maxDepth = 5, maxBins = 16)
#'
#' # get the summary of the model
#' summary(model)
#' }
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


#' Gradient Boosted Tree Model for Regression and Classification
#'
#' \code{ml_gbt} fits a Gradient Boosted Tree Regression model or Classification model on a
#' spark_tbl. Users can call \code{summary} to get a summary of the fitted
#' Gradient Boosted Tree model, \code{predict} to make predictions on new data, and
#' \code{write_ml}/\code{read_ml} to save/load fitted models.
#' For more details, see
# nolint start
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-regression}{
#' GBT Regression} and
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier}{
#' GBT Classification}
# nolint end
#'
#' @param data a spark_tbl for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', ':', '+', and '-'.
#' @param type type of model, one of "regression" or "classification", to fit
#' @param maxDepth Maximum depth of the tree (>= 0).
#' @param maxBins Maximum number of bins used for discretizing continuous features and for choosing
#'                how to split on features at each node. More bins give higher granularity. Must be
#'                >= 2 and >= number of categories in any categorical feature.
#' @param maxIter Param for maximum number of iterations (>= 0).
#' @param stepSize Param for Step size to be used for each iteration of optimization.
#' @param lossType Loss function which GBT tries to minimize.
#'                 For classification, must be "logistic". For regression, must be one of
#'                 "squared" (L2) and "absolute" (L1), default is "squared".
#' @param seed integer seed for random number generation.
#' @param subsamplingRate Fraction of the training data used for learning each decision tree, in
#'                        range (0, 1].
#' @param minInstancesPerNode Minimum number of instances each child must have after split. If a
#'                            split causes the left or right child to have fewer than
#'                            minInstancesPerNode, the split will be discarded as invalid. Should be
#'                            >= 1.
#' @param minInfoGain Minimum information gain for a split to be considered at a tree node.
#' @param checkpointInterval Param for set checkpoint interval (>= 1) or disable checkpoint (-1).
#'                           Note: this setting will be ignored if the checkpoint directory is not
#'                           set.
#' @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation.
#' @param cacheNodeIds If FALSE, the algorithm will pass trees to executors to match instances with
#'                     nodes. If TRUE, the algorithm will cache node IDs for each instance. Caching
#'                     can speed up training of deeper trees. Users can set how often should the
#'                     cache be checkpointed or disable it by setting checkpointInterval.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type in classification model.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @aliases ml_gbt,spark_tbl,formula-method
#' @return \code{ml_gbt} returns a fitted Gradient Boosted Tree model.
#' @rdname ml_gbt
#' @name ml_gbt
#' @examples
#' \dontrun{
#' # fit a Gradient Boosted Tree Regression Model
#' df <- spark_tbl(longley)
#' model <- ml_gbt(df, Employed ~ ., type = "regression", maxDepth = 5, maxBins = 16)
#'
#' # get the summary of the model
#' summary(model)
#' }
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

# Create the summary of a tree ensemble model (eg. Random Forest, GBT)
summary.treeEnsemble <- function(model) {
  jobj <- model@jobj
  formula <- call_method(jobj, "formula")
  numFeatures <- call_method(jobj, "numFeatures")
  features <-  call_method(jobj, "features")
  featureImportances <- call_method(call_method(jobj, "featureImportances"), "toString")
  maxDepth <- call_method(jobj, "maxDepth")
  numTrees <- call_method(jobj, "numTrees")
  treeWeights <- call_method(jobj, "treeWeights")
  list(formula = formula,
       numFeatures = numFeatures,
       features = features,
       featureImportances = featureImportances,
       maxDepth = maxDepth,
       numTrees = numTrees,
       treeWeights = treeWeights,
       jobj = jobj)
}

# Prints the summary of tree ensemble models (eg. Random Forest, GBT)
print.summary.treeEnsemble <- function(x) {
  jobj <- x$jobj
  cat("Formula: ", x$formula)
  cat("\nNumber of features: ", x$numFeatures)
  cat("\nFeatures: ", unlist(x$features))
  cat("\nFeature importances: ", x$featureImportances)
  cat("\nMax Depth: ", x$maxDepth)
  cat("\nNumber of trees: ", x$numTrees)
  cat("\nTree weights: ", unlist(x$treeWeights))

  summaryStr <- call_method(jobj, "summary")
  cat("\n", summaryStr, "\n")
  invisible(x)
}

# Create the summary of a decision tree model
summary.decisionTree <- function(model) {
  jobj <- model@jobj
  formula <- call_method(jobj, "formula")
  numFeatures <- call_method(jobj, "numFeatures")
  features <-  call_method(jobj, "features")
  featureImportances <- call_method(call_method(jobj, "featureImportances"), "toString")
  maxDepth <- call_method(jobj, "maxDepth")
  list(formula = formula,
       numFeatures = numFeatures,
       features = features,
       featureImportances = featureImportances,
       maxDepth = maxDepth,
       jobj = jobj)
}

# Prints the summary of decision tree models
print.summary.decisionTree <- function(x) {
  jobj <- x$jobj
  cat("Formula: ", x$formula)
  cat("\nNumber of features: ", x$numFeatures)
  cat("\nFeatures: ", unlist(x$features))
  cat("\nFeature importances: ", x$featureImportances)
  cat("\nMax Depth: ", x$maxDepth)

  summaryStr <- call_method(jobj, "summary")
  cat("\n", summaryStr, "\n")
  invisible(x)
}

setMethod("summary", signature(object = "GBTRegressionModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.GBTRegressionModel"
            ans
          })


print.summary.GBTRegressionModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}

setMethod("summary", signature(object = "GBTClassificationModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.GBTClassificationModel"
            ans
          })


print.summary.GBTClassificationModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}

setMethod("predict", signature(object = "GBTRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

setMethod("predict", signature(object = "GBTClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

setMethod("summary", signature(object = "RandomForestRegressionModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.RandomForestRegressionModel"
            ans
          })

print.summary.RandomForestRegressionModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}

setMethod("summary", signature(object = "RandomForestClassificationModel"),
          function(object) {
            ans <- summary.treeEnsemble(object)
            class(ans) <- "summary.RandomForestClassificationModel"
            ans
          })

print.summary.RandomForestClassificationModel <- function(x, ...) {
  print.summary.treeEnsemble(x)
}

setMethod("predict", signature(object = "RandomForestRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

setMethod("predict", signature(object = "RandomForestClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

setMethod("summary", signature(object = "DecisionTreeRegressionModel"),
          function(object) {
            ans <- summary.decisionTree(object)
            class(ans) <- "summary.DecisionTreeRegressionModel"
            ans
          })

print.summary.DecisionTreeRegressionModel <- function(x, ...) {
  print.summary.decisionTree(x)
}

setMethod("summary", signature(object = "DecisionTreeClassificationModel"),
          function(object) {
            ans <- summary.decisionTree(object)
            class(ans) <- "summary.DecisionTreeClassificationModel"
            ans
          })

print.summary.DecisionTreeClassificationModel <- function(x, ...) {
  print.summary.decisionTree(x)
}

setMethod("predict", signature(object = "DecisionTreeRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

setMethod("predict", signature(object = "DecisionTreeClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

