#' Generalized Linear Models
#'
#' Fits generalized linear model against a SparkDataFrame.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#'
#' @param data a SparkDataFrame for training.
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
#' @aliases spark.glm,SparkDataFrame,formula-method
#' @return \code{spark.glm} returns a fitted generalized linear model.
#' @rdname spark.glm
#' @name spark.glm
#' @examples
#' \dontrun{
#' spark_session()
#' t <- as.data.frame(Titanic, stringsAsFactors = FALSE)
#' df <- spark_tbl(t)
#' model <- ml_glm(df, Freq ~ Sex + Age, family = "gaussian")
#' summary(model)
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


#' Logistic Regression Model
#'
#' Fits an logistic regression model against a SparkDataFrame. It supports "binomial": Binary
#' logistic regression with pivoting; "multinomial": Multinomial logistic (softmax) regression
#' without pivoting, similar to glmnet. Users can print, make predictions on the produced model
#' and save the model to the input path.
#'
#' @param data SparkDataFrame for training.
#' @param formula A symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param regParam the regularization parameter.
#' @param elasticNetParam the ElasticNet mixing parameter. For alpha = 0.0, the penalty is an L2
#'                        penalty. For alpha = 1.0, it is an L1 penalty. For 0.0 < alpha < 1.0,
#'                        the penalty is a combination of L1 and L2. Default is 0.0 which is an
#'                        L2 penalty.
#' @param maxIter maximum iteration number.
#' @param tol convergence tolerance of iterations.
#' @param family the name of family which is a description of the label distribution to be used
#'               in the model.
#'               Supported options:
#'                 \itemize{
#'                   \item{"auto": Automatically select the family based on the number of classes:
#'                           If number of classes == 1 || number of classes == 2, set to "binomial".
#'                           Else, set to "multinomial".}
#'                   \item{"binomial": Binary logistic regression with pivoting.}
#'                   \item{"multinomial": Multinomial logistic (softmax) regression without
#'                           pivoting.}
#'                 }
#' @param standardization whether to standardize the training features before fitting the model.
#'                        The coefficients of models will be always returned on the original scale,
#'                        so it will be transparent for users. Note that with/without
#'                        standardization, the models should be always converged to the same
#'                        solution when no regularization is applied. Default is TRUE, same as
#'                        glmnet.
#' @param thresholds in binary classification, in range [0, 1]. If the estimated probability of
#'                   class label 1 is > threshold, then predict 1, else 0. A high threshold
#'                   encourages the model to predict 0 more often; a low threshold encourages the
#'                   model to predict 1 more often. Note: Setting this with threshold p is
#'                   equivalent to setting thresholds c(1-p, p). In multiclass (or binary)
#'                   classification to adjust the probability of predicting each class. Array must
#'                   have length equal to the number of classes, with values > 0, excepting that
#'                   at most one value may be 0. The class with largest value p/t is predicted,
#'                   where p is the original probability of that class and t is the class's
#'                   threshold.
#' @param weightCol The weight column name.
#' @param aggregationDepth The depth for treeAggregate (greater than or equal to 2). If the
#'                         dimensions of features or the number of partitions are large, this param
#'                         could be adjusted to a larger size. This is an expert parameter. Default
#'                         value should be good for most cases.
#' @param lowerBoundsOnCoefficients The lower bounds on coefficients if fitting under bound
#'                                  constrained optimization.
#'                                  The bound matrix must be compatible with the shape (1, number
#'                                  of features) for binomial regression, or (number of classes,
#'                                  number of features) for multinomial regression.
#'                                  It is a R matrix.
#' @param upperBoundsOnCoefficients The upper bounds on coefficients if fitting under bound
#'                                  constrained optimization.
#'                                  The bound matrix must be compatible with the shape (1, number
#'                                  of features) for binomial regression, or (number of classes,
#'                                  number of features) for multinomial regression.
#'                                  It is a R matrix.
#' @param lowerBoundsOnIntercepts The lower bounds on intercepts if fitting under bound constrained
#'                                optimization.
#'                                The bounds vector size must be equal to 1 for binomial regression,
#'                                or the number
#'                                of classes for multinomial regression.
#' @param upperBoundsOnIntercepts The upper bounds on intercepts if fitting under bound constrained
#'                                optimization.
#'                                The bound vector size must be equal to 1 for binomial regression,
#'                                or the number of classes for multinomial regression.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @return \code{ml_logit} returns a fitted logistic regression model.
#' @aliases spark.logit,SparkDataFrame,formula-method
#' @examples
#' \dontrun{
#' sparkR.session()
#' # binary logistic regression
#' t <- as.data.frame(Titanic)
#' training <- spark_tbl(t)
#' model <- ml_logit(training, Survived ~ ., regParam = 0.5)
#' summary <- summary(model)

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


#' Decision Tree Model for Regression and Classification
#'
#' \code{ml_decision_tree} fits a Decision Tree Regression model or Classification model on
#' a SparkDataFrame. Users can call \code{summary} to get a summary of the fitted Decision Tree
#' model, \code{predict} to make predictions on new data, and \code{write.ml}/\code{read.ml} to
#' save/load fitted models.
#' For more details, see
# nolint start
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-regression}{
#' Decision Tree Regression} and
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-classifier}{
#' Decision Tree Classification}
# nolint end
#'
#' @param data a SparkDataFrame for training.
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
#' @aliases spark.decisionTree,SparkDataFrame,formula-method
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
#' a SparkDataFrame. Users can call \code{summary} to get a summary of the fitted Random Forest
#' model, \code{predict} to make predictions on new data, and \code{write.ml}/\code{read.ml} to
#' save/load fitted models.
#' For more details, see
# nolint start
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-regression}{
#' Random Forest Regression} and
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-classifier}{
#' Random Forest Classification}
# nolint end
#'
#' @param data a SparkDataFrame for training.
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
#' @aliases spark.randomForest,SparkDataFrame,formula-method
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
#' SparkDataFrame. Users can call \code{summary} to get a summary of the fitted
#' Gradient Boosted Tree model, \code{predict} to make predictions on new data, and
#' \code{write.ml}/\code{read.ml} to save/load fitted models.
#' For more details, see
# nolint start
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-regression}{
#' GBT Regression} and
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html#gradient-boosted-tree-classifier}{
#' GBT Classification}
# nolint end
#'
#' @param data a SparkDataFrame for training.
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
#' @aliases spark.gbt,SparkDataFrame,formula-method
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
  formula <- callJMethod(jobj, "formula")
  numFeatures <- callJMethod(jobj, "numFeatures")
  features <-  callJMethod(jobj, "features")
  featureImportances <- callJMethod(callJMethod(jobj, "featureImportances"), "toString")
  maxDepth <- callJMethod(jobj, "maxDepth")
  numTrees <- callJMethod(jobj, "numTrees")
  treeWeights <- callJMethod(jobj, "treeWeights")
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

  summaryStr <- callJMethod(jobj, "summary")
  cat("\n", summaryStr, "\n")
  invisible(x)
}

# Create the summary of a decision tree model
summary.decisionTree <- function(model) {
  jobj <- model@jobj
  formula <- callJMethod(jobj, "formula")
  numFeatures <- callJMethod(jobj, "numFeatures")
  features <-  callJMethod(jobj, "features")
  featureImportances <- callJMethod(callJMethod(jobj, "featureImportances"), "toString")
  maxDepth <- callJMethod(jobj, "maxDepth")
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

  summaryStr <- callJMethod(jobj, "summary")
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












#' Accelerated Failure Time (AFT) Survival Regression Model
#'
#' \code{ml_survival_regression} fits an accelerated failure time (AFT) survival regression model on
#' a SparkDataFrame. Users can call \code{summary} to get a summary of the fitted AFT model,
#' \code{predict} to make predictions on new data, and \code{write.ml}/\code{read.ml} to
#' save/load fitted models.
#'
#' @param data a SparkDataFrame for training.
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
#' @examples
#' \dontrun{
#' df <- spark_tbl(ovarian)
#' model <- ml_survival_regression(df, Surv(futime, fustat) ~ ecog_ps + rx)
#'
#' # get a summary of the model
#' summary(model)
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

#' Isotonic Regression Model
#'
#' Fits an Isotonic Regression model against a SparkDataFrame, similarly to R's isoreg().
#' Users can print, make predictions on the produced model and save the model to the input path.
#'
#' @param data SparkDataFrame for training.
#' @param formula A symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param isotonic Whether the output sequence should be isotonic/increasing (TRUE) or
#'                 antitonic/decreasing (FALSE).
#' @param featureIndex The index of the feature if \code{featuresCol} is a vector column
#'                     (default: 0), no effect otherwise.
#' @param weightCol The weight column name.
#' @param ... additional arguments passed to the method.
#' @return \code{spark.isoreg} returns a fitted Isotonic Regression model.
#' @rdname spark.isoreg
#' @aliases spark.isoreg,SparkDataFrame,formula-method
#' @name spark.isoreg
#' @examples
#' \dontrun{
#' sparkr_session()
#' data <- list(list(7.0, 0.0), list(5.0, 1.0), list(3.0, 2.0),
#'         list(5.0, 3.0), list(1.0, 4.0))
#' df <- spark_tbl(data %>% setNames("label", "feature"))
#' model <- ml_isotonic_regression(df, label ~ feature, isotonic = FALSE)
#' # return model boundaries and prediction as lists
#' result <- summary(model, df)
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

#' Multilayer Perceptron Classification Model
#'
#' \code{ml_mlp} fits a multi-layer perceptron neural network model against a SparkDataFrame.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#' Only categorical data is supported.
#' For more details, see
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html}{
#'   Multilayer Perceptron}
#'
#' @param data a \code{SparkDataFrame} of observations and labels for model fitting.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#' @param blockSize blockSize parameter.
#' @param layers integer vector containing the number of nodes for each layer.
#' @param solver solver parameter, supported options: "gd" (minibatch gradient descent) or "l-bfgs".
#' @param maxIter maximum iteration number.
#' @param tol convergence tolerance of iterations.
#' @param stepSize stepSize parameter.
#' @param seed seed parameter for weights initialization.
#' @param initialWeights initialWeights parameter for weights initialization, it should be a
#'        numeric vector.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @examples
#' \dontrun{
#' df <- read.df("data/mllib/sample_multiclass_classification_data.txt", source = "libsvm")
#'
#' # fit a Multilayer Perceptron Classification Model
#' model <- ml_mlp(df, label ~ features, blockSize = 128, layers = c(4, 3), solver = "l-bfgs",
#'                    maxIter = 100, tol = 0.5, stepSize = 1, seed = 1,
#'                    initialWeights = c(0, 0, 0, 0, 0, 5, 5, 5, 5, 5, 9, 9, 9, 9, 9))
#'
#' # get the summary of the model
#' summary(model)
ml_mlp <- function(data, formula, layers, blockSize = 128,
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
