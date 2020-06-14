#' @include mllib_utils.R

# Linear SMV -------------------------------------------------------------------

#' @title S4 class that represents an LinearSVCModel
#'
#' @param jobj a Java object reference to the backing Scala LinearSVCModel
#' @note LinearSVCModel since 2.2.0
setClass("LinearSVCModel", representation(jobj = "jobj"))

#' Linear SVM Model
#'
#' Fits a linear SVM model against a spark_tbl, similar to svm in e1071 package.
#' Currently only supports binary classification model with linear kernel.
#' Users can print, make predictions on the produced model and save the model to the input path.
#'
#' @param data spark_tbl for training.
#' @param formula A symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', '-', '*', and '^'.
#' @param regParam The regularization parameter. Only supports L2 regularization currently.
#' @param maxIter Maximum iteration number.
#' @param tol Convergence tolerance of iterations.
#' @param standardization Whether to standardize the training features before fitting the model.
#'                        The coefficients of models will be always returned on the original scale,
#'                        so it will be transparent for users. Note that with/without
#'                        standardization, the models should be always converged to the same
#'                        solution when no regularization is applied.
#' @param threshold The threshold in binary classification applied to the linear model prediction.
#'                  This threshold can be any real number, where Inf will make all predictions 0.0
#'                  and -Inf will make all predictions 1.0.
#' @param weightCol The weight column name.
#' @param aggregationDepth The depth for treeAggregate (greater than or equal to 2). If the
#'                         dimensions of features or the number of partitions are large, this param
#'                         could be adjusted to a larger size.
#'                         This is an expert parameter. Default value should be good for most cases.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional arguments passed to the method.
#' @return \code{ml_svm_linear} returns a fitted linear SVM model.
#' @rdname ml_svm_linear
#' @aliases ml_svm_linear,spark_tbl,formula-method
#' @name ml_svm_linear
#' @export
#' @examples
#' \dontrun{
#' spark_session()
#' t <- as.data.frame(Titanic)
#' training <- spark_tbl(t)
#' model <- ml_svm_linear(training, Survived ~ ., regParam = 0.5)
#' summary <- summary(model)
#'
#' # fitted values on training data
#' fitted <- predict(model, training)
#'
#' # save fitted model to input path
#' path <- "path/to/model"
#' write_ml(model, path)
#'
#' # can also read back the saved model and predict
#' # Note that summary deos not work on loaded model
#' savedModel <- read_ml(path)
#' summary(savedModel)
#' }
#' @note ml_svm_linear since 2.2.0
ml_svm_linear <- function(data, formula, regParam = 0.0, maxIter = 100,
                          tol = 1E-6, standardization = TRUE, threshold = 0.0,
                          weightCol = NULL, aggregationDepth = 2,
                          handleInvalid = c("error", "keep", "skip")) {
  formula <- paste(deparse(formula), collapse = "")

  if (!is.null(weightCol) && weightCol == "") {
    weightCol <- NULL
  } else if (!is.null(weightCol)) {
    weightCol <- as.character(weightCol)
  }

  handleInvalid <- match.arg(handleInvalid)

  jobj <- call_static("org.apache.spark.ml.r.LinearSVCWrapper", "fit",
                      attr(data, "jc"), formula, as.numeric(regParam), as.integer(maxIter),
                      as.numeric(tol), as.logical(standardization), as.numeric(threshold),
                      weightCol, as.integer(aggregationDepth), handleInvalid)
  new("LinearSVCModel", jobj = jobj)
}

#  Predicted values based on a LinearSVCModel model

#' @param newData a spark_tbl for testing.
#' @return \code{predict} returns the predicted values based on a LinearSVCModel.
#' @rdname ml_svm_linear
#' @aliases predict,LinearSVCModel,spark_tbl-method
#' @note predict(LinearSVCModel) since 2.2.0
setMethod("predict", signature(object = "LinearSVCModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Get the summary of a LinearSVCModel

#' @param object a LinearSVCModel fitted by \code{ml_svm_linear}.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{coefficients} (coefficients of the fitted model),
#'         \code{numClasses} (number of classes), \code{numFeatures} (number of features).
#' @rdname ml_svm_linear
#' @aliases summary,LinearSVCModel-method
#' @note summary(LinearSVCModel) since 2.2.0
setMethod("summary", signature(object = "LinearSVCModel"),
          function(object) {
            jobj <- object@jobj
            features <- call_method(jobj, "rFeatures")
            coefficients <- call_method(jobj, "rCoefficients")
            coefficients <- as.matrix(unlist(coefficients))
            colnames(coefficients) <- c("Estimate")
            rownames(coefficients) <- unlist(features)
            numClasses <- call_method(jobj, "numClasses")
            numFeatures <- call_method(jobj, "numFeatures")
            list(coefficients = coefficients, numClasses = numClasses, numFeatures = numFeatures)
          })

#  Save fitted LinearSVCModel to the input path

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname ml_svm_linear
#' @aliases write_ml,LinearSVCModel,character-method
#' @note write_ml(LogisticRegression, character) since 2.2.0
setMethod("write_ml", signature(object = "LinearSVCModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

# Logistic regression ----------------------------------------------------------

#' S4 class that represents an LogisticRegressionModel
#'
#' @param jobj a Java object reference to the backing Scala LogisticRegressionModel
#' @note LogisticRegressionModel since 2.1.0
setClass("LogisticRegressionModel", representation(jobj = "jobj"))

#' Logistic Regression Model
#'
#' Fits an logistic regression model against a spark_tbl. It supports "binomial": Binary
#' logistic regression with pivoting; "multinomial": Multinomial logistic (softmax) regression
#' without pivoting, similar to glmnet. Users can print, make predictions on the produced model
#' and save the model to the input path.
#'
#' @param data spark_tbl for training.
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
#' @aliases ml_logit,spark_tbl,formula-method
#' @export
#' @examples
#' \dontrun{
#' spark_session()
#' # binary logistic regression
#' t <- as.data.frame(Titanic)
#' training <- spark_tbl(t)
#' model <- ml_logit(training, Survived ~ ., regParam = 0.5)
#' summary <- summary(model)
#'
#' # fitted values on training data
#' fitted <- predict(model, training)
#'
#' # save fitted model to input path
#' path <- "path/to/model"
#' write_ml(model, path)
#'
#' # can also read back the saved model and predict
#' # Note that summary deos not work on loaded model
#' savedModel <- read_ml(path)
#' summary(savedModel)
#'
#' training <- spark_tbl(iris)
#'
#' # binary logistic regression against two classes with
#' # upperBoundsOnCoefficients and upperBoundsOnIntercepts
#' ubc <- matrix(c(1.0, 0.0, 1.0, 0.0), nrow = 1, ncol = 4)
#' model <- ml_logit(training, Species ~ .,
#'                   upperBoundsOnCoefficients = ubc,
#'                   upperBoundsOnIntercepts = 1.0)
#'
#' # multinomial logistic regression
#' model <- ml_logit(training, Class ~ ., regParam = 0.5)
#' summary <- summary(model)
#'
#' # multinomial logistic regression with
#' # lowerBoundsOnCoefficients and lowerBoundsOnIntercepts
#' lbc <- matrix(c(0.0, -1.0, 0.0, -1.0, 0.0, -1.0, 0.0, -1.0), nrow = 2, ncol = 4)
#' lbi <- as.array(c(0.0, 0.0))
#' model <- ml_logit(training, Species ~ ., family = "multinomial",
#'                      lowerBoundsOnCoefficients = lbc,
#'                      lowerBoundsOnIntercepts = lbi)
#' }
#' @note ml_logit since 2.1.0
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

#' @param object an LogisticRegressionModel fitted by \code{ml_logit}.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{coefficients} (coefficients matrix of the fitted model).
#' @rdname ml_logit
#' @aliases summary,LogisticRegressionModel-method
#' @note summary(LogisticRegressionModel) since 2.1.0
setMethod("summary", signature(object = "LogisticRegressionModel"),
          function(object) {
            jobj <- object@jobj
            features <- call_method(jobj, "rFeatures")
            labels <- call_method(jobj, "labels")
            coefficients <- call_method(jobj, "rCoefficients")
            nCol <- length(coefficients) / length(features)
            coefficients <- matrix(unlist(coefficients), ncol = nCol)
            # If nCol == 1, means this is a binomial logistic regression model with pivoting.
            # Otherwise, it's a multinomial logistic regression model without pivoting.
            if (nCol == 1) {
              colnames(coefficients) <- c("Estimate")
            } else {
              colnames(coefficients) <- unlist(labels)
            }
            rownames(coefficients) <- unlist(features)

            list(coefficients = coefficients)
          })

#' @param newData a spark_tbl for testing.
#' @return \code{predict} returns the predicted values based on an LogisticRegressionModel.
#' @rdname ml_logit
#' @aliases predict,LogisticRegressionModel,spark_tbl-method
#' @note predict(LogisticRegressionModel) since 2.1.0
setMethod("predict", signature(object = "LogisticRegressionModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname ml_logit
#' @aliases write_ml,LogisticRegressionModel,character-method
#' @note write_ml(LogisticRegression, character) since 2.1.0
setMethod("write_ml", signature(object = "LogisticRegressionModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

# Multilayer Perceptron Classifier ---------------------------------------------

#' S4 class that represents a MultilayerPerceptronClassificationModel
#'
#' @param jobj a Java object reference to the backing Scala MultilayerPerceptronClassifierWrapper
#' @note MultilayerPerceptronClassificationModel since 2.1.0
setClass("MultilayerPerceptronClassificationModel", representation(jobj = "jobj"))

#' Multilayer Perceptron Classification Model
#'
#' \code{ml_mlp} fits a multi-layer perceptron neural network model against a spark_tbl.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write_ml}/\code{read_ml} to save/load fitted models.
#' Only categorical data is supported.
#' For more details, see
#' \href{http://spark.apache.org/docs/latest/ml-classification-regression.html}{
#'   Multilayer Perceptron}
#'
#' @param data a \code{spark_tbl} of observations and labels for model fitting.
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
#' @importFrom stats na.omit
#' @export
#' @examples
#' \dontrun{
#' df <- spark_read_source("data/mllib/sample_multiclass_classification_data.txt",
#'                         source = "libsvm")
#'
#' # fit a Multilayer Perceptron Classification Model
#' model <- ml_mlp(df, label ~ features, blockSize = 128, layers = c(4, 3),
#'                 solver = "l-bfgs", maxIter = 100, tol = 0.5, stepSize = 1,
#'                 seed = 1, initialWeights = c(0, 0, 0, 0, 0, 5, 5, 5, 5, 5, 9, 9, 9, 9, 9))
#'
#' # get the summary of the model
#' summary(model)
#' }
ml_mlp <- function(data, formula, layers, blockSize = 128,
                   solver = "l-bfgs", maxIter = 100, tol = 1e-06,
                   stepSize = 0.03, seed = NULL, initialWeights = NULL,
                   handleInvalid = c("error", "keep", "skip")) {
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
                      as.array(layers), as.character(solver),
                      as.integer(maxIter), as.numeric(tol),
                      as.numeric(stepSize), seed, initialWeights, handleInvalid)
  new("MultilayerPerceptronClassificationModel", jobj = jobj)
}

#' @param object a Multilayer Perceptron Classification Model fitted by \code{ml_mlp}
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{numOfInputs} (number of inputs), \code{numOfOutputs}
#'         (number of outputs), \code{layers} (array of layer sizes including input
#'         and output layers), and \code{weights} (the weights of layers).
#'         For \code{weights}, it is a numeric vector with length equal to the expected
#'         given the architecture (i.e., for 8-10-2 network, 112 connection weights).
#' @rdname ml_mlp
#' @aliases summary,MultilayerPerceptronClassificationModel-method
#' @note summary(MultilayerPerceptronClassificationModel) since 2.1.0
#' @importFrom utils tail
setMethod("summary", signature(object = "MultilayerPerceptronClassificationModel"),
          function(object) {
            jobj <- object@jobj
            layers <- unlist(call_method(jobj, "layers"))
            numOfInputs <- head(layers, n = 1)
            numOfOutputs <- tail(layers, n = 1)
            weights <- call_method(jobj, "weights")
            list(numOfInputs = numOfInputs, numOfOutputs = numOfOutputs,
                 layers = layers, weights = weights)
          })

#' @param newData a spark_tbl for testing.
#' @return \code{predict} returns a spark_tbl containing predicted labeled in a column named
#' "prediction".
#' @rdname ml_mlp
#' @aliases predict,MultilayerPerceptronClassificationModel-method
#' @note predict(MultilayerPerceptronClassificationModel) since 2.1.0
setMethod("predict", signature(object = "MultilayerPerceptronClassificationModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Saves the Multilayer Perceptron Classification Model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname ml_mlp
#' @aliases write_ml,MultilayerPerceptronClassificationModel,character-method
#' @seealso \link{write_ml}
#' @note write_ml(MultilayerPerceptronClassificationModel, character) since 2.1.0
setMethod("write_ml", signature(object = "MultilayerPerceptronClassificationModel",
                                path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

# Naive Bayes Models -----------------------------------------------------------

#' S4 class that represents a NaiveBayesModel
#'
#' @param jobj a Java object reference to the backing Scala NaiveBayesWrapper
#' @note NaiveBayesModel since 2.0.0
setClass("NaiveBayesModel", representation(jobj = "jobj"))

#' Naive Bayes Models
#'
#' \code{ml_naive_bayes} fits a Bernoulli naive Bayes model against a spark_tbl.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write_ml}/\code{read_ml} to save/load fitted models.
#' Only categorical data is supported.
#'
#' @param data a \code{spark_tbl} of observations and labels for model fitting.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'               operators are supported, including '~', '.', ':', '+', and '-'.
#' @param smoothing smoothing parameter.
#' @param handleInvalid How to handle invalid data (unseen labels or NULL values) in features and
#'                      label column of string type.
#'                      Supported options: "skip" (filter out rows with invalid data),
#'                                         "error" (throw an error), "keep" (put invalid data in
#'                                         a special additional bucket, at index numLabels). Default
#'                                         is "error".
#' @param ... additional argument(s) passed to the method. Currently only \code{smoothing}.
#' @return \code{ml_naive_bayes} returns a fitted naive Bayes model.
#' @rdname ml_naive_bayes
#' @aliases ml_naive_bayes,spark_tbl,formula-method
#' @name ml_naive_bayes
#' @seealso e1071: \url{https://cran.r-project.org/package=e1071}
#' @export
#' @examples
#' \dontrun{
#' data <- as.data.frame(UCBAdmissions)
#' df <- spark_tbl(data)
#'
#' # fit a Bernoulli naive Bayes model
#' model <- ml_naive_bayes(df, Admit ~ Gender + Dept, smoothing = 0)
#'
#' # get the summary of the model
#' summary(model)
#'
#' # make predictions
#' predictions <- predict(model, df)
#'
#' # save and load the model
#' path <- "path/to/model"
#' write_ml(model, path)
#' savedModel <- read_ml(path)
#' summary(savedModel)
#' }
#' @note ml_naive_bayes since 2.0.0
ml_naive_bayes <- function(data, formula, smoothing = 1.0,
                           handleInvalid = c("error", "keep", "skip")) {
  formula <- paste(deparse(formula), collapse = "")
  handleInvalid <- match.arg(handleInvalid)
  jobj <- call_static("org.apache.spark.ml.r.NaiveBayesWrapper", "fit",
                      formula, attr(data, "jc"), smoothing, handleInvalid)
  new("NaiveBayesModel", jobj = jobj)
}

#  Returns the summary of a naive Bayes model produced by \code{ml_naive_bayes}

#' @param object a naive Bayes model fitted by \code{ml_naive_bayes}.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes \code{apriori} (the label distribution) and
#'         \code{tables} (conditional probabilities given the target label).
#' @rdname ml_naive_bayes
#' @note summary(NaiveBayesModel) since 2.0.0
setMethod("summary", signature(object = "NaiveBayesModel"),
          function(object) {
            jobj <- object@jobj
            features <- call_method(jobj, "features")
            labels <- call_method(jobj, "labels")
            apriori <- call_method(jobj, "apriori")
            apriori <- t(as.matrix(unlist(apriori)))
            colnames(apriori) <- unlist(labels)
            tables <- call_method(jobj, "tables")
            tables <- matrix(tables, nrow = length(labels))
            rownames(tables) <- unlist(labels)
            colnames(tables) <- unlist(features)
            list(apriori = apriori, tables = tables)
          })

#  Makes predictions from a naive Bayes model or a model produced by
# ml_naive_bayes(), similarly to R package e1071's predict.

#' @param newData a spark_tbl for testing.
#' @return \code{predict} returns a spark_tbl containing predicted labeled in a column named
#' "prediction".
#' @rdname ml_naive_bayes
#' @note predict(NaiveBayesModel) since 2.0.0
setMethod("predict", signature(object = "NaiveBayesModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#  Saves the Bernoulli naive Bayes model to the input path.

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname ml_naive_bayes
#' @seealso \link{write_ml}
#' @note write_ml(NaiveBayesModel, character) since 2.0.0
setMethod("write_ml", signature(object = "NaiveBayesModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })
