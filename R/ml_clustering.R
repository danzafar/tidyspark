#' S4 class that represents a KMeansModel
#'
#' @param jobj a Java object reference to the backing Scala KMeansModel
#' @note KMeansModel since 2.0.0
setClass("KMeansModel", representation(jobj = "jobj"))


#' K-Means Clustering Model
#'
#' Fits a k-means clustering model against a SparkDataFrame, similarly to R's kmeans().
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#'                Note that the response variable of formula is empty in spark.kmeans.
#' @param k number of centers.
#' @param maxIter maximum iteration number.
#' @param initMode the initialization algorithm chosen to fit the model.
#' @param seed the random seed for cluster initialization.
#' @param initSteps the number of steps for the k-means|| initialization mode.
#'                  This is an advanced setting, the default of 2 is almost always enough.
#'                  Must be > 0.
#' @param tol convergence tolerance of iterations.
#' @param ... additional argument(s) passed to the method.
#' @return \code{spark.kmeans} returns a fitted k-means model.
#' @rdname spark.kmeans
#' @aliases spark.kmeans,SparkDataFrame,formula-method
#' @name spark.kmeans
#' @examples
#' \dontrun{
#' spark_session()
#' t <- as.data.frame(Titanic)
#' df <- spark_tbl(t)
#' model <- ml_kmeans(df, Class ~ Survived, k = 4, initMode = "random")
#' summary(model)
#' #' @export
ml_kmeans <- function(data,
                      formula,
                      k = 2,
                      maxIter = 20,
                      initMode = c("k-means||",
                                   "random"),
                      seed = NULL,
                      initSteps = 2,
                      tol = 1e-04) {
  formula <- paste(deparse(formula), collapse = "")
  initMode <- match.arg(initMode)
  if (!is.null(seed)) {
    seed <- as.character(as.integer(seed))
  }
  jobj <- call_static("org.apache.spark.ml.r.KMeansWrapper",
                      "fit", attr(data, "jc"), formula, as.integer(k), as.integer(maxIter),
                      initMode, seed, as.integer(initSteps), as.numeric(tol))
  new("KMeansModel", jobj = jobj)
}

setMethod("summary", signature(object = "KMeansModel"),
          function(object) {
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            features <- callJMethod(jobj, "features")
            coefficients <- callJMethod(jobj, "coefficients")
            k <- callJMethod(jobj, "k")
            size <- callJMethod(jobj, "size")
            clusterSize <- callJMethod(jobj, "clusterSize")
            coefficients <- t(matrix(unlist(coefficients), ncol = clusterSize))
            colnames(coefficients) <- unlist(features)
            rownames(coefficients) <- 1:clusterSize
            cluster <- if (is.loaded) {
              NULL
            } else {
              dataFrame(callJMethod(jobj, "cluster"))
            }
            list(k = k, coefficients = coefficients, size = size,
                 cluster = cluster, is.loaded = is.loaded, clusterSize = clusterSize)
          })

setMethod("predict", signature(object = "KMeansModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' Get fitted result from a k-means model
#'
#' Get fitted result from a k-means model, similarly to R's fitted().
#' Note: A saved-loaded model does not support this method.
#'
#' @param object a fitted k-means model.
#' @param method type of fitted results, \code{"centers"} for cluster centers
#'        or \code{"classes"} for assigned classes.
#' @param ... additional argument(s) passed to the method.
#' @return \code{fitted} returns a SparkDataFrame containing fitted values.

setMethod("fitted", signature(object = "KMeansModel"),
          function(object, method = c("centers", "classes")) {
            method <- match.arg(method)
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            if (is.loaded) {
              stop("Saved-loaded k-means model does not support 'fitted' method")
            } else {
              dataFrame(callJMethod(jobj, "fitted", method))
            }
          })


#' S4 class that represents a BisectingKMeansModel
#'
#' @param jobj a Java object reference to the backing Scala BisectingKMeansModel
#' @note BisectingKMeansModel since 2.2.0
setClass("BisectingKMeansModel", representation(jobj = "jobj"))

#' Spark ML -- Bisecting K-Means Clustering
#'
#' A bisecting k-means algorithm based on the paper "A comparison of document clustering techniques"
#' by Steinbach, Karypis, and Kumar, with modification to fit Spark.
#' The algorithm starts from a single cluster that contains all points. Iteratively it finds divisible
#' clusters on the bottom level and bisects each of them using k-means, until there are k leaf clusters
#' in total or no leaf clusters are divisible. The bisecting steps of clusters on the same level are
#' grouped together to increase parallelism. If bisecting all divisible clusters on the bottom level
#' would result more than k leaf clusters, larger clusters get higher priority.
#'
#' Fits a bisecting k-means clustering model against a SparkDataFrame.
#' Users can call \code{summary} to print a summary of the fitted model, \code{predict} to make
#' predictions on new data, and \code{write.ml}/\code{read.ml} to save/load fitted models.
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#'                Note that the response variable of formula is empty in spark.bisectingKmeans.
#' @param k the desired number of leaf clusters. Must be > 1.
#'          The actual number could be smaller if there are no divisible leaf clusters.
#' @param maxIter maximum iteration number.
#' @param seed the random seed.
#' @param minDivisibleClusterSize The minimum number of points (if greater than or equal to 1.0)
#'                                or the minimum proportion of points (if less than 1.0) of a
#'                                divisible cluster. Note that it is an expert parameter. The
#'                                default value should be good enough for most cases.
#' @param ... additional argument(s) passed to the method.
#' @return \code{spark.bisectingKmeans} returns a fitted bisecting k-means model.
#' @examples
#' \dontrun{
#' spark_session()
#' iris_fix <- iris %>%
#' setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
#'  mutate(Species = levels(Species)[Species])
#' iris_spk <- spark_tbl(iris)
#' model <- spark.bisectingKmeans(iris_spk, Sepal_Width ~ Sepal_Length, k = 4)
#' summary(model)
#' #' @export
ml_kmeans_bisecting <- function(data,
                                formula,
                                k = 4,
                                maxIter = 20,
                                seed = NULL,
                                minDivisibleClusterSize = 1) {
  formula <- paste0(deparse(formula), collapse = "")
  if (!is.null(seed)) {
    seed <- as.character(as.integer(seed))
  }
  jobj <- call_static("org.apache.spark.ml.r.BisectingKMeansWrapper",
                      "fit", attr(data, "jc"), formula, as.integer(k), as.integer(maxIter),
                      seed, as.numeric(minDivisibleClusterSize))
  new("BisectingKMeansModel", jobj = jobj)
}

setMethod("summary", signature(object = "BisectingKMeansModel"),
          function(object) {
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            features <- callJMethod(jobj, "features")
            coefficients <- callJMethod(jobj, "coefficients")
            k <- callJMethod(jobj, "k")
            size <- callJMethod(jobj, "size")
            coefficients <- t(matrix(coefficients, ncol = k))
            colnames(coefficients) <- unlist(features)
            rownames(coefficients) <- 1:k
            cluster <- if (is.loaded) {
              NULL
            } else {
              dataFrame(callJMethod(jobj, "cluster"))
            }
            list(k = k, coefficients = coefficients, size = size,
                 cluster = cluster, is.loaded = is.loaded)
          })

setMethod("predict", signature(object = "BisectingKMeansModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' Get fitted result from a bisecting k-means model
#'
#' Get fitted result from a bisecting k-means model.
#' Note: A saved-loaded model does not support this method.
#'
#' @param method type of fitted results, \code{"centers"} for cluster centers
#'        or \code{"classes"} for assigned classes.
#' @return \code{fitted} returns a SparkDataFrame containing fitted values.
setMethod("fitted", signature(object = "BisectingKMeansModel"),
          function(object, method = c("centers", "classes")) {
            method <- match.arg(method)
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            if (is.loaded) {
              stop("Saved-loaded bisecting k-means model does not support 'fitted' method")
            } else {
              dataFrame(callJMethod(jobj, "fitted", method))
            }
          })



#' S4 class that represents an LDAModel
#'
#' @param jobj a Java object reference to the backing Scala LDAWrapper
#' @note LDAModel since 2.1.0
setClass("LDAModel", representation(jobj = "jobj"))

#' Latent Dirichlet Allocation
#'
#' \code{ml_lda} fits a Latent Dirichlet Allocation model on a SparkDataFrame. Users can call
#' \code{summary} to get a summary of the fitted LDA model.
#'
#' @param data A SparkDataFrame for training.
#' @param features Features column name. Either libSVM-format column or character-format column is
#'        valid.
#' @param k Number of topics.
#' @param maxIter Maximum iterations.
#' @param optimizer Optimizer to train an LDA model, "online" or "em", default is "online".
#' @param subsamplingRate (For online optimizer) Fraction of the corpus to be sampled and used in
#'        each iteration of mini-batch gradient descent, in range (0, 1].
#' @param topicConcentration concentration parameter (commonly named \code{beta} or \code{eta}) for
#'        the prior placed on topic distributions over terms, default -1 to set automatically on the
#'        Spark side. Use \code{summary} to retrieve the effective topicConcentration. Only 1-size
#'        numeric is accepted.
#' @param docConcentration concentration parameter (commonly named \code{alpha}) for the
#'        prior placed on documents distributions over topics (\code{theta}), default -1 to set
#'        automatically on the Spark side. Use \code{summary} to retrieve the effective
#'        docConcentration. Only 1-size or \code{k}-size numeric is accepted.
#' @param customizedStopWords stopwords that need to be removed from the given corpus. Ignore the
#'        parameter if libSVM-format column is used as the features column.
#' @param maxVocabSize maximum vocabulary size, default 1 << 18
#' @param ... additional argument(s) passed to the method.
#' @return \code{ml_lda} returns a fitted Latent Dirichlet Allocation model.
#' @seealso topicmodels: \url{https://cran.r-project.org/package=topicmodels}
#' @export
ml_lda <- function(data,
                   features = "features",
                   k = 10,
                   maxIter = 20,
                   optimizer = c("online", "em"),
                   subsamplingRate = 0.05,
                   topicConcentration = -1,
                   docConcentration = -1,
                   customizedStopWords = "",
                   maxVocabSize = bitwShiftL(1, 18)) {
  optimizer <- match.arg(optimizer)
  jobj <- call_static("org.apache.spark.ml.r.LDAWrapper",
                      "fit", attr(data, "jc"), features, as.integer(k), as.integer(maxIter),
                      optimizer, as.numeric(subsamplingRate), topicConcentration,
                      as.array(docConcentration), as.array(customizedStopWords),
                      maxVocabSize)
  new("LDAModel", jobj = jobj)
}

setMethod("summary", signature(object = "LDAModel"),
          function(object, maxTermsPerTopic) {
            maxTermsPerTopic <- as.integer(ifelse(missing(maxTermsPerTopic), 10, maxTermsPerTopic))
            jobj <- object@jobj
            docConcentration <- callJMethod(jobj, "docConcentration")
            topicConcentration <- callJMethod(jobj, "topicConcentration")
            logLikelihood <- callJMethod(jobj, "logLikelihood")
            logPerplexity <- callJMethod(jobj, "logPerplexity")
            isDistributed <- callJMethod(jobj, "isDistributed")
            vocabSize <- callJMethod(jobj, "vocabSize")
            topics <- dataFrame(callJMethod(jobj, "topics", maxTermsPerTopic))
            vocabulary <- callJMethod(jobj, "vocabulary")
            trainingLogLikelihood <- if (isDistributed) {
              callJMethod(jobj, "trainingLogLikelihood")
            } else {
              NA
            }
            logPrior <- if (isDistributed) {
              callJMethod(jobj, "logPrior")
            } else {
              NA
            }
            list(docConcentration = unlist(docConcentration),
                 topicConcentration = topicConcentration,
                 logLikelihood = logLikelihood, logPerplexity = logPerplexity,
                 isDistributed = isDistributed, vocabSize = vocabSize,
                 topics = topics, vocabulary = unlist(vocabulary),
                 trainingLogLikelihood = trainingLogLikelihood, logPrior = logPrior)
          })


#' @return \code{spark.perplexity} returns the log perplexity of given SparkDataFrame, or the log
#'         perplexity of the training data if missing argument "data".
#' @rdname ml_lda
#' @aliases spark.perplexity,LDAModel-method
#' @note spark.perplexity(LDAModel) since 2.1.0
setMethod("ml_perplexity", signature(object = "LDAModel", data = "SparkDataFrame"),
          function(object, data) {
            ifelse(missing(data), callJMethod(object@jobj, "logPerplexity"),
                   callJMethod(object@jobj, "computeLogPerplexity", data@sdf))
          })


#' @param newData A SparkDataFrame for testing.
#' @return \code{spark.posterior} returns a SparkDataFrame containing posterior probabilities
#'         vectors named "topicDistribution".
#' @rdname ml_lda
#' @aliases spark.posterior,LDAModel,SparkDataFrame-method
#' @note spark.posterior(LDAModel) since 2.1.0
setMethod("ml_posterior", signature(object = "LDAModel", newData = "SparkDataFrame"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' S4 class that represents a GaussianMixtureModel
#'
#' @param jobj a Java object reference to the backing Scala GaussianMixtureModel
#' @note GaussianMixtureModel since 2.1.0
setClass("GaussianMixtureModel", representation(jobj = "jobj"))

#' Multivariate Gaussian Mixture Model (GMM)
#'
#' Fits multivariate gaussian mixture model against a SparkDataFrame, similarly to R's
#' mvnormalmixEM(). Users can call \code{summary} to print a summary of the fitted model,
#' \code{predict} to make predictions on new data, and \code{write.ml}/\code{read.ml}
#' to save/load fitted models.
#'
#' @param data a SparkDataFrame for training.
#' @param formula a symbolic description of the model to be fitted. Currently only a few formula
#'                operators are supported, including '~', '.', ':', '+', and '-'.
#'                Note that the response variable of formula is empty in spark.gaussianMixture.
#' @param k number of independent Gaussians in the mixture model.
#' @param maxIter maximum iteration number.
#' @param tol the convergence tolerance.
#' @param ... additional arguments passed to the method.
#' @aliases spark.gaussianMixture,SparkDataFrame,formula-method
#' @return \code{ml_gaussian_mixture} returns a fitted multivariate gaussian mixture model.
#' @seealso mixtools: \url{https://cran.r-project.org/package=mixtools}
#' @examples
#' \dontrun{
#' spark_session()
#' library(mvtnorm)
#' set.seed(100)
#' a <- rmvnorm(4, c(0, 0))
#' b <- rmvnorm(6, c(3, 4))
#' data <- rbind(a, b)
#' df <- spark_tbl(as.data.frame(data))
#' model <- ml_gaussian_mixture(df, ~ V1 + V2, k = 2)
#' summary(model)
#' @export
ml_gaussian_mixture <- function(data, formula, k = 2, maxIter = 100,
                                tol = 0.01) {
  formula <- paste(deparse(formula), collapse = "")
  jobj <- call_static("org.apache.spark.ml.r.GaussianMixtureWrapper",
                      "fit", attr(data, "jc"), formula, as.integer(k), as.integer(maxIter),
                      as.numeric(tol))
  new("GaussianMixtureModel", jobj = jobj)
}

setMethod("summary", signature(object = "GaussianMixtureModel"),
          function(object) {
            jobj <- object@jobj
            is.loaded <- callJMethod(jobj, "isLoaded")
            lambda <- unlist(callJMethod(jobj, "lambda"))
            muList <- callJMethod(jobj, "mu")
            sigmaList <- callJMethod(jobj, "sigma")
            k <- callJMethod(jobj, "k")
            dim <- callJMethod(jobj, "dim")
            loglik <- callJMethod(jobj, "logLikelihood")
            mu <- c()
            for (i in 1 : k) {
              start <- (i - 1) * dim + 1
              end <- i * dim
              mu[[i]] <- unlist(muList[start : end])
            }
            sigma <- c()
            for (i in 1 : k) {
              start <- (i - 1) * dim * dim + 1
              end <- i * dim * dim
              sigma[[i]] <- t(matrix(sigmaList[start : end], ncol = dim))
            }
            posterior <- if (is.loaded) {
              NULL
            } else {
              dataFrame(callJMethod(jobj, "posterior"))
            }
            list(lambda = lambda, mu = mu, sigma = sigma, loglik = loglik,
                 posterior = posterior, is.loaded = is.loaded)
          })

setMethod("predict", signature(object = "GaussianMixtureModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })
