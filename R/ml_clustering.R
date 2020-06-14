#' @include mllib_utils.R

#' @title S4 class that represents a KMeansModel
#'
#' @param jobj a Java object reference to the backing Scala KMeansModel
#' @note KMeansModel since 2.0.0
setClass("KMeansModel", representation(jobj = "jobj"))


#' K-Means Clustering Model
#'
#' Fits a k-means clustering model against a spark_tbl, similarly to R's
#' kmeans(). Users can call \code{summary} to print a summary of the fitted
#' model, \code{predict} to make predictions on new data, and \code{write_ml}/
#' \code{read_ml} to save/load fitted models.
#'
#' @param data a spark_tbl for training.
#' @param formula a symbolic description of the model to be fitted. Currently
#'                only a few formula operators are supported, including '~',
#'                '.', ':', '+', and '-'. Note that the response variable of
#'                formula is empty in ml_kmeans.
#' @param k number of centers.
#' @param maxIter maximum iteration number.
#' @param initMode the initialization algorithm chosen to fit the model.
#' @param seed the random seed for cluster initialization.
#' @param initSteps the number of steps for the k-means|| initialization mode.
#'                  This is an advanced setting, the default of 2 is almost
#'                  always enough. Must be > 0.
#' @param tol convergence tolerance of iterations.
#' @param ... additional argument(s) passed to the method.
#' @return \code{ml_kmeans} returns a fitted k-means model.
#' @rdname ml_kmeans
#' @aliases ml_kmeans,spark_tbl,formula-method
#' @name ml_kmeans
#' @examples
#' \dontrun{
#' spark_session()
#' t <- as.data.frame(Titanic)
#' df <- spark_tbl(t)
#' model <- ml_kmeans(df, Class ~ Survived, k = 4, initMode = "random")
#' summary(model)
#' }
#' @export
ml_kmeans <- function(data, formula, k = 2, maxIter = 20,
                      initMode = c("k-means||", "random"), seed = NULL,
                      initSteps = 2, tol = 1e-04) {
  formula <- paste(deparse(formula), collapse = "")
  initMode <- match.arg(initMode)
  if (!is.null(seed)) {
    seed <- as.character(as.integer(seed))
  }
  jobj <- call_static("org.apache.spark.ml.r.KMeansWrapper",
                      "fit", attr(data, "jc"), formula, as.integer(k),
                      as.integer(maxIter), initMode, seed,
                      as.integer(initSteps), as.numeric(tol))
  new("KMeansModel", jobj = jobj)
}

#' @param object a fitted k-means model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes the model's \code{k} (the configured number of cluster centers),
#'         \code{coefficients} (model cluster centers),
#'         \code{size} (number of data points in each cluster), \code{cluster}
#'         (cluster centers of the transformed data), {is.loaded} (whether the model is loaded
#'         from a saved file), and \code{clusterSize}
#'         (the actual number of cluster centers. When using initMode = "random",
#'         \code{clusterSize} may not equal to \code{k}).
#' @rdname ml_kmeans
#' @note summary(KMeansModel) since 2.0.0
setMethod("summary", signature(object = "KMeansModel"),
          function(object) {
            jobj <- object@jobj
            is.loaded <- call_method(jobj, "isLoaded")
            features <- call_method(jobj, "features")
            coefficients <- call_method(jobj, "coefficients")
            k <- call_method(jobj, "k")
            size <- call_method(jobj, "size")
            clusterSize <- call_method(jobj, "clusterSize")
            coefficients <- t(matrix(unlist(coefficients), ncol = clusterSize))
            colnames(coefficients) <- unlist(features)
            rownames(coefficients) <- 1:clusterSize
            cluster <- if (is.loaded) {
              NULL
            } else {
              new_spark_tbl(call_method(jobj, "cluster"))
            }
            list(k = k, coefficients = coefficients, size = size,
                 cluster = cluster, is.loaded = is.loaded,
                 clusterSize = clusterSize)
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
#' @rdname ml_kmeans
#' @return \code{fitted} returns a spark_tbl containing fitted values.
setMethod("fitted", signature(object = "KMeansModel"),
          function(object, method = c("centers", "classes")) {
            method <- match.arg(method)
            jobj <- object@jobj
            is.loaded <- call_method(jobj, "isLoaded")
            if (is.loaded) {
              stop("Saved-loaded k-means model does not support 'fitted' method")
            } else {
              new_spark_tbl(call_method(jobj, "fitted", method))
            }
          })

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname ml_kmeans
#' @note write_ml(KMeansModel, character) since 2.0.0
setMethod("write_ml", signature(object = "KMeansModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' S4 class that represents a BisectingKMeansModel
#'
#' @param jobj a Java object reference to the backing Scala BisectingKMeansModel
#' @note BisectingKMeansModel since 2.2.0
setClass("BisectingKMeansModel", representation(jobj = "jobj"))

#' Spark ML -- Bisecting K-Means Clustering
#'
#' A bisecting k-means algorithm based on the paper "A comparison of document
#' clustering techniques" by Steinbach, Karypis, and Kumar, with modification to
#' fit Spark. The algorithm starts from a single cluster that contains all
#' points. Iteratively it finds divisible clusters on the bottom level and
#' bisects each of them using k-means, until there are k leaf clusters in total
#' or no leaf clusters are divisible. The bisecting steps of clusters on the
#' same level are grouped together to increase parallelism. If bisecting all
#' divisible clusters on the bottom level would result more than k leaf
#' clusters, larger clusters get higher priority.
#'
#' Fits a bisecting k-means clustering model against a spark_tbl.
#' Users can call \code{summary} to print a summary of the fitted model,
#' \code{predict} to make predictions on new data, and \code{write_ml}/
#' \code{read_ml} to save/load fitted models.
#'
#' @param data a spark_tbl for training.
#' @param formula a symbolic description of the model to be fitted. Currently
#'                only a few formula operators are supported, including '~',
#'                '.', ':', '+', and '-'. Note that the response variable of
#'                formula is empty in ml_bisectingKmeans.
#' @param k the desired number of leaf clusters. Must be > 1. The actual number
#'          could be smaller if there are no divisible leaf clusters.
#' @param maxIter maximum iteration number.
#' @param seed the random seed.
#' @param minDivisibleClusterSize The minimum number of points (if greater than
#'                                or equal to 1.0) or the minimum proportion of
#'                                points (if less than 1.0) of a divisible
#'                                cluster. Note that it is an expert parameter.
#'                                The default value should be good enough for
#'                                most cases.
#' @param ... additional argument(s) passed to the method.
#' @return \code{ml_bisectingKmeans} returns a fitted bisecting k-means model.
#' @rdname ml_bisectingKmeans
#' @examples
#' \dontrun{
#' spark_session()
#' iris_fix <- iris %>%
#' setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
#'  mutate(Species = levels(Species)[Species])
#' iris_spk <- spark_tbl(iris)
#' model <- ml_bisectingKmeans(iris_spk, Sepal_Width ~ Sepal_Length, k = 4)
#' summary(model)
#' }
#' @export
ml_kmeans_bisecting <- function(data, formula, k = 4, maxIter = 20, seed = NULL,
                                minDivisibleClusterSize = 1) {
  formula <- paste0(deparse(formula), collapse = "")
  if (!is.null(seed)) {
    seed <- as.character(as.integer(seed))
  }
  jobj <- call_static("org.apache.spark.ml.r.BisectingKMeansWrapper",
                      "fit", attr(data, "jc"), formula, as.integer(k),
                      as.integer(maxIter), seed,
                      as.numeric(minDivisibleClusterSize))
  new("BisectingKMeansModel", jobj = jobj)
}

#' @param object a fitted bisecting k-means model.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes the model's \code{k} (number of cluster centers),
#'         \code{coefficients} (model cluster centers),
#'         \code{size} (number of data points in each cluster), \code{cluster}
#'         (cluster centers of the transformed data; cluster is NULL if is.loaded is TRUE),
#'         and \code{is.loaded} (whether the model is loaded from a saved file).
#' @rdname ml_bisectingKmeans
#' @note summary(BisectingKMeansModel) since 2.2.0
setMethod("summary", signature(object = "BisectingKMeansModel"),
          function(object) {
            jobj <- object@jobj
            is.loaded <- call_method(jobj, "isLoaded")
            features <- call_method(jobj, "features")
            coefficients <- call_method(jobj, "coefficients")
            k <- call_method(jobj, "k")
            size <- call_method(jobj, "size")
            coefficients <- t(matrix(coefficients, ncol = k))
            colnames(coefficients) <- unlist(features)
            rownames(coefficients) <- 1:k
            cluster <- if (is.loaded) {
              NULL
            } else {
              new_spark_tbl(call_method(jobj, "cluster"))
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
#' @param object a fitted bisecting k-means model.
#' @param method type of fitted results, \code{"centers"} for cluster centers
#'        or \code{"classes"} for assigned classes.
#' @rdname ml_bisectingKmeans
#' @return \code{fitted} returns a spark_tbl containing fitted values.
setMethod("fitted", signature(object = "BisectingKMeansModel"),
          function(object, method = c("centers", "classes")) {
            method <- match.arg(method)
            jobj <- object@jobj
            is.loaded <- call_method(jobj, "isLoaded")
            if (is.loaded) {
              stop("Saved-loaded bisecting k-means model does not support 'fitted' method")
            } else {
              new_spark_tbl(call_method(jobj, "fitted", method))
            }
          })

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname ml_bisectingKmeans
#' @note write_ml(BisectingKMeansModel, character) since 2.2.0
setMethod("write_ml", signature(object = "BisectingKMeansModel",
                                path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

#' S4 class that represents an LDAModel
#'
#' @param jobj a Java object reference to the backing Scala LDAWrapper
#' @note LDAModel since 2.1.0
setClass("LDAModel", representation(jobj = "jobj"))

#' Latent Dirichlet Allocation
#'
#' \code{ml_lda} fits a Latent Dirichlet Allocation model on a spark_tbl.
#' Users can call
#' \code{summary} to get a summary of the fitted LDA model.
#'
#' @param data A spark_tbl for training.
#' @param features Features column name. Either libSVM-format column or
#'        character-format column is valid.
#' @param k Number of topics.
#' @param maxIter Maximum iterations.
#' @param optimizer Optimizer to train an LDA model, "online" or "em", default
#'        is "online".
#' @param subsamplingRate (For online optimizer) Fraction of the corpus to be
#'        sampled and used in each iteration of mini-batch gradient descent,
#'        in range (0, 1].
#' @param topicConcentration concentration parameter (commonly named \code{beta}
#'        or \code{eta}) for the prior placed on topic distributions over terms,
#'        default -1 to set automatically on the Spark side. Use \code{summary}
#'        to retrieve the effective topicConcentration. Only 1-size numeric is
#'        accepted.
#' @param docConcentration concentration parameter (commonly named \code{alpha})
#'        for the prior placed on documents distributions over topics
#'        (\code{theta}), default -1 to set automatically on the Spark side.
#'        Use \code{summary} to retrieve the effective docConcentration. Only
#'        1-size or \code{k}-size numeric is accepted.
#' @param customizedStopWords stopwords that need to be removed from the given
#'        corpus. Ignore the parameter if libSVM-format column is used as the
#'        features column.
#' @param maxVocabSize maximum vocabulary size, default 1 << 18
#' @param ... additional argument(s) passed to the method.
#' @rdname ml_lda
#' @return \code{ml_lda} returns a fitted Latent Dirichlet Allocation model.
#' @seealso topicmodels: \url{https://cran.r-project.org/package=topicmodels}
#' @export
ml_lda <- function(data, features = "features", k = 10, maxIter = 20,
                   optimizer = c("online", "em"), subsamplingRate = 0.05,
                   topicConcentration = -1, docConcentration = -1,
                   customizedStopWords = "", maxVocabSize = bitwShiftL(1, 18)) {
  optimizer <- match.arg(optimizer)
  jobj <- call_static("org.apache.spark.ml.r.LDAWrapper",
                      "fit", attr(data, "jc"), features, as.integer(k),
                      as.integer(maxIter),
                      optimizer, as.numeric(subsamplingRate),
                      topicConcentration, as.array(docConcentration),
                      as.array(customizedStopWords), maxVocabSize)
  new("LDAModel", jobj = jobj)
}

#' @param object A Latent Dirichlet Allocation model fitted by \code{spark.lda}.
#' @param maxTermsPerTopic Maximum number of terms to collect for each topic. Default value of 10.
#' @return \code{summary} returns summary information of the fitted model, which is a list.
#'         The list includes
#'         \item{\code{docConcentration}}{concentration parameter commonly named \code{alpha} for
#'               the prior placed on documents distributions over topics \code{theta}}
#'         \item{\code{topicConcentration}}{concentration parameter commonly named \code{beta} or
#'               \code{eta} for the prior placed on topic distributions over terms}
#'         \item{\code{logLikelihood}}{log likelihood of the entire corpus}
#'         \item{\code{logPerplexity}}{log perplexity}
#'         \item{\code{isDistributed}}{TRUE for distributed model while FALSE for local model}
#'         \item{\code{vocabSize}}{number of terms in the corpus}
#'         \item{\code{topics}}{top 10 terms and their weights of all topics}
#'         \item{\code{vocabulary}}{whole terms of the training corpus, NULL if libsvm format file
#'               used as training set}
#'         \item{\code{trainingLogLikelihood}}{Log likelihood of the observed tokens in the
#'               training set, given the current parameter estimates:
#'               log P(docs | topics, topic distributions for docs, Dirichlet hyperparameters)
#'               It is only for distributed LDA model (i.e., optimizer = "em")}
#'         \item{\code{logPrior}}{Log probability of the current parameter estimate:
#'               log P(topics, topic distributions for docs | Dirichlet hyperparameters)
#'               It is only for distributed LDA model (i.e., optimizer = "em")}
#' @rdname ml_lda
#' @aliases summary,LDAModel-method
#' @note summary(LDAModel) since 2.1.0
setMethod("summary", signature(object = "LDAModel"),
          function(object, maxTermsPerTopic) {
            maxTermsPerTopic <- as.integer(ifelse(missing(maxTermsPerTopic), 10,
                                                  maxTermsPerTopic))
            jobj <- object@jobj
            docConcentration <- call_method(jobj, "docConcentration")
            topicConcentration <- call_method(jobj, "topicConcentration")
            logLikelihood <- call_method(jobj, "logLikelihood")
            logPerplexity <- call_method(jobj, "logPerplexity")
            isDistributed <- call_method(jobj, "isDistributed")
            vocabSize <- call_method(jobj, "vocabSize")
            topics <- new_spark_tbl(call_method(jobj, "topics",
                                                maxTermsPerTopic))
            vocabulary <- call_method(jobj, "vocabulary")
            trainingLogLikelihood <- if (isDistributed) {
              call_method(jobj, "trainingLogLikelihood")
            } else {
              NA
            }
            logPrior <- if (isDistributed) {
              call_method(jobj, "logPrior")
            } else {
              NA
            }
            list(docConcentration = unlist(docConcentration),
                 topicConcentration = topicConcentration,
                 logLikelihood = logLikelihood, logPerplexity = logPerplexity,
                 isDistributed = isDistributed, vocabSize = vocabSize,
                 topics = topics, vocabulary = unlist(vocabulary),
                 trainingLogLikelihood = trainingLogLikelihood,
                 logPrior = logPrior)
          })

#  Returns the log perplexity of a Latent Dirichlet Allocation model produced
#  by \code{ml_lda}

#' @return \code{ml_perplexity} returns the log perplexity of given
#'         spark_tbl, or the log perplexity of the training data if
#'         missing argument "data".
#' @rdname ml_lda
#' @aliases ml_perplexity,LDAModel-method
#' @note ml_perplexity(LDAModel) since 2.1.0
ml_perplexity <- function(object, data) {
            stopifnot(inherits(object, "LDAModel"))
            stopifnot(inherits(data, "spark_tbl"))
            ifelse(missing(data), call_method(object@jobj, "logPerplexity"),
                   call_method(object@jobj, "computeLogPerplexity", data@sdf))
          }

#  Returns posterior probabilities from a Latent Dirichlet Allocation model produced by ml_lda()

#' @param newData A spark_tbl for testing.
#' @return \code{ml_posterior} returns a spark_tbl containing posterior probabilities
#'         vectors named "topicDistribution".
#' @rdname ml_lda
#' @aliases ml_posterior,LDAModel,spark_tbl-method
#' @note ml_posterior(LDAModel) since 2.1.0
ml_posterior <- function(object, newData) {
            stopifnot(inherits(object, "LDAModel"))
            stopifnot(inherits(newData, "spark_tbl"))
            predict_internal(object, newData)
          }

#' @param path The directory where the model is saved.
#' @param overwrite Overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @rdname ml_lda
#' @aliases write_ml,LDAModel,character-method
#' @seealso \link{read_ml}
#' @note write_ml(LDAModel, character) since 2.1.0
setMethod("write_ml", signature(object = "LDAModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })



#' S4 class that represents a GaussianMixtureModel
#'
#' @param jobj a Java object reference to the backing Scala GaussianMixtureModel
#' @note GaussianMixtureModel since 2.1.0
setClass("GaussianMixtureModel", representation(jobj = "jobj"))

#' Multivariate Gaussian Mixture Model (GMM)
#'
#' Fits multivariate gaussian mixture model against a spark_tbl, similarly
#' to R's mvnormalmixEM(). Users can call \code{summary} to print a summary of
#' the fitted model, \code{predict} to make predictions on new data, and
#' \code{write_ml}/\code{read_ml} to save/load fitted models.
#'
#' @param data a spark_tbl for training.
#' @param formula a symbolic description of the model to be fitted. Currently
#'                only a few formula operators are supported, including '~',
#'                '.', ':', '+', and '-'. Note that the response variable of
#'                formula is empty in ml_gaussianMixture.
#' @param k number of independent Gaussians in the mixture model.
#' @param maxIter maximum iteration number.
#' @param tol the convergence tolerance.
#' @param ... additional arguments passed to the method.
#' @aliases ml_gaussianMixture,spark_tbl,formula-method
#' @return \code{ml_gaussian_mixture} returns a fitted multivariate gaussian
#'         mixture model.
#' @seealso mixtools: \url{https://cran.r-project.org/package=mixtools}
#' @rdname gaussianMixture
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
#' }
#' @export
ml_gaussian_mixture <- function(data, formula, k = 2, maxIter = 100,
                                tol = 0.01) {
  formula <- paste(deparse(formula), collapse = "")
  jobj <- call_static("org.apache.spark.ml.r.GaussianMixtureWrapper",
                      "fit", attr(data, "jc"), formula, as.integer(k),
                      as.integer(maxIter), as.numeric(tol))
  new("GaussianMixtureModel", jobj = jobj)
}

#  Get the summary of a multivariate gaussian mixture model

#' @param object a fitted gaussian mixture model.
#' @return \code{summary} returns summary of the fitted model, which is a list.
#'         The list includes the model's \code{lambda} (lambda), \code{mu} (mu),
#'         \code{sigma} (sigma), \code{loglik} (loglik), and \code{posterior} (posterior).
#' @aliases gaussianMixture,spark_tbl,formula-method
#' @rdname gaussianMixture
#' @note summary(GaussianMixtureModel) since 2.1.0
setMethod("summary", signature(object = "GaussianMixtureModel"),
          function(object) {
            jobj <- object@jobj
            is.loaded <- call_method(jobj, "isLoaded")
            lambda <- unlist(call_method(jobj, "lambda"))
            muList <- call_method(jobj, "mu")
            sigmaList <- call_method(jobj, "sigma")
            k <- call_method(jobj, "k")
            dim <- call_method(jobj, "dim")
            loglik <- call_method(jobj, "logLikelihood")
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
              new_spark_tbl(call_method(jobj, "posterior"))
            }
            list(lambda = lambda, mu = mu, sigma = sigma, loglik = loglik,
                 posterior = posterior, is.loaded = is.loaded)
          })

setMethod("predict", signature(object = "GaussianMixtureModel"),
          function(object, newData) {
            predict_internal(object, newData)
          })

#' @param path the directory where the model is saved.
#' @param overwrite overwrites or not if the output path already exists. Default is FALSE
#'                  which means throw exception if the output path exists.
#'
#' @aliases write_ml,GaussianMixtureModel,character-method
#' @rdname gaussianMixture
#' @note write_ml(GaussianMixtureModel, character) since 2.1.0
setMethod("write_ml", signature(object = "GaussianMixtureModel", path = "character"),
          function(object, path, overwrite = FALSE) {
            write_internal(object, path, overwrite)
          })

