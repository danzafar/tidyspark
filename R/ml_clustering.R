# K- Means
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

# Bisecting K-Means
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

# LDA
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

# GMM

ml_gaussian_mixture <- function(data, formula, k = 2, maxIter = 100,
                                tol = 0.01) {
  formula <- paste(deparse(formula), collapse = "")
  jobj <- call_static("org.apache.spark.ml.r.GaussianMixtureWrapper",
                      "fit", attr(data, "jc"), formula, as.integer(k), as.integer(maxIter),
                      as.numeric(tol))
  new("GaussianMixtureModel", jobj = jobj)
}

