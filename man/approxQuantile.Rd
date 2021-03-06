% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/stats.R
\name{approxQuantile}
\alias{approxQuantile}
\alias{approxQuantile,spark_tbl,character,numeric,numeric-method}
\title{Calculates the approximate quantiles of numerical columns of a spark_tbl}
\usage{
approxQuantile(x, cols, probabilities, relativeError)
}
\arguments{
\item{x}{A spark_tbl.}

\item{cols}{A single column name, or a list of names for multiple columns.}

\item{probabilities}{A list of quantile probabilities. Each number must belong to [0, 1].
For example 0 is the minimum, 0.5 is the median, 1 is the maximum.}

\item{relativeError}{The relative target precision to achieve (>= 0). If set to zero,
the exact quantiles are computed, which could be very expensive.
Note that values greater than 1 are accepted but give the same result as 1.}
}
\value{
The approximate quantiles at the given probabilities. If the input is a single column
        name, the output is a list of approximate quantiles in that column; If the input is
        multiple column names, the output should be a list, and each element in it is a list of
        numeric values which represents the approximate quantiles in corresponding column.
}
\description{
Calculates the approximate quantiles of numerical columns of a spark_tbl.
The result of this algorithm has the following deterministic bound:
If the spark_tbl has N elements and if we request the quantile at probability p up to
error err, then the algorithm will return a sample x from the spark_tbl so that the
*exact* rank of x is close to (p * N). More precisely,
  floor((p - err) * N) <= rank(x) <= ceil((p + err) * N).
This method implements a variation of the Greenwald-Khanna algorithm (with some speed
optimizations). The algorithm was first present in [[https://doi.org/10.1145/375663.375670
Space-efficient Online Computation of Quantile Summaries]] by Greenwald and Khanna.
Note that NA values will be ignored in numerical columns before calculation. For
  columns only containing NA values, an empty list is returned.
}
\note{
approxQuantile since 2.0.0
}
\examples{
\dontrun{
df <- read.json("/path/to/file.json")
quantiles <- approxQuantile(df, "key", c(0.5, 0.8), 0.0)
}
}
\seealso{
Other stat functions: 
\code{\link{corr}()},
\code{\link{covariance}()},
\code{\link{crosstab}()},
\code{\link{freqItems}()},
\code{\link{sampleBy}()}
}
\concept{stat functions}
