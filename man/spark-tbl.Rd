% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/spark_tbl.R
\name{spark_tbl}
\alias{spark_tbl}
\alias{spark_tbl.data.frame}
\alias{spark_tbl.SparkDataFrame}
\alias{createDataFrame}
\alias{is.spark_tbl}
\title{Create a \code{spark_tbl}}
\usage{
spark_tbl(.df, ...)

\method{spark_tbl}{data.frame}(.df, ...)

\method{spark_tbl}{SparkDataFrame}(.df, ...)

createDataFrame(.df, ...)

is.spark_tbl(.df)
}
\arguments{
\item{.df}{object coercible to \code{spark_tbl}}

\item{...}{any other arguments passed to \code{spark_tbl}, currently unused}
}
\value{
an object of class \code{spark_tbl}
}
\description{
Create a \code{spark_tbl}
}
\examples{
\dontrun{
spark_tbl(iris)
spark_tbl(tibble(x = 1:10, y = 10:1))
spark_tbl(SparkR::as.DataFrame(iris))
}
}
