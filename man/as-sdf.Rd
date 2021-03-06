% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/spark_tbl.R
\name{as_SparkDataFrame}
\alias{as_SparkDataFrame}
\alias{as_SparkDataFrame.spark_tbl}
\alias{as_SparkDataFrame.jobj}
\title{Convert to a SparkR \code{SparkDataFrame}}
\usage{
as_SparkDataFrame(x, ...)

\method{as_SparkDataFrame}{spark_tbl}(x, ...)

\method{as_SparkDataFrame}{jobj}(x, ...)
}
\arguments{
\item{x}{a \code{spark_tbl} or \code{jobj} representing a \code{DataFrame}}

\item{...}{additional arguments passed on to methods, currently unused}
}
\description{
Convert to a SparkR \code{SparkDataFrame}
}
\examples{

\dontrun{
spark_session()

df <- spark_tbl(iris)
as_SparkDataFrame(df)

df_jobj <- attr(df, "jc")
as_SparkDataFrame(df_jobj)

spark_session_stop()
}

}
