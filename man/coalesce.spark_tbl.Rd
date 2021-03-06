% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/spark_tbl.R
\name{coalesce.spark_tbl}
\alias{coalesce.spark_tbl}
\title{Coalesce the number of partitions in a \code{spark_tbl}}
\usage{
\method{coalesce}{spark_tbl}(.data, n_partitions, ...)
}
\arguments{
\item{.data}{a \code{spark_tbl}}

\item{n_partitions}{integer, the number of partitions to resize to}

\item{...}{additional argument(s), currently unused}
}
\description{
Returns the newly coalesced spark_tbl.
}
