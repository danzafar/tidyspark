% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/WindowSpec.R
\name{partitionBy}
\alias{partitionBy}
\alias{partitionBy,WindowSpec-method}
\title{partitionBy}
\usage{
\S4method{partitionBy}{WindowSpec}(x, col, ...)
}
\arguments{
\item{x}{a WindowSpec.}

\item{col}{a column to partition on (desribed by the name or Column).}

\item{...}{additional column(s) to partition on.}
}
\value{
A WindowSpec.
}
\description{
Defines the partitioning columns in a WindowSpec.
}
\note{
partitionBy(WindowSpec) since 2.0.0
}
\examples{
\dontrun{
  partitionBy(ws, "col1", "col2")
  partitionBy(ws, df$col1, df$col2)
}
}
\seealso{
Other windowspec_method: 
\code{\link{orderBy}()},
\code{\link{rangeBetween}()},
\code{\link{rowsBetween}()}
}
\concept{windowspec_method}
