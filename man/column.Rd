% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/generics.R, R/columns.R, R/functions.R
\name{column}
\alias{column}
\alias{column,jobj-method}
\alias{column,character-method}
\title{Returns a Column based on the given column name}
\usage{
column(x)

\S4method{column}{jobj}(x)

\S4method{column}{character}(x)
}
\arguments{
\item{x}{Character column name.}
}
\description{
Returns a Column based on the given column name.
}
\note{
column since 1.6.0
}
\examples{
\dontrun{column("name")}
}
\seealso{
Other non-aggregate functions: 
\code{\link{column_nonaggregate_functions}},
\code{\link{not}()}
}
\concept{non-aggregate functions}
