% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/spark.R
\name{register_temp_view}
\alias{register_temp_view}
\title{Create or replace a temporary view}
\usage{
register_temp_view(.data, name)
}
\arguments{
\item{.data}{a \code{spark_tbl} to be registered}

\item{name}{a \code{string} of the name to store the table as}
}
\description{
similar to Spark's \code{createOrReplaceTemporaryView} method,
this function registers the DAG at a given point in the lineage as a temp view
in the hive metastore. It does not cache the data, but the name supplied can
be used in future Spark SQL queries.
}
\examples{
\dontrun{
spark_tbl(iris) \%>\% register_temp_view("iris")
iris_preview <- spark_sql("SELECT * FROM iris LIMIT 10")
iris_preview \%>\% collect
}
}
