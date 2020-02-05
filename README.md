tidyspark: tidy interface for Apache SparkR
================

## Motivation
With the advent of the `tidyverse`, R coders expect a clean, tidy,
and standardized API for interacting with packages. Unfortunetely Apache SparkR's
interface contains numererous namespace conflicts with dplyr:

```
> library(SparkR)

Attaching package: ‘SparkR’

The following objects are masked from ‘package:dplyr’:

    arrange, between, coalesce, collect, contains, count, cume_dist,
    dense_rank, desc, distinct, explain, expr, filter, first, group_by,
    intersect, lag, last, lead, mutate, n, n_distinct, ntile,
    percent_rank, rename, row_number, sample_frac, select, slice, sql,
    summarize, union

The following objects are masked from ‘package:stats’:

    cov, filter, lag, na.omit, predict, sd, var, window

The following objects are masked from ‘package:base’:

    as.data.frame, colnames, colnames<-, drop, endsWith, intersect,
    rank, rbind, sample, startsWith, subset, summary, transform, union
```

and can be clunky:

```
# Prevent duplicated columns when joining two dataframes
library(SparkR)
drop(join(left, right, left$name == right$name, "left_outer"), left$name)
```

Many R users, accustomed to a tidy API seek refuge in RStudio's `sparklyr` which
uses a backend similar to `dbconnect`. With `sparkyr`, dplyr commands are translated into SQL
syntax and that is passed to SparkSQL. While this solves the syntax problem, it also
creates another layer of complexity which is prone to error. Thus, Spark experts complain of bug 
and performance issues. Running `SparkR` and `sparklyr` code side-by-side, `SparkR` commonly shows 
more efficent execution plans. This eventually leads to the question, "choose one: syntax or functionality?".

`tidyspark` represents a "best of both worlds" approach. The goal of `tidyspark` is to provide a
tidy wrapper to `SparkR` so that R users can work with Spark through `dplyr`. This ensures that
R users get the funcationality of `SparkR` with the syntax of `sparklyr`.

## Status
This library is nacent (first code written Jan 23, 2020). The primary focus is to bring Spark's DataFrame API into dplyr methods, while preserving as many ancillary functions (such as `n_distinct`, `n()`, `length`, etc.) as possible. So far, the following `dplyr` verbs are supported:

- `select`/`rename`
- `mutate`
- `filter`
- all joins except `cross_joins`
- `summarise`
- `group_by(...) %>% summarise(...)`

The following workflows are still being developed
- `group_by(...) %>% mutate(...)`
- `group_by(...) %>% filter(...)`
- `gather`/`spread` (`pivot_wider`/`pivot_longer` in `dplyr` 1.0.0)
- `nest`

At this time, the package should be considered a proof-of-concept.

## Installation

You can install the **tidyspark** package from
Github as follows:

``` r
install.packages("SparkR")
devtools::install_github("tidyspark")
```

You should also install a local version of Spark for development
purposes:

``` r
SparkR::install.spark()
```

## Connecting to Spark

You can connect to both local instances of Spark as well as remote Spark
clusters. Here we’ll connect to a local instance of Spark via the
`spark_session`, which is a thin wrapper of SparkR's `sparkR.session`
function, and supports all the same arguments.

``` r
library(tidyspark)
spark_session()
```

Unlike sparklyr, the output of the session is stored in the R session environment 
and it is not needed in subsequent code. For more information on connecting to remote 
Spark clusters you can use the `master` argument for `spark_session`. Refer to 
[this page](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls)
for more information.

## Using dplyr

Following `sparklyr`'s readme, we can now use all of the available dplyr verbs against the tables
within the cluster.

We’ll start by copying some datasets from R into the Spark cluster (note
that you may need to install the nycflights13 and Lahman packages in
order to execute this code):

``` r
install.packages(c("nycflights13", "Lahman"))
```

``` r
spark_session()
library(dplyr)
iris_tbl <- spark_tbl(iris)
flights_tbl <- spark_tbl(nycflights13::flights)
batting_tbl <- spark_tbl(Lahman::Batting)
```

To start with here’s a simple filtering example:

``` r
# filter by departure delay and print the first few records
flights_tbl %>% filter(dep_delay == 2)
```
