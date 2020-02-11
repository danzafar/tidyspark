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

Many R users, accustomed to a tidy API, seek refuge in RStudio's `sparklyr` which
uses a backend similar to `dbconnect`. With `sparkyr`, dplyr commands are translated into SQL
syntax and that is passed to SparkSQL. `sparklyr` is based on the RDD API and centers around the
Spark Context, though the RDD API will be deprecated with Spark 3.0.0. While this solves the syntax 
problem, it also creates another layer of complexity. As such, Spark experts complain of bug 
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
- `arrange`
- `gather`/`spread` (`pivot_wider`/`pivot_longer` in `dplyr` 1.0.0)
- `nest`

At this time, the package should be considered a proof-of-concept.

## Installation

You can install the **tidyspark** package from
Github as follows:

``` r
install.packages("SparkR")
devtools::install_github("danzafar/tidyspark")
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
library(tidyspark)
spark_session()
iris_tbl <- spark_tbl(iris)
flights_tbl <- spark_tbl(nycflights13::flights)
batting_tbl <- spark_tbl(Lahman::Batting)
```

To start with here’s a simple filtering example:

``` r
# filter by departure delay and print the first few records
flights_tbl %>% filter(dep_delay == 2)
```
Unlike `sparklyr`, this only returns the schema of the resulting `DataFrame`, not a sample of
the data. This approach avoids time-consuming spark jobs set off by accidental `print`s.

    A spark_tbl: ?? x 19
    [year <int>, month <int>, day <int>, dep_time <int>, sched_dep_time <int>, dep_delay <double>, arr_time <int>, sched_arr_time <int>, arr_delay <double>, carrier <string>, flight <int>, tailnum <string>, origin <string>, dest <string>, air_time <double>, distance <double>, hour <double>, minute <double>, time_hour <timestamp>]
    
To display data, use the `display` or `glimpse` commands:
``` r
# filter by departure delay and print the first few records
flights_tbl %>% filter(dep_delay == 2) %>% display
```

    # A tibble: 10 x 19
        year month   day dep_time sched_dep_time dep_delay arr_time sched_arr_time arr_delay carrier flight tailnum
       <int> <int> <int>    <int>          <int>     <dbl>    <int>          <int>     <dbl> <chr>    <int> <chr>  
     1  2013     1     1      517            515         2      830            819        11 UA        1545 N14228 
     2  2013     1     1      542            540         2      923            850        33 AA        1141 N619AA 
     3  2013     1     1      702            700         2     1058           1014        44 B6         671 N779JB 
     4  2013     1     1      715            713         2      911            850        21 UA         544 N841UA 
     5  2013     1     1      752            750         2     1025           1029        -4 UA         477 N511UA 
     6  2013     1     1      917            915         2     1206           1211        -5 B6          41 N568JB 
     7  2013     1     1      932            930         2     1219           1225        -6 VX         251 N641VA 
     8  2013     1     1     1028           1026         2     1350           1339        11 UA        1004 N76508 
     9  2013     1     1     1042           1040         2     1325           1326        -1 B6          31 N529JB 
    10  2013     1     1     1231           1229         2     1523           1529        -6 UA         428 N402UA 
    # … with 7 more variables: origin <chr>, dest <chr>, air_time <dbl>, distance <dbl>, hour <dbl>, minute <dbl>,
    #   time_hour <dttm>
    # … with ?? more rows
    
``` r
delay <- flights_tbl %>% 
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20, dist < 2000, !is.na(delay)) %>%
  collect

# plot delays
library(ggplot2)
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area(max_size = 2)
```

    ## `geom_smooth()` using method = 'gam' and formula 'y ~ s(x, bs = "cs")'
    
## Using SQL

It’s also possible to execute SQL queries directly against tables within
a Spark cluster. In order to operate on a Spark table, it must be registered
in the hive metastore as either a table or a temporary view. Unlike `sparklyr`, 
temporary views are not created by default. To register a temporary view in the 
hive metastore and run a SQL query on it:

``` r
iris_tbl %>% register_temp_view("iris")
iris_preview <- spark_sql("SELECT * FROM iris LIMIT 10")
iris_preview %>% collect
```

    ##    Sepal_Length Sepal_Width Petal_Length Petal_Width Species
    ## 1           5.1         3.5          1.4         0.2  setosa
    ## 2           4.9         3.0          1.4         0.2  setosa
    ## 3           4.7         3.2          1.3         0.2  setosa
    ## 4           4.6         3.1          1.5         0.2  setosa
    ## 5           5.0         3.6          1.4         0.2  setosa
    ## 6           5.4         3.9          1.7         0.4  setosa
    ## 7           4.6         3.4          1.4         0.3  setosa
    ## 8           5.0         3.4          1.5         0.2  setosa
    ## 9           4.4         2.9          1.4         0.2  setosa
    ## 10          4.9         3.1          1.5         0.1  setosa
