# library(SparkR)
library(dplyr)

SparkR::sparkR.session()
# SparkR::sparkR.session.stop()

iris_sdf <- SparkR::createDataFrame(iris)

# it looks like the SparkDataFrame S4 object is just not
# compatible with NSE because it is not a data.frame. Since it's not a
# data frame we can't use data masks with it see:
# https://adv-r.hadley.nz/evaluation.html#data_masks

# because of this, I think we need to create a new class to work
# as an intermediary between tidy eval and SparkDataFrame
# This needs to be either a list or a data frame to be able to work
# with data masks. My preference would be a data frame for optics,
# Each column will need to call a column for the SparkDataFrame
# LATER: under further inspection only a list will be able to sub out cols.

# create a low-level constructor for an new S3 class called "spark_tbl"
# following tidy guidelines here https://adv-r.hadley.nz/s3.html#constructors
new_spark_tbl <- function(sdf) {
  if (class(sdf) != "SparkDataFrame") {
    stop("Incoming object of class ", class(sdf),
         "must be of class 'SparkDataFrame'")
  }
  spk_tbl <- structure(lapply(names(sdf), function(x) sdf[[x]]),
                       class = c("spark_tbl", "list"),
                       DataFrame = sdf)
  names(spk_tbl) <- names(sdf)
  spk_tbl
}

# Let's create a helper generic
spark_tbl <- function(x, ...) {
  UseMethod("spark_tbl")
}

# create a method for data.frame (in memory) objects
spark_tbl.data.frame <- function(.df) {
  df <- SparkR::createDataFrame(.df)
  new_spark_tbl(df)
}

# let's give it a print statement, pretty similar to
# getMethod("show","SparkDataFrame") from SparkR
# I want to avoid printing rows, it's just spark to avoid collects
print.spark_tbl <- function(x) {
  cols <- lapply(SparkR::dtypes(attr(x, "DataFrame")),
                 function(l) {
                   paste0(paste(l, collapse = " <"), ">")
                 })
  s <- paste(cols, collapse = ", ")
  cat(paste(class(x)[1], "[", s, "]\n", sep = ""))
}

# in case we do want to see results, let's give a display
display <- function(x, n = NULL) {

  rows <- if (is.null(n)) {
    getOption("tibble.print_min", getOption("dplyr.print_min", 10))
  } else n

  print(as_tibble(SparkR::take(attr(x, "DataFrame"), rows)))
  cat("# … with ?? more rows")

}

# collect
collect.spark_tbl <- function(x) {
  as_tibble(SparkR::collect(attr(x, "DataFrame")))
}

iris_spk <- spark_tbl(iris)

# Now we need to come up with a way to update the DataFrame being stored
# with the columns that may have been updated in the last step, let's just
# replace them in a for loop (is case there is iterative stuff?)

update_DataFrame <- function(spk_tbl) {
  DF <- attr(spk_tbl, "DataFrame")

  # update the columns iteratively
  for (i in seq_along(spk_tbl)) {
    DF <- SparkR::withColumn(DF, names(spk_tbl)[[i]], spk_tbl[[i]])
  }

  # drop any stale columns that might have gotten dropped
  to_drop <- names(DF)[!(names(DF) %in% names(spk_tbl))]

  # replace it
  attr(spk_tbl, "DataFrame") <- SparkR::drop(DF, to_drop)

  spk_tbl
}

# Now just playing around I figured out the mutate should be easy
# I can't believe that worked

iris_spk$Sepal_Length <- iris_spk$Sepal_Length + 1

update_DataFrame(iris_spk) %>% collect

# let's go ahead and create the mutate and see what happens
mutate.spark_tbl <- function(.data, ...) {
  require(rlang)
  dots <- enquos(...)

  for (i in seq_along(dots)) {
    name <- names(dots)[[i]]
    dot <- dots[[i]]

    .data[[name]] <- eval_tidy(dot, .data)
  }

  update_DataFrame(.data)
}

# it looks like it works for simple stuff, the issue arises
# when i try to create  new column:
iris_spk <- spark_tbl(iris)
iris_spk %>%
  mutate(Sepal_Area = Sepal_Length * Sepal_Width)

# I get the Spark exception:
# Resolved attribute(s) Sepal_Length#297,Sepal_Width#298 missing from
# Petal_Width#325,Petal_Length#319,Sepal_Length#307,Sepal_Width#313,Species#331
# in operator !Project [Sepal_Length#307, Sepal_Width#313, Petal_Length#319,
# Petal_Width#325, Species#331, (Sepal_Length#297 * Sepal_Width#298) AS Sepal_Area#337].
# Attribute(s) with the same name appear in the operation: Sepal_Length,Sepal_Width.
# Please check if the right attribute(s) are used.

# it looks like Column state is being maintained using the #<number> marker on the
# column. Running the withColumn multiple times is screwing that up because by the
# time it gets to Sepal_Area, the column tags are updated from the original DF.
# If thats true, I'm not sure why the other columns work? But if we only run i=6 in
# update_DataFrame, then it works fine. Ideally we could use withColumns to update
# all at once, but that hasn't been coded in Spark R.

# I may be able to use 'sdf <- callJMethod(x@sdf, "select", cols)' to call spark
# directly, doing so I would essentially be discarding of the idea that I am
# coding a wrapper around SparkR, I would be just using it's most basic functions.
# to start, let's see if I can leverage the withColumns() straight from Java

# testing out that functionality so I know how it works:
# choose two columns, save as list
test_cols <- list(iris_spk$Sepal_Length, iris_spk$Sepal_Width)
# for each, get the java reference
cols_in <- test_cols %>% lapply(function(c) c@jc)
# now call select
sdf <- SparkR:::callJMethod(attr(iris_spk, "DataFrame")@sdf, "select", cols_in)

update_DataFrame <- function(spk_tbl) {
  DF <- attr(spk_tbl, "DataFrame")
  jcols <- lapply(spk_tbl, function(c) {
    if (class(c) == "Column") {
      c@jc
    }
    else {
      col(c)@jc
    }
  })
  names(jcols) <- ifelse(names(jcols) %in% names(DF), seq_along(jcols), names(jcols))
  # browser()
  DF@sdf <- SparkR:::callJMethod(DF@sdf, "select", jcols)
  attr(spk_tbl, "DataFrame") <- DF # setNames(DF, names(cols))

  spk_tbl
}

iris_spk %>%
  mutate(Sepal_Area = Sepal_Length * Sepal_Width) %>%
  collect()
# works!

# let's try arbitrily more complicated stuff
iris_spk %>%
  mutate(Sepal_Area = Sepal_Length * Sepal_Width,
         Petal_Area = Petal_Length * Petal_Width,
         tot_Sepal_Petal = Sepal_Area + Petal_Area,
         ralph = SparkR:::lit("a")) %>%  # this is a problem...
  #mutate(tot_Sepal_Petal = Sepal_Area^2 + Petal_Area^3) %>%
  collect()

# the other mutate vars should work too

# after i define this...
tbl_vars.spark_tbl <- function(x) {
  names(x)
}

iris_spk <- spark_tbl(iris)
iris_spk %>%
  mutate_all(~ . + 5) %>%
  mutate_at(vars(matches("etal")), ~ .-1000.0) %>%
  collect()

# these work individually, but looks like mutates are not working in chain

# so looks like lit() still has to be used....also need to fix that

# let's work on the chain first

iris_spk <- spark_tbl(iris)
iris_spk %>%
  mutate(Sepal_Area = Sepal_Length * Sepal_Width) %>%
  mutate(Petal_Area = Petal_Length * Petal_Width) %>%
  collect()

# made a function to inspect the numbers of the java objects
inspect_java <- function(sdf) {
  df <- attr(sdf, "DataFrame")
  cat("DataFrame: ")
  print(df@sdf)
  sapply(names(df), function(x) df[[x]]@jc)
  df[[1]]@jc
}

# it provides...unexpected results. Check out the numbers changing?
inspect_java(iris_spk)
inspect_java(iris_spk)

# this is the 'mystery of the changing IDs'!!!

# no change in env #
iris_spk[[1]]@jc
iris_spk[[1]]@jc

attr(iris_spk, "DataFrame")[[1]]@jc
attr(iris_spk, "DataFrame")[[1]]@jc
# diff of 8

# first let's eliminate the attr call
v <- attr(iris_spk, "DataFrame")
v[[1]]@jc
v[[1]]@jc
# still 8, it does not have to do with the attr call

# next let's get rid of the `[[` call
k <- attr(iris_spk, "DataFrame")[[1]]
k@jc
k@jc
# bingo, it's the subset...which is a primative

v$Sepal_Length@jc
v$Sepal_Length@jc


# OK I just had another brainstorm. It seems like my issue is caused by
# a difference between the stored columns and the DataFrame's columns
# so why not just always operate off of the DataFrame's columns?
# doing it this way also obviates the need for update_spark_tbl()
# instead I just create a new spark_tbl on the other side.
mutate.spark_tbl <- function(.data, ...) {
  require(rlang)
  dots <- enquos(...)
  #browser()
  sdf <- attr(.data, "DataFrame")

  for (i in seq_along(dots)) {
    name <- names(dots)[[i]]
    dot <- dots[[i]]

    df_cols <- lapply(names(sdf), function(x) sdf[[x]])
    sdf[[name]] <- eval_tidy(dot, setNames(df_cols, names(sdf)))
  }

  new_spark_tbl(sdf)
}

# let's run our tests
iris_spk <- spark_tbl(iris)
iris_spk %>%
  mutate(Sepal_Area = Sepal_Length * Sepal_Width) %>%
  mutate(Petal_Area = Petal_Length * Petal_Width) %>%
  collect()
# passes, also puts the correct column name!

iris_spk %>%
  mutate_if(is.numeric, ~ . + 5) %>%
  mutate_at(vars(matches("etal")), ~ .-1000.0) %>%
  collect()
# somehow this...also works!? I'm surprised `is.numeric(iris_sdf[[5]])` works

iris_spk %>%
  mutate(Sepal_Area = Sepal_Length * Sepal_Width,
         Petal_Area = Petal_Length * Petal_Width) %>%
  collect()
# looks like there's an issue with multiple args to mutate

iris_spk %>%
  mutate(Sepal_Area = Sepal_Length * Sepal_Width,
         Petal_Area = Petal_Length * Petal_Width,
         tot_Sepal_Petal = Sepal_Area + Petal_Area,
         ralph = "a") %>%
  mutate(tot_Sepal_Petal = Sepal_Area^2 + Petal_Area^3) %>%
  collect()
