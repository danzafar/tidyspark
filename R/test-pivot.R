library(testthat)
library(dplyr)

mtcars_spk <- data.frame(A = c("G", "H"),
                         X = c(4, 1),
                         Y = c(20, 40),
                         Z = c(200, 400)) %>%
  spark_tbl() %>%
  piv_longer(cols = c(X,Y,Z), names_to = "name_stuff", values_to = "number_stuff")


mtcars %>%
  tidyr::pivot_longer(cols = c(mpg, cyl), names_to = "name_stuff", values_to = "number_stuff")
