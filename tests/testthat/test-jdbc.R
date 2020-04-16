
# NOTES: I'm having some M'FUCKIN ISSUES with my connecting to my postgres and mysql DBs.I'm going to continue debugging this later...

# # load the jdbc driver that's on my desktop
# spark_session_reset(sparkPackages = c("org.postgresql:postgresql:42.2.12", "mysql:mysql-connector-java:5.1.48"))
#
# iris_tbl <- spark_tbl(iris)
#
# iris_tbl %>%
#   spark_write_jdbc(url = "jdbc:postgresql://localhost/tidyspark_test",
#                    table = "iris_test",
#                    partition_by = "Species",
#                    mode = "overwrite",
#                    user = "tidyspark_tester", password = "test4life",
#                    driver = "org.postgresql.Driver")
#
# test_that("read jdbc with specified postgres driver", {
#   iris_fix <- iris %>%
#     setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
#     mutate(Species = levels(Species)[Species])
#
#   expect_equal(
#     spark_read_jdbc(url = "jdbc:postgresql://localhost/tidyspark_test",
#                     table = "iris_test",
#                     driver = "org.postgresql.Driver") %>%
#       collect,
#     iris_fix)
# })
#
# test_that("read jdbc with mysql", {
#
#   spark_tbl(data.frame(index = 0:100, field = 100:200)) %>%
#     spark_write_jdbc(url = "jdbc:mysql://localhost:3306/tidyspark_test",
#                      table = "table1",
#                      mode = "overwrite",
#                      driver = "com.mysql.jdbc.Driver",
#                      user = "tidyspark_tester",
#                      password = "test4life")
#
#   expect_equal(
#     spark_read_jdbc(url = "jdbc:mysql://localhost:3306/tidyspark_test",
#                     table = "table1",
#                     predicates = list("field <= 123"),
#                     driver = "com.mysql.jdbc.Driver",
#                     user = "tidyspark_tester",
#                     password = "test4life") %>%
#       collect,
#     data.frame(index = 0:100, field = 100:200))
#
#   expect_equal(
#     spark_read_jdbc(url = "jdbc:mysql://localhost:3306/tidyspark_test",
#                     table = "table1",
#                     partition_by = "index",
#                     lower_bound = 0,
#                     upper_bound = 100,
#                     driver = "com.mysql.jdbc.Driver",
#                     user = "tidyspark_tester",
#                     password = "test4life") %>%
#       collect,
#     data.frame(index = 0:100, field = 100:200))
#
# })
#
