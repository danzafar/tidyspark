
iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
# batting_tbl <- spark_tbl(Lahman::Batting)
flights_tbl <- spark_tbl(nycflights13::flights)

test_that("simple filter", {
  expect_equal(flights_tbl %>%
                 filter(dep_delay == 2) %>%
                 collect %>% select(1:5) %>%
                 as.list,
               nycflights13::flights %>%
                 filter(dep_delay == 2) %>%
                 select(1:5) %>%
                 as.list)
})

test_that("group_by, summarise, filter on flights_tbl", {
  expect_equal(flights_tbl %>%
                 group_by(tailnum) %>%
                 summarise(count = n(),
                           dist = mean(distance),
                           delay = mean(arr_delay)) %>%
                 filter(count > 20, dist < 2000, !is.na(delay)) %>%
                 collect %>%
                 arrange(tailnum) %>%
                 mutate(delay = round(delay, 16)),
               nycflights13::flights %>%
                 group_by(tailnum) %>%
                 summarise(count = as.numeric(dplyr::n()),
                           dist = mean(distance),
                           delay = round(mean(arr_delay, na.rm = T), 16)) %>%
                 filter(count > 20, dist < 2000, !is.na(delay)) %>%
                 arrange(tailnum)
               )
})

test_that("spark_sql works as advertised", {
  spark_tbl(iris) %>% register_temp_view("iris")
  iris_preview <- spark_sql("SELECT * FROM iris LIMIT 10")
  expect_equal(iris_preview %>% collect,
               iris %>%
                 setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
                 mutate(Species = levels(Species)[Species]) %>%
                 head(10))
})

test_that("window function", {
  batting_tbl <- spark_tbl(Lahman::Batting)

  expect_equal(batting_tbl %>%
                 select(playerID, yearID, teamID, G, AB:H) %>%
                 arrange(playerID, yearID, teamID) %>%
                 group_by(playerID) %>%
                 filter(min_rank(desc(H)) <= 2 & H > 0) %>%
                 collect %>%
                 arrange(playerID, yearID, teamID),
               Lahman::Batting %>%
                 select(playerID, yearID, teamID, G, AB:H) %>%
                 arrange(playerID, yearID, teamID) %>%
                 group_by(playerID) %>%
                 filter(rank(desc(H), na.last = 'keep', ties.method = 'min') <= 2 &
                          H > 0) %>%
                 mutate(teamID = levels(teamID)[teamID]))
})

