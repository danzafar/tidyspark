
test_that("cache works", {

  iris_tbl <- spark_tbl(iris)
  iris_cache <- cache(iris_tbl)

  expect_equal(iris_tbl %>% collect(),
               iris_cache %>% collect())

  expect_equal(storage_level(iris_cache),
               "MEMORY_AND_DISK - StorageLevel(disk, memory, deserialized, 1 replicas)")

})

test_that("persist works", {

  iris_tbl <- spark_tbl(iris)
  iris_persist <- persist(iris_tbl, "DISK_ONLY")

  expect_equal(iris_tbl %>% collect(),
               iris_persist %>% collect())

  expect_equal(storage_level(iris_persist),
               "DISK_ONLY - StorageLevel(disk, 1 replicas)")

  unpersist(iris_persist)
  iris_persist2 <- persist(iris_persist, "MEMORY_ONLY_2")
  expect_equal(storage_level(iris_persist2),
               "MEMORY_ONLY_2 - StorageLevel(memory, deserialized, 2 replicas)")

})
