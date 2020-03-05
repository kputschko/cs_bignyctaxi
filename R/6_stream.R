
# Spark - Streaming Analysis ----------------------------------------------

# Setup -------------------------------------------------------------------

rs_env <- "experis_local"
pacman::p_load(tidyverse, sparklyr, mleap)


# Connect -----------------------------------------------------------------

sc <- spark_connect(master = "local", version = "2.3")


# Load Pipelines ----------------------------------------------------------

stream_pipenames <- dir("pipelines/2020-02-26")
stream_filepaths <- dir("pipelines/2020-02-26", full.names = TRUE)

time_load_pipelines <- system.time({
  stream_pipelines <-
    stream_filepaths %>%
    set_names(stream_pipenames) %>%
    as.list() %>%
    modify(ml_load, sc = sc)
})[[3]]


# Load Models -------------------------------------------------------------

stream_modelnames <- dir("models/2020-02-26", pattern = "model_")
stream_modelpaths <- dir("models/2020-02-26", full.names = TRUE, pattern = "model_")

stream_model_list <-
  stream_modelpaths %>%
  set_names(stream_modelnames) %>%
  as.list()

time_load_models <- system.time({
  stream_model <-
    list(fpm = stream_model_list$model_fpm_local  %>% ml_load(sc, path = .),
         cluster = stream_model_list$model_rides_local %>% ml_load(sc, path = .))
})[[3]]


# Load Data ---------------------------------------------------------------

data_new <-
  spark_read_csv(sc, path = "data-small/csv", memory = FALSE) %>%
  sdf_sample(0.0001, replacement = FALSE)

data_grid <-
  list(cluster = 1:5,
       weekday = 1:7,
       hour = 0:23) %>%
  cross_df()

# Get Predictions ---------------------------------------------------------

stream_model$fpm %>% ml_predict(data_new)

# stream_prediction <-
#   ml_transform(stream_pipelines$sql_rawToCluster, data_new) %>%
#   ml_transform(stream_model$cluster, .) %>%
#   ml_transform(stream_pipelines$sql_clusterToML, .) %>%
#   ml_transform(stream_model$fpm, .) %>%
#   collect()
