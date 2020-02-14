
# Spark - Streaming Analysis ----------------------------------------------

# Setup -------------------------------------------------------------------

rs_env <- "experis_local"
pacman::p_load(tidyverse, sparklyr, mleap)


# Connect -----------------------------------------------------------------

sc <- spark_connect(master = "local", version = "2.3")


# Load Pipelines ----------------------------------------------------------

stream_pipenames <- dir("pipelines")
stream_filepaths <- dir("pipelines", full.names = TRUE)

time_load_pipelines <- system.time({
  stream_pipelines <-
    stream_filepaths %>%
    set_names(stream_pipenames) %>%
    as.list() %>%
    modify(ml_load, sc = sc)
})[[3]]


# Load Models -------------------------------------------------------------

stream_modelnames <- dir("models")
stream_modelpaths <- dir("models", full.names = TRUE)

stream_model_list <-
  stream_modelpaths %>%
  set_names(stream_modelnames) %>%
  as.list()

time_load_models <- system.time({
  stream_model <-
    list(fpm = stream_model_list$ml_fpm %>% ml_load(sc, .),
         cluster = stream_model_list$cluster %>% ml_load(sc, .))
})[[3]]


# Load Data ---------------------------------------------------------------

data_new <-
  spark_read_csv(sc, path = "data-small/csv/yellow_tripdata_2009-01", memory = FALSE) %>%
  sdf_sample(0.0001, replacement = FALSE)


# Get Predictions ---------------------------------------------------------

stream_prediction <-
  ml_transform(stream_pipelines$sql_rawToCluster, data_new) %>%
  ml_transform(stream_model$cluster, .) %>%
  ml_transform(stream_pipelines$sql_clusterToML, .) %>%
  ml_transform(stream_model$fpm, .) %>%
  collect()
