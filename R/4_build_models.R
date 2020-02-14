
# Spark - Build Models with Pipelines -------------------------------------
# In this script, I'll be using the pipelines created earlier, apply them to
# new data, and build models that will be stored for later use

# Setup -------------------------------------------------------------------

pacman::p_load(tidyverse, sparklyr, rlang, RColorBrewer, dbplot, mleap, foreach)

rs_env <- "experis_local"
data_input <- "data-small/parquet/yellow_tripdata_2009-01"
model_output <- "models"


# Connect -----------------------------------------------------------------

sc <- spark_connect(master = "local", version = "2.3")


# Step 1: Import Pipelines ------------------------------------------------

pipenames <- dir("pipelines")
filepaths <- dir("pipelines", full.names = TRUE)

time_load_pipelines <- system.time({
  pipelines <-
    filepaths %>%
    set_names(pipenames) %>%
    as.list() %>%
    purrr::modify(ml_load, sc = sc)
})[[3]]


# No need to split the data for clustering here,
# because the clusters are simply a surrogate for lat and lon, i'm not reusing data
data_raw <- spark_read_parquet(sc, path = data_input)


time_model <- system.time({
  models <- purrr::modify(pipelines, ml_fit, data_raw)
})[[3]]

data_cluster <-
  models[[1]] %>%
  ml_stage(3) %>%
  ml_summary("predictions")

data_hourly <-
  models[[1]] %>%
  ml_stage(4) %>%
  ml_transform(data_cluster)


# Step 6: Summaries -------------------------------------------------------

s_summary <-
  data_raw %>%
  summarise(n = count(),
            date_min = min(trip_pickup_datetime, na.rm = TRUE),
            date_max = max(trip_pickup_datetime, na.rm = TRUE)) %>%
  collect() %>%
  mutate_at(c("date_min", "date_max"), lubridate::date)

s_centers <-
  data_cluster %>%
  group_by(cluster) %>%
  summarise(center_lon = mean(start_lon, na.rm = TRUE),
            center_lat = mean(start_lat, na.rm = TRUE),
            size = count()) %>%
  arrange(desc(size)) %>%
  collect() %>%
  mutate(taxi_hub = LETTERS[sequence(n())] %>% as_factor(),
         hub_color = brewer.pal(n(), "Pastel1"),
         hub_pct = size / sum(size))

s_heatmap <-
  data_hourly %>%
  group_by(pickup_wday, pickup_hour) %>%
  summarise(rides = sum(rides, na.rm = TRUE),
            fpm = sum(fare, na.rm = TRUE) / sum(distance, na.rm = TRUE)) %>%
  collect() %>%
  complete(pickup_wday, pickup_hour, fill = list(rides = 0, passengers = 0, fpm = 0)) %>%
  mutate_at("pickup_wday", factor, levels = c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")) %>%
  arrange(pickup_wday, pickup_hour) %>%
  mutate_at("pickup_hour", str_pad, width = 2, pad = "0", side = "left") %>%
  mutate_at("pickup_hour", factor)


s_blackmap <-
  data_cluster %>%
  db_compute_raster(x = Start_Lon, y = Start_Lat, resolution = 500) %>%
  rename_all(str_to_lower) %>%
  rename(n = `n()`)


# Get All Summaries
export_summary <-
  ls(pattern = "^s_") %>%
  mget(inherits = TRUE)


# Export Summaries
export_summary %>% write_rds("model_summary/summary.rds")


# Step 8: Export Spark Models --------------------------------------------

export_models <-
  models %>%
  enframe("title", "model") %>%
  mutate_at("title", str_replace, pattern = "pipeline_", replacement = "model_") %>%
  mutate(filepath = file.path("models", title))

if (!dir.exists(model_output)) dir.create(model_output)


time_model_export <- system.time({
  foreach(i = 1:nrow(export_models), .errorhandling = "pass") %do% {
    .model <- export_models$model[[i]]
    .path  <- export_models$filepath[[i]]
    ml_save(.model, .path, overwrite = TRUE)
  }
})[[3]]



# Step 9: Export Run Time -------------------------------------------------

export_time <-
  ls(pattern = "time_") %>%
  mget(inherits = TRUE) %>%
  enframe("action", "time") %>%
  mutate(timestamp = Sys.time(),
         environment = rs_env) %>%
  unnest(time)

export_time %>% write_csv("runtime_logs/build_models.csv")


# Step 9: Disconnect ------------------------------------------------------

spark_disconnect(sc)
