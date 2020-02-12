
# Spark - Build Models with Pipelines -------------------------------------
# In this script, I'll be using the pipelines created earlier, apply them to
# new data, and build models that will be stored for later use

# Setup -------------------------------------------------------------------

rs_env <- "experis_local"
pacman::p_load(tidyverse, sparklyr, rlang, RColorBrewer, dbplot)

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


# Step 2: Import Data -----------------------------------------------------

data_raw <-
  spark_read_parquet(
    sc,
    name = "full_data",
    path = "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi/data-small/parquet/yellow_tripdata_2009-01")


# Step 3: Prepare and Split -----------------------------------------------

data_prep <-
  ml_transform(pipelines$sql_rawToCluster, data_raw) %>%
  sdf_random_split(cluster = 0.50, predict = 0.50, seed = 42)

data_cluster <- data_prep$cluster
data_predict <- data_prep$predict

# Step 4: Model Cluster ---------------------------------------------------

time_model_cluster <- system.time({
  model_cluster <- ml_fit(pipelines$model_cluster, data_cluster)
})[[3]]


# Step 5: Prepare for Predictions -----------------------------------------

data_predict_clus <- ml_transform(model_cluster, data_predict)
data_predict_prep <- ml_transform(pipelines$sql_clusterToML, data_predict_clus)


# Step 6: Model Predictions -----------------------------------------------

time_model_predict <- system.time({
  model_predict <-
    pipelines %>%
    enframe("title", "pipeline") %>%
    filter(str_detect(title, "model_ml_")) %>%
    mutate(model = map(pipeline, ml_fit, dataset = data_predict_prep))
})[[3]]


# Step X: Summaries -------------------------------------------------------

s_count <- data_raw %>% count()

s_centers <-
  data_predict_clus %>%
  group_by(cluster) %>%
  summarise(center_lon = mean(start_lon),
            center_lat = mean(start_lat),
            size = count()) %>%
  arrange(desc(size)) %>%
  collect() %>%
  mutate(taxi_hub = LETTERS[sequence(n())] %>% as_factor(),
         hub_color = brewer.pal(n(), "Pastel1"),
         hub_pct = size / sum(size))

s_heatmap <-
  data_predict %>%
    group_by(pickup_wday, pickup_hour) %>%
    summarise(rides = count(),
              fpm = sum(fare_amt, na.rm = TRUE) / sum(trip_distance, na.rm = TRUE)) %>%
    collect() %>%
    complete(pickup_wday, pickup_hour, fill = list(rides = 0, passengers = 0, fpm = 0)) %>%
    mutate_at("pickup_wday", factor, levels = c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")) %>%
    arrange(pickup_wday, pickup_hour) %>%
    mutate_at("pickup_hour", str_pad, width = 2, pad = "0", side = "left") %>%
    mutate_at("pickup_hour", factor)


s_blackmap <-
  data_predict %>%
  db_compute_raster(x = start_lon, y = start_lat, resolution = 500) %>%
  rename(n = `n()`)

# Carry Summaries Forward
export_summary <-
  ls(pattern = "^s_") %>%
  mget(inherits = TRUE) %>%


export_summary %>% write_rds("model_summary/summary.rds")

# --- For Show Only ---
# s_heatmap %>%
#   ggplot(aes(x = pickup_hour, y = pickup_wday, fill = rides)) +
#   geom_tile() +
#   scale_fill_viridis_c(label = scales::number_format())
#
# s_heatmap %>%
#   ggplot(aes(x = pickup_hour, y = pickup_wday, fill = fpm)) +
#   geom_tile() +
#   scale_fill_viridis_c(label = scales::dollar_format())
#
# library(leaflet)
# s_blackmap %>%
#   leaflet() %>%
#   addProviderTiles(providers$CartoDB.DarkMatterNoLabels) %>%
#   addCircles(lng = ~start_lon,
#              lat = ~start_lat,
#              opacity = ~n,
#              radius = ~n,
#              stroke = FALSE,
#              color = "yellow")


# Step 7: Export Models ---------------------------------------------------

export_models <-
  mget("model_cluster") %>%
  enframe("title", "model") %>%
  bind_rows(model_predict) %>%
  mutate_at("title", str_replace, pattern = "model_", replacement = "models/")

time_model_export <- system.time({
  foreach(i = 1:nrow(export_models), .errorhandling = "pass") %do% {
    .model <- export_models$model[[i]]
    .path  <- export_models$title[[i]]
    ml_save(.model, .path, overwrite = TRUE)
  }
})[[3]]


# Step 8: Export Run Time -------------------------------------------------

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
