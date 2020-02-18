
# Spark - Build Models with Pipelines -------------------------------------
# In this script, I'll be using the pipelines created earlier, apply them to
# new data, and build models that will be stored for later use

# User Info ---------------------------------------------------------------

r_env <- "local"
input_pipeline <- NULL


# R Setup -----------------------------------------------------------------
pacman::p_load(tidyverse, sparklyr, rlang, RColorBrewer, dbplot, foreach)

source("R/0_config.R")
script_config <- master_config %>% pluck(r_env)

script_config$input_pipeline <- if (!is_empty(input_pipeline)) input_pipeline


# Connect to Spark --------------------------------------------------------

sc_config <- spark_config()
sc_config$spark.driver.memory <- script_config$spark_memory
sc <- spark_connect("local", config = sc_config)


# Step 1: Import Pipelines ------------------------------------------------

filepaths <-
  file.path(script_config$filepath_pipelines,
            script_config$input_pipeline) %>%
  dir(full.names = TRUE)

pipenames <- basename(filepaths)

time_model.load_pipelines <- system.time({
  pipelines <-
    filepaths %>%
    set_names(pipenames) %>%
    as.list() %>%
    purrr::modify(ml_load, sc = sc)
})[[3]]


# Import Data -------------------------------------------------------------
# No need to split the data for clustering here,
# because the clusters are simply a surrogate for lat and lon, i'm not reusing data

data_raw <-
  spark_read_parquet(sc,
                     path = script_config$filepath_parquet,
                     memory = FALSE,
                     name = "data_raw")


# Fit Models --------------------------------------------------------------

time_model.fit_models <- system.time({
  models <- purrr::modify(pipelines, ml_fit, data_raw)

  data_cluster <-
    models[[1]] %>%
    ml_stage(3) %>%
    ml_summary("predictions")

  data_hourly <-
    models[[1]] %>%
    ml_stage(4) %>%
    ml_transform(data_cluster)
})[[3]]


# Step 6: Summaries -------------------------------------------------------

time_model.model_summaries <- system.time({

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

  # og author round lon/lat to 4 decimals places, counts by group
  # count is alpha and size, on log scale
  # alpha range is 0.10, 0.75
  # size range is 0.134, 0.173; both *4
  s_blackmap <-
    data_cluster %>%
    mutate(lon_r = round(start_lon, 3),
           lat_r = round(start_lat, 3)) %>%
    group_by(lat_r, lon_r) %>%
    count() %>%
    collect() %>%
	sample_n(10000)


  # Get All Summaries
  export_summary <-
    ls(pattern = "^s_") %>%
    mget(inherits = TRUE)


  # Export Summaries
  output_models <- script_config$filepath_models %>% file.path(Sys.Date())
  output_models %>% fx_dir_create()

  export_summary %>% write_rds(str_glue("{output_models}/summary.rds"))

})[[3]]



# Step 8: Export Spark Models --------------------------------------------

export_models <-
  models %>%
  enframe("title", "model") %>%
  mutate_at("title", str_replace, pattern = "pipeline_", replacement = "model_") %>%
  mutate(filepath = str_glue("{output_models}/{title}_{script_config$r_env}"))

time_model.export_models <- system.time({
  foreach(i = 1:nrow(export_models), .errorhandling = "pass") %do% {
    .model <- export_models$model[[i]]
    .path  <- export_models$filepath[[i]]
    ml_save(.model, .path, overwrite = TRUE)
  }
})[[3]]



# Step 9: Export Run Time -------------------------------------------------

export_time <-
  ls(pattern = "time_model.") %>%
  mget(inherits = TRUE) %>%
  enframe("action", "runtime") %>%
  mutate(action = action %>% str_replace("time_model.", "Model: ") %>% str_replace("_", " ") %>% str_to_title(),
         n_row = if_else(action == "Model: Fit Models", s_summary$n[[1]], na_dbl),
         timestamp = Sys.time(),
         run_env   = script_config$r_env,
         run_ram   = script_config$spark_memory) %>%
  unnest(time)

export_time %>%
  write_csv(
    str_glue("{script_config$filepath_logs}/model_{Sys.time() %>% str_remove_all('-|:| ')}.csv"))


# Step 9: Disconnect ------------------------------------------------------

spark_disconnect(sc)
