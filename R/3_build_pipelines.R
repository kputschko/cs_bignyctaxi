
# Spark - Pipelines -------------------------------------------------------
# In this script I'm going to be building pipelines for the NYC Taxi Analysis
# I'll export the pipelines for later use

# User Info ---------------------------------------------------------------

r_env <- "local"

# R Setup -----------------------------------------------------------------
source("R/0_config.R")
pacman::p_load(tidyverse, sparklyr)
script_config <- master_config %>% pluck(r_env)


# Connect to Spark --------------------------------------------------------

sc_config <- spark_config()
sc_config$spark.driver.memory <- script_config$spark_memory
sc <- spark_connect("local", config = sc_config)


# Begin Pipeline Creation -------------------------------------------------

time_pipelines <- system.time({

  # Import Data for Pipeline  -----------------------------------------------
  data_1 <- spark_read_parquet(sc, path = script_config$filepath_sample)

  # Construct SQL Filters ---------------------------------------------------

  transform_1 <-
    data_1 %>%
    rename_all(str_to_lower) %>%
    filter(start_lon > -74.05,  start_lon < -73.75,
           start_lat >  40.58,  start_lat <  40.90,
           end_lon > -74.05,    end_lon < -73.75,
           end_lat >  40.58,    end_lat <  40.90,
           passenger_count > 0, passenger_count < 7,
           trip_distance > 0,   trip_distance < 100,
           total_amt > 0,
           !is.na(trip_pickup_datetime))


  # Construct Cluster Stage -------------------------------------------------

  pipeline_1 <-
    ml_pipeline(sc) %>%
    ft_dplyr_transformer(transform_1) %>%
    ft_r_formula(~ start_lat + start_lon) %>%
    ml_kmeans(seed = 42, k = 5, prediction_col = "cluster")


  # Construct SQL Feature Engineering ---------------------------------------

  transform_2 <-
    ml_fit_and_transform(pipeline_1, data_1) %>%
    mutate_at("payment_type", str_to_lower) %>%
    mutate(trip_time = unix_timestamp(trip_dropoff_datetime) - unix_timestamp(trip_pickup_datetime)) %>%
    mutate(trip_pickup_datetime = to_utc_timestamp(trip_pickup_datetime, "UTC")) %>%
    mutate(pickup_date = date_format(trip_pickup_datetime, "YYYY-MM-DD"),
           pickup_year = date_format(trip_pickup_datetime, "YYYY"),
           pickup_mon  = date_format(trip_pickup_datetime, "MM"),
           pickup_day  = date_format(trip_pickup_datetime, "DD"),
           pickup_hour = sql("hour(`trip_pickup_datetime`)"),
           pickup_wday = date_format(trip_pickup_datetime, "E"),
           pickup_nday = dayofweek(trip_pickup_datetime)) %>%
    select(cluster, start_lon, start_lat,
           passenger_count, trip_time, trip_distance,
           payment_type, fare_amt, tip_amt,
           pickup_date:pickup_nday) %>%
    group_by(cluster, pickup_year, pickup_mon, pickup_wday, pickup_nday, pickup_hour) %>%
    summarise(rides = count(),
              passengers = sum(passenger_count, na.rm = TRUE),
              distance = sum(trip_distance, na.rm = TRUE),
              fare = sum(fare_amt, na.rm = TRUE),
              tip = sum(tip_amt, na.rm = TRUE)) %>%
    mutate(ppr = passengers / rides,
           dpr = distance / rides,
           fpr = fare / rides,
           tpr = tip / rides,
           fpm = fare / distance,
           tpm = tip / distance) %>%
    arrange(cluster)


  # Create Function for Pipeline Creation -----------------------------------
  # The function helps create separate pipelines for a given response

  fx_spark_pipeline_cluster_lr <- function(response, features, sc = sc){

    .formula <- ml_standardize_formula(response = response, features = features)
    .p_out   <- str_c("p_", response)

    ml_pipeline(sc) %>%
      ft_dplyr_transformer(transform_1) %>%
      ft_r_formula(~ start_lat + start_lon) %>%
      ml_kmeans(seed = 42, k = 5, prediction_col = "cluster") %>%
      ft_dplyr_transformer(transform_2) %>%
      ft_one_hot_encoder("cluster", "en_cluster") %>%
      ft_r_formula(.formula) %>%
      ml_linear_regression(prediction_col = .p_out)

  }


  # Construct Full Pipeline -------------------------------------------------
  # model_y <- c("rides", "ppr", "dpr", "fpr", "tpr", "fpm", "tpm")

  model_y <- c("rides", "fpm")
  model_x <- c("en_cluster","pickup_wday", "pickup_hour")

  pipeline_prediction <-
    model_y %>%
    map(fx_spark_pipeline_cluster_lr, sc = sc, features = model_x) %>%
    set_names(model_y)

})[[3]]

# Export Pipelines --------------------------------------------------------

output_pipelines <- file.path(script_config$filepath_pipelines, Sys.Date())
output_pipelines %>% fx_dir_create()

export_1 <-
  pipeline_prediction %>%
  enframe("filepath", "pipeline") %>%
  mutate(filepath = str_glue("{output_pipelines}/pipeline_{filepath}"))


time_export_pipelines <- system.time({
  walk2(export_1$pipeline, export_1$filepath, ml_save, overwrite = TRUE)
})[[3]]



# Export Runtime ----------------------------------------------------------

log_time <-
  tibble(action    = "Pipeline: Build",
         n_row     = sdf_nrow(data_1),
         runtime   = time_pipelines,
         timestamp = Sys.time(),
         run_env   = script_config$r_env,
         run_ram   = script_config$spark_memory)


write_csv(log_time, str_glue("{script_config$filepath_logs}/pipeline_{Sys.time() %>% str_remove_all('-|:| ')}.csv"))


# Disconnect --------------------------------------------------------------

spark_disconnect(sc)
