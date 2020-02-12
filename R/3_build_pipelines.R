
# Spark - Pipelines -------------------------------------------------------
# In this script I'm going to be building pipelines for the NYC Taxi Analysis
# I'll export the pipelines for later use


# Setup -------------------------------------------------------------------
pacman::p_load(tidyverse, sparklyr, sparklyr.nested, broom)

# Connect -----------------------------------------------------------------

sc <- spark_connect(master = "local", version = "2.3")


# Step 1: Data - Raw Import -----------------------------------------------

data_1 <-
  spark_read_parquet(
    sc,
    name = "full_data",
    path = "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi/data-small/parquet/yellow_tripdata_2009-01")


# Step 2: Pipeline - Prepare Data -----------------------------------------

pipeline_sql_rawToCluster <-
  data_1 %>%
  rename_all(str_to_lower) %>%
  mutate_at("payment_type", str_to_lower) %>%
  filter(start_lon > -74.05,  start_lon < -73.75,
         start_lat >  40.58,  start_lat <  40.90,
         end_lon > -74.05,    end_lon < -73.75,
         end_lat >  40.58,    end_lat <  40.90,
         passenger_count > 0, passenger_count < 7,
         trip_distance > 0,   trip_distance < 100,
         total_amt > 0,
         !is.na(trip_pickup_datetime)) %>%
  mutate(trip_time = unix_timestamp(trip_dropoff_datetime) - unix_timestamp(trip_pickup_datetime)) %>%
  mutate(trip_pickup_datetime = to_utc_timestamp(trip_pickup_datetime, "UTC")) %>%
  mutate(pickup_date = date_format(trip_pickup_datetime, "YYYY-MM-DD"),
         pickup_year = date_format(trip_pickup_datetime, "YYYY"),
         pickup_mon  = date_format(trip_pickup_datetime, "MM"),
         pickup_day  = date_format(trip_pickup_datetime, "DD"),
         pickup_hour = sql("hour(`trip_pickup_datetime`)"),
         pickup_wday = date_format(trip_pickup_datetime, "E"),
         pickup_nday = dayofweek(trip_pickup_datetime)) %>%
  select(start_lon, start_lat, passenger_count,
         trip_time, trip_distance, payment_type,
         fare_amt, tip_amt,
         pickup_date:pickup_nday) %>%
  ft_dplyr_transformer(sc, tbl = .)


# Step 3: Data - Apply Preparations ---------------------------------------

data_2 <- ml_transform(pipeline_sql_rawToCluster, data_1)


# Step 4: Pipeline - Cluster ----------------------------------------------

pipeline_model_cluster <-
  ml_pipeline(sc) %>%
  ft_r_formula(~ start_lat + start_lon) %>%
  ml_kmeans(seed = 42, k = 5, prediction_col = "cluster")


# Step 5: Model - Clusters ------------------------------------------------

model_cluster <- ml_fit(pipeline_model_cluster, data_2)


# Step 6: Data - Apply Clusters -------------------------------------------

data_3 <- ml_transform(model_cluster, data_2)


# Step 7: Pipeline - Prepare for Prediction Models ------------------------

pipeline_sql_clusterToML <-
  data_3 %>%
  group_by(cluster, pickup_mon, pickup_wday, pickup_hour) %>%
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
  arrange(cluster) %>%
  ft_dplyr_transformer(sc, tbl = .)


# Step 8: Data - Apply SQL Preparations for Prediction --------------------

data_4 <- ml_transform(pipeline_sql_clusterToML, data_3)


# Step 9: Pipeline - Prediction -------------------------------------------

fx_predict_pipeline <- function(response, features, sc = sc){

  .formula <- ml_standardize_formula(response = response, features = features)
  .p_out   <- str_c("p_", response)

  ml_pipeline(sc) %>%
    ft_one_hot_encoder("cluster", "en_cluster") %>%
    ft_r_formula(.formula) %>%
    ml_linear_regression(prediction_col = .p_out)
}

model_x <- c("en_cluster","pickup_wday", "pickup_hour")
model_y <- c("rides", "ppr", "dpr", "fpr", "tpr", "fpm", "tpm")

pipeline_model_ml <-
  model_y %>%
  map(fx_predict_pipeline, sc = sc, features = model_x) %>%
  set_names(model_y)


# Step 10: Model - Predictions --------------------------------------------
# I don't actually want to build models here, I'll do this in next script
# model_prediction <- pipeline_model_ml %>% modify(ml_fit, data_4)


# Step 11: Pipeline - Export ----------------------------------------------

export_1 <-
  pipeline_model_ml %>%
  enframe("filepath", "pipeline") %>%
  mutate(filepath = str_c("pipelines/model_ml_", filepath))

export_2 <-
  c("pipeline_model_cluster",
    "pipeline_sql_clusterToML",
    "pipeline_sql_rawToCluster") %>%
  mget(inherits = TRUE) %>%
  enframe("filepath", "pipeline") %>%
  mutate(filepath = str_replace(filepath, "pipeline_", "pipelines/"))

export_3 <- bind_rows(export_1, export_2)

walk2(export_3$pipeline, export_3$filepath,
      ml_save, overwrite = TRUE, type = "pipeline")


# Disconnect --------------------------------------------------------------

spark_disconnect(sc)
