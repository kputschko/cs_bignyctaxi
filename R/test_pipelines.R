
# Test MLeap --------------------------------------------------------------


mtcars_tbl <- sdf_copy_to(sc, mtcars, overwrite = TRUE)

new_mtcars <- mtcars_tbl %>% select(hp, wt, qsec, mpg)

pipeline <-
  ml_pipeline(sc) %>%
  ft_dplyr_transformer(new_mtcars) %>%
  ft_binarizer("hp", "big_hp", threshold = 100) %>%
  ft_vector_assembler(c("big_hp", "wt", "qsec"), "features") %>%
  ml_gbt_regressor(label_col = "mpg")

pipeline_model <- ml_fit(pipeline, mtcars_tbl)

transformed_tbl <- ml_transform(pipeline_model, mtcars_tbl)

model_path <- file.path(tempdir(), "mtcars_model.zip")
ml_write_bundle(pipeline_model, mtcars_tbl, model_path, overwrite = TRUE)

model <- mleap_load_bundle(model_path)
model

mleap_model_schema(model)

newdata <-
  tibble::tribble(
    ~qsec, ~hp, ~wt,
    16.2,  101, 2.68,
    18.1,  99,  3.08
  )

transformed_df <- mleap_transform(model, newdata)
dplyr::glimpse(transformed_df)



# New Pipelines -----------------------------------------------------------

demo_filters <-
  data_raw %>%
  rename_all(str_to_lower) %>%
  filter(start_lon > -74.05,  start_lon < -73.75,
         start_lat >  40.58,  start_lat <  40.90,
         end_lon > -74.05,    end_lon < -73.75,
         end_lat >  40.58,    end_lat <  40.90,
         passenger_count > 0, passenger_count < 7,
         trip_distance > 0,   trip_distance < 100,
         total_amt > 0,
         !is.na(trip_pickup_datetime))

pipeline_rawToCluster <-
  ml_pipeline(sc) %>%
  ft_dplyr_transformer(demo_filters) %>%
  ft_vector_assembler(c("start_lon", "start_lat"), "features") %>%
  ml_kmeans(prediction_col = "cluster", seed = 42, k = 5)

demo_cluster <-
  ml_fit_and_transform(pipeline_rawToCluster, data_raw)

demo_cluster <- ml_transform(pipeline_mlCluster, data_raw)


demo_features <-
  demo_cluster %>%
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
  select(cluster, start_lon, start_lat, passenger_count,
         trip_time, trip_distance, payment_type,
         fare_amt, tip_amt,
         pickup_date:pickup_nday) %>%
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
  arrange(cluster)


pipeline_everything <-
  ml_pipeline(sc) %>%
  ft_dplyr_transformer(demo_filters) %>%
  ft_vector_assembler(c("start_lon", "start_lat"), "features") %>%
  ml_kmeans(prediction_col = "cluster", seed = 42, k = 5) %>%
  ft_dplyr_transformer(demo_features) %>%
  ft_one_hot_encoder("cluster", "en_cluster") %>%
  ft_r_formula(ml_standardize_formula(response = "fpm", features = c("en_cluster","pickup_wday", "pickup_hour"))) %>%
  ml_linear_regression()

model_everything <- ml_fit(pipeline_everything, data_raw)
data_everything  <- ml_fit_and_transform(pipeline_everything, data_raw)

# Doesn't Work
# ml_write_bundle(model_everything, data_raw, file.path(tempdir(), "test_cluster.zip"), overwrite = TRUE)
