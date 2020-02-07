
# Analyze NYC Taxi --------------------------------------------------------


# Setup -------------------------------------------------------------------

pacman::p_load(tidyverse, sparklyr, vroom, dbplot, leaflet)


# Connection --------------------------------------------------------------
# Ensure ZScaler VPN is active
sc <- spark_connect(master = "local", version = "2.3")

# Import ------------------------------------------------------------------

# Issues when reading in boolean values, i'll keep it as character for now
# map_types <-
#   c(numeric = "double",
#     logical = "character",
#     character = "character",
#     POSIXct = "timestamp",
#     POSIXt = "timestamp")
#
# metadata_taxi <-
#   vroom("data-small/yellow_tripdata_2009-01.csv") %>%
#   map(class) %>%
#   enframe() %>%
#   unnest(value) %>%
#   distinct(name, .keep_all = TRUE) %>%
#   mutate(spark_type = recode(value, !!!map_types)) %>%
#   select(name, spark_type) %>%
#   deframe()


data_taxi <-
  spark_read_csv(sc = sc,
                 path = "data-small/yellow_tripdata_2009-01.csv",
                 name = "nyctaxi_0901",
                 header = TRUE,
                 columns = c(
                   vendor_name = "character",
                   Trip_Pickup_DateTime = "timestamp",
                   Trip_Dropoff_DateTime = "timestamp",
                   Passenger_Count = "double",
                   Trip_Distance = "double",
                   Start_Lon = "double",
                   Start_Lat = "double",
                   Rate_Code = "character",
                   store_and_forward = "character",
                   End_Lon = "double",
                   End_Lat = "double",
                   Payment_Type = "character",
                   Fare_Amt = "double",
                   surcharge = "double",
                   mta_tax = "character",
                   Tip_Amt = "double",
                   Tolls_Amt = "double",
                   Total_Amt = "double"
                 ),
                 # infer_schema = TRUE,
                 delimiter = ",",
                 memory = TRUE,
                 repartition = 2,
                 overwrite = TRUE)


# Prepare -----------------------------------------------------------------

data_prep <-
  data_taxi %>%
  rename_all(str_to_lower) %>%
  filter(start_lon > -74.05, start_lon < -73.75,
         start_lat >  40.58, start_lat <  40.90,
         end_lon > -74.05, end_lon < -73.75,
         end_lat >  40.58, end_lat <  40.90,
         passenger_count > 0, passenger_count < 7,
         trip_distance > 0, trip_distance < 100,
         total_amt > 0) %>%
  mutate(pickup_hour = sql("hour(to_utc_timestamp(`trip_pickup_datetime`, 'EST'))"),
         pickup_wday = date_format(trip_pickup_datetime, "E")) %>%
  mutate_at("payment_type", str_to_lower) %>%
  compute("data_prep")



# Partitions --------------------------------------------------------------

data_split <-
  data_prep %>%
  sdf_random_split(train = 0.60,
                   valid = 0.30,
                   test = 0.10,
                   seed = 42)

data_historical <- data_split$train
data_validation <- data_split$valid
data_streaming  <- data_split$test



# Clustering --------------------------------------------------------------

model_cluster <-
  data_historical %>%
  ml_kmeans(~ start_lat + start_lon, k = 5, seed = 42, prediction_col = "taxi_hub")

model_cluster$centers %>%
  leaflet() %>%
  addProviderTiles(providers$CartoDB.Positron) %>%
  addCircleMarkers(lng = ~ start_lon,
                   lat = ~ start_lat)

data_historical_cluster_prediction <-
  model_cluster %>% ml_summary("predictions")


# Cluster Predictions -----------------------------------------------------

model_cluster %>% ml_predict(data_validation)

model_cluster %>% ml_predict(data_streaming)


# Disconnect --------------------------------------------------------------

spark_disconnect(sc)


# Testing Sections --------------------------------------------------------

# Cluster - Tests ---------------------------------------------------------


  # summarise(hour = percentile(pickup_hour, array(0.25, 0.50, 0.75))) %>%
  # mutate(hour = explode(hour))


# --- Exploration ---
# Factors upon pickup: pickup wday, hour, passenger count, startlon, startlat

# model_x <- c("start_lon", "start_lat", "pickup_wday", "pickup_hour", "passenger_count")

# data_cluster_prep <-
#   data_historical %>%
#   ft_string_indexer("pickup_wday", "wday_index") %>%
#   ft_one_hot_encoder_estimator(input_cols = c("wday_index", "pickup_hour"),
#                                output_cols = c("wday_encode", "hour_encode")) %>%
#   ft_vector_assembler(input_cols = c("start_lon", "start_lat", "passenger_count",
#                                      "hour_encode", "wday_encode"),
#                       output_col = "model_features")

# --- Bisection K Means
# Way too narrow of an area is covered here
# model_cl_bk <- data_cluster_prep %>% ml_bisecting_kmeans(~ model_features, k = 5)
# 4 is fine, 5 good, 6 nah, 7 nah
# model_cl_bk <- data_cluster_prep %>% ml_bisecting_kmeans(~ start_lat + start_lon, k = 4)
# model_cl_bk$centers %>% fx_leaflet_plot()
# model_cl_bk %>% ml_summary()
# model_cl_bk %>% ml_summary("predictions")

# --- K Means
# No good.  Too much information, clusters are too condensed
# model_cl_km <- data_cluster_prep %>% ml_kmeans(~ model_features, k = 5)

# 5 is good, 6 nah, 7 nah
# model_cl_km <-
#   data_historical %>%
#   ml_kmeans(~ start_lat + start_lon, k = 5, seed = 42, prediction_col = "taxi_hub")
#
# model_cl_km$centers %>% fx_leaflet_plot()
# model_cl_km %>% ml_summary("predictions")
#
# # --- Helper Function
# fx_leaflet_plot <- function(x){
#   as_tibble(x) %>%
#     leaflet() %>%
#     addProviderTiles(providers$CartoDB.Positron) %>%
#     addCircleMarkers(lng = ~ start_lon,
#                      lat = ~ start_lat)
# }


# COPIED IDEAS ------------------------------------------------------------

data_taxi %>% count(vendor_name)
data_taxi %>% count(rate_code)
data_taxi %>% count(Payment_Type)
data_taxi %>% count(store_and_forward)
data_taxi %>% count(Passenger_Count)

# Questions:
# What's up with pickup time?
# How does distance change with pickup time?
# What's up with number of passengers?
# Tell me about the tips, and number of passengers, and distance, and % of fare
# Shall we cluster? Based on lat/lon, distance, passenger_count, pay type

# Track current tips
# Track requests
# Track cluster outliers

# --- Rider Count
exp_ridecount <-
  data_prep %>%
  count(passenger_count) %>%
  collect() %>%
  arrange(passenger_count) %>%
  mutate_at("passenger_count", str_pad, width = 2, side = "left", pad = "0")

exp_ridecount %>% ggplot(aes(passenger_count, y = n)) + geom_col()

# In Database
# data_prep %>% dbplot::db_compute_count(x = passenger_count)
# data_prep %>% dbplot::dbplot_bar(x = passenger_count)



# --- Trip Distance
exp_distance <-
  data_prep %>%
  transmute(trip_distance = round(trip_distance, digits = 1)) %>%
  count(trip_distance) %>%
  collect() %>%
  arrange(trip_distance)

exp_distance %>% ggplot(aes(x = trip_distance, y = n)) + geom_line()



# --- Pickup Hour
# Some hive functions don't work, so i have to use the sql() version
exp_hour <-
  data_prep %>%
  count(pickup_hour) %>%
  collect() %>%
  arrange(pickup_hour)

exp_hour %>% ggplot(aes(x = pickup_hour, y = n)) + geom_line()


# --- Pickup Map
# data_prep %>%
#   collect() %>%
#   leaflet::leaflet() %>%
#   leaflet::addProviderTiles(providers$CartoDB.Positron) %>%
#   leaflet::addCircleMarkers(lng = ~start_lon, lat = ~start_lat,
#                             opacity = .01, weight = .01,
#                             fillOpacity = .1, color = "grey60")

# Copy Pasta
alpha_range <- c(0.14, 0.75)
size_range <- c(0.134, 0.173) * 4
font_family = "Calibri"
title_font_family = "Calibri"
theme_dark_map = function(base_size = 12) {
  theme_bw(base_size) +
    theme(text = element_text(family = font_family, color = "#ffffff"),
          rect = element_rect(fill = "#000000", color = "#000000"),
          plot.background = element_rect(fill = "#000000", color = "#000000"),
          panel.background = element_rect(fill = "#000000", color = "#000000"),
          plot.title = element_text(family = title_font_family),
          panel.grid = element_blank(),
          panel.border = element_blank(),
          axis.text = element_blank(),
          axis.title = element_blank(),
          axis.ticks = element_blank())
}

pt_count <-
  data_prep %>%
  dbplot::db_compute_raster(x = start_lon, y = start_lat, resolution = 1000) %>%
  rename(n = `n()`)

pt_count %>%
  ggplot(aes(x = start_lon, y = start_lat, alpha = n, size = n)) +
  geom_point(colour = "yellow") +
  theme_dark_map() +
  scale_size_continuous(range = size_range, trans = "log", limits = range(pt_count$n)) +
  scale_alpha_continuous(range = alpha_range, trans = "log", limits = range(pt_count$n)) +
  scale_fill_gradient(low = "black", high = "white") +
  coord_map() +
  theme(legend.position = "none")



# --- Heatmap
exp_heatmap <-
  data_prep %>%
  group_by(pickup_hour, pickup_wday) %>%
  summarise(n = count(),
            fpm = sum(fare_amt) / sum(trip_distance)) %>%
  collect() %>%
  complete(pickup_hour, pickup_wday, fill = list(n = 0)) %>%
  mutate_at("pickup_wday", factor, levels = c("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")) %>%
  mutate_at("pickup_hour", str_pad, width = 2, pad = "0", side = "left") %>%
  arrange(pickup_wday, pickup_hour) %>%
  print()

exp_heatmap %>%
  ggplot(aes(x = as.factor(pickup_hour), y = pickup_wday, fill = n)) +
  geom_tile() +
  scale_fill_viridis_c(label = scales::comma_format())

exp_heatmap %>%
  ggplot(aes(x = as.factor(pickup_hour), y = pickup_wday, fill = fpm)) +
  geom_tile() +
  scale_fill_viridis_c(label = scales::dollar_format())


# Analyze -----------------------------------------------------------------
# http://freerangestats.info/blog/2019/12/22/nyc-taxis-sql


# I don't like any of these
data_model <-
  data_prep %>%
  mutate(start_lon = round(start_lon, 4),
         start_lat = round(start_lat, 4)) %>%
  group_by(start_lat, start_lon) %>%
  summarise(fare_amt = mean(fare_amt),
            n = count()) %>%
  arrange(desc(n)) %>%
  compute("data_for_modeling")

model_1 <-
  data_model %>%
  ml_generalized_linear_regression(formula = fare_amt ~ start_lat + start_lon,
                                   weight_col = "n")

model_2 <-
  data_model %>%
  ml_generalized_linear_regression(formula = fare_amt ~ start_lat + start_lon,
                                   weight_col = "n",
                                   family = "poisson")

model_3 <-
  data_model %>%
  ml_generalized_linear_regression(formula = fare_amt ~ start_lat + start_lon,
                                   weight_col = "n",
                                   family = "gamma")

model_4 <-
  data_model %>%
  ml_generalized_linear_regression(formula = fare_amt ~ start_lat + start_lon,
                                   weight_col = "n",
                                   family = "tweedie")

model_5 <- ml_gradient_boosted_trees(data_prep, fare_amt ~ start_lat + start_lon)

model_6 <- ml_linear_regression(data_model, weight_col = "n", fit_intercept = FALSE,
                                formula = fare_amt ~ start_lat + start_lon)

model_7 <- ml_decision_tree(data_prep, fare_amt ~ start_lat + start_lon)


# Predict -----------------------------------------------------------------
pacman::p_load(ggmap, metR, scales, geospark)

all_lon <- seq(from = -74.05, to = -73.75, length.out = 200)
all_lat <- seq(from =  40.58, to =  40.90, length.out = 200)

predict_grid <-
  expand_grid(start_lon = all_lon, start_lat = all_lat) %>%
  copy_to(sc, ., "predict_grid", overwrite = TRUE)

predict_1 <- ml_predict(model_1, predict_grid) #ok
predict_2 <- ml_predict(model_2, predict_grid) #bad
predict_3 <- ml_predict(model_3, predict_grid) #worse
predict_4 <- ml_predict(model_4, predict_grid) #ok
predict_5 <- ml_predict(model_5, predict_grid) #not even good
predict_6 <- ml_predict(model_6, predict_grid) #not even good
predict_7 <- ml_predict(model_7, predict_grid) #not even good

d <- predict_1 %>% collect()
d <- predict_1 %>% collect() %>% filter(start_lat < 40.8)
d <- predict_5 %>% collect() %>% filter(start_lat < 40.8)
d <- predict_6 %>% collect() %>% filter(start_lat < 40.8)
d <- predict_7 %>% collect() %>% filter(start_lat < 40.8)


# --- Visualize Predictions ---

nyc_map <-
  get_stamenmap(bbox = c(-74.05, 40.58, -73.75, 40.80),
                maptype = "toner-background")

nyc_map %>%
  ggmap() +
  geom_raster(data = d,aes(x = start_lon,
                           y = start_lat,
                           fill = prediction), alpha = 0.6) +
  geom_contour(data = d, aes(x = start_lon,
                             y = start_lat,
                             z = prediction)) +
  geom_text_contour(data = d, aes(x = start_lon,
                                  y = start_lat,
                                  z = prediction),
                    colour = "white") +
  scale_fill_viridis_c(option = "A", label = dollar) +
  coord_map()

# Junk
# ldm <- data_model %>% collect()
# ggmap(nyc_map) +
#   geom_raster(data = data_model, aes(x = start_lon, y = start_lat, fill = fare_amt), alpha = 0.60) +
#   geom_contour(data = data_model, aes(x = start_lon, y = start_lat, z = fare_amt)) +
#   geom_text_contour(data = data_model, aes(x = start_lon, y = start_lat, z = fare_amt), text = "white") +
#   coord_cartesian() +
#   scale_fill_viridis_c(option = "A", label = dollar)
