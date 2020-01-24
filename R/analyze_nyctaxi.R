
# Analyze NYC Taxi --------------------------------------------------------


# Setup -------------------------------------------------------------------

pacman::p_load(tidyverse, sparklyr, vroom, dbplot)


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

data_taxi %>% count(vendor_name)
data_taxi %>% count(rate_code)
data_taxi %>% count(Payment_Type)
data_taxi %>% count(store_and_forward)
data_taxi %>% count(Passenger_Count)


# Explore -----------------------------------------------------------------

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


# Predict -----------------------------------------------------------------


# Disconnect --------------------------------------------------------------


