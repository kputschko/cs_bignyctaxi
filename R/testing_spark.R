
# Testing Spark -----------------------------------------------------------

# Links to Use ------------------------------------------------------------
# A Book: https://therinspark.com/starting.html
# Documentation: https://spark.rstudio.com/guides/connections/

# AWS: https://spark.rstudio.com/guides/aws-s3/
# Streaming: https://spark.rstudio.com/guides/streaming/


# Packages ----------------------------------------------------------------
# The nested package is for working with nested dfs and json files

pacman::p_load(tidyverse, sparklyr, sparklyr.nested)

# Initial Setup -----------------------------------------------------------

# Needs Java 1.8
system("java -version")

# Sparklyr Version
packageVersion("sparklyr")

# Install Spark
spark_install()       # Install the latest version
spark_install("2.3")  # Install the version the book uses

spark_available_versions()
spark_installed_versions()

# Setup -------------------------------------------------------------------
# Connect to Spark
sc <- spark_connect(master = "local", version = "2.3", log = "console")

# Copy Data
data <- read_csv("data-small/yellow_tripdata_2009-01.csv")
sdata <- spark_read_csv(sc, path = "data-small/yellow_tripdata_2009-01.csv")

# Web Interface
spark_web(sc)


# Basic Analysis ----------------------------------------------------------

data %>%
  ggplot(aes(x = Tip_Amt)) +
  geom_density()

sdata %>%
  select(Tip_Amt) %>%
  sample_n(500) %>%
  collect() %>%
  ggplot(aes(x = Tip_Amt)) +
  geom_density()

sdata %>%
  count(Passenger_Count) %>%
  collect() %>%
  ggplot(aes(x = str_pad(Passenger_Count, 3, pad = "0"), y = n)) +
  geom_col() +
  scale_y_log10(label = scales::comma) +
  scale_x_discrete(labels = sort(unique(.data$Passenger_Count))) +
  labs(x = "Number of passengers (discrete scale - only values with at least one trip shown)",
       y = "Number of trips (log scale)",
       title = "Number of passengers per trip",
       subtitle = "New York City yellow cabs January 2009 to mid 2016")

sdata %>%
  transmute(td = floor(Trip_Distance)) %>%
  count(td) %>%
  collect() %>%
  ggplot(aes(x = td, y = n)) +
  geom_line()


# Modeling ----------------------------------------------------------------

smodel <-
  sdata %>%
  ml_gradient_boosted_trees(Tip_Amt ~ Start_Lat + Start_Lon)


# Prediction --------------------------------------------------------------

data %>%
  filter(Tip_Amt > 0) %>%
  sample_n(1) %>%
  select(Start_Lat, Start_Lon, act_Tip_Amt = Tip_Amt) %>%
  copy_to(sc, ., name = "new_data", overwrite = TRUE) %>%
  ml_predict(smodel, .) %>%
  rename(est_Tip_Amt = prediction)


# Stream ------------------------------------------------------------------
# Specify directory that will be continuously populated with new data

dir("data-small/stream_input", full.names = TRUE) %>% file.remove()
dir("data-small/stream_output", full.names = TRUE) %>% file.remove()

data %>% sample_n(1) %>% write_csv("data-small/stream_input/new_data_1.csv")

stream <-
  stream_read_csv(sc, "data-small/stream_input") %>%
  select(Tip_Amt, Start_Lat, Start_Lon) %>%
  stream_write_csv("data-small/stream_output")

data %>% sample_n(1) %>% write_csv("data-small/stream_input/new_data_2.csv")
data %>% sample_n(1) %>% write_csv("data-small/stream_input/new_data_3.csv")

dir("data-small/stream_output", pattern = ".csv")

stream_stop(stream)


# Disconnect --------------------------------------------------------------

spark_disconnect(sc)
