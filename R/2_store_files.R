
# Store Files -------------------------------------------------------------

# Files are around 2.5gb
# Storing them as compressed parquet files
# Storing a small sample for development in the project folder

# Setup -------------------------------------------------------------------

pacman::p_load(tidyverse, fst, sparklyr, vroom, foreach, rlang, tools)
data_directory <- "C:/Users/exp01754/Downloads/r_bigdata/nyc_taxi"
data_small_dir <- "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi/data-small"


# Get List of Files -------------------------------------------------------

file_list <-
  dir(data_directory, full.names = TRUE) %>%
  map(dir, full.names = TRUE) %>%
  set_names(c("csv", "fst", "parquet")) %>%
  enframe("format", "path") %>%
  unnest(path) %>%
  mutate(title = path %>% tools::file_path_sans_ext() %>% base::basename()) %>%
  pivot_wider(names_from = format, values_from = path)


# Convert CSV to FST ------------------------------------------------------
# Not needed for anymore

# foreach(i = 1:nrow(file_list)) %do% {
#   .title <- file_list$title[[i]]
#   .path_in <- file_list$csv[[i]]
#   .path_out <- str_glue("{data_directory}/fst/{.title}.fst")
#
#   .data_big <- vroom(.path_in, delim = ",")
#   .data_big %>% write_fst(.path_out)
#   remove(.data_big)
# }


# Convert CSV to Parquet --------------------------------------------------

# Working Schema - Valid up to Jan 2009
spark_types <-
  c(vendor_name = "character",
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
    Total_Amt = "double")


sc <- spark_connect("local", version = "2.3")


time_convert <- system.time({

  loop_result <-
    foreach(i = 1:nrow(file_list), .errorhandling = "pass") %do% {
      .title <- file_list$title[[i]]
      .path_in <- file_list$csv[[i]]
      .path_out <- str_glue("{data_directory}/parquet/{.title}")

      if (file.exists(.path_out)) {
        inform(str_glue("Skipping: {.title}"))
      } else {

        # Read Big Data
        inform(str_glue("Reading: {.title}"))
        .data_big <- spark_read_csv(sc, path = .path_in, memory = TRUE, columns = spark_types, name = .title)

        # Write Small Data to CSV/Parquet
        inform(str_glue("Writing small dataset to csv and parquet in project folder..."))
        .data_small <- .data_big %>% sdf_sample(0.002, replacement = FALSE, seed = 42)
        .data_small %>% spark_write_csv(path = str_glue("{data_small_dir}/csv/{.title}"), mode = "overwrite")
        .data_small %>% spark_write_parquet(path = str_glue("{data_small_dir}/parquet/{.title}"), mode = "overwrite")

        # Write Big Data to Parquet
        inform(str_glue("Writing full dataset to parquet in downloads folder..."))
        .data_big %>% spark_write_parquet(.path_out, mode = "overwrite")

        # Remove Spark DataFrame
        .data_big %>% db_drop_table(sc, .title)
      }

      str_c("Loop Success for i = ", i)

    }

})[[3]]


# Disconnect --------------------------------------------------------------

spark_disconnect(sc)
