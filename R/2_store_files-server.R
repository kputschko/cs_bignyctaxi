
# Store Files -------------------------------------------------------------

# Files are around 2.5gb
# Storing them as compressed parquet files
# Storing a small sample for development in the project folder

# Setup -------------------------------------------------------------------

rs_env <- "varney_server"
data_directory <- "/opt/project_area/shared/NYCTaxi_data"
data_small_dir <- "/home/kputschko/projects/cs_bignyctaxi/data-small"
data_output_mode <- "append"

pacman::p_load(tidyverse, sparklyr, vroom, foreach, rlang, tools)


# Get List of Files -------------------------------------------------------

file_list <-
  dir(data_directory, full.names = TRUE, pattern = ".csv") %>%
  enframe() %>%
  mutate(title = value %>% tools::file_path_sans_ext() %>% base::basename())


# Data Schema -------------------------------------------------------------
# Working Schema - Valid up to March 2009
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


# Output Directories ------------------------------------------------------

if (data_output_mode != "append") {stop("Only supporting an appended parquet file at this time")}

path_out <- str_glue("{data_directory}/parquet")
if (!dir.exists(path_out)) dir.create(path_out)


# Spark Connection --------------------------------------------------------
sc <- spark_connect("local")


# Read and Write ----------------------------------------------------------

loop_result <-
  foreach(i = 1:nrow(file_list), .errorhandling = "pass") %do% {

    .title <- file_list$title[[i]] %>% str_remove("-")
    .path_in <- file_list$value[[i]]


    time_convert <- system.time({
      # Read Big Data
      inform(str_glue("Reading: {.title}"))
      .data_big <- spark_read_csv(sc, path = .path_in, columns = spark_types, name = .title, memory = FALSE)

      # File Size
      file_size <- file.size(.path_in) %>% scales::number_bytes(accuracy = 0.01)

      # Write Small Data to CSV/Parquet for Development
      inform(str_glue("- Writing small dataset to csv and parquet in project folder..."))
      .data_small <- .data_big %>% sdf_sample(0.002, replacement = FALSE, seed = 42) %>% sdf_persist()
      .data_small %>% spark_write_csv(path = str_glue("{data_small_dir}/csv/{.title}"), mode = "overwrite")
      .data_small %>% spark_write_parquet(path = str_glue("{data_small_dir}/parquet/{.title}"), mode = "overwrite")

      # Write Big Data to Parquet
      inform(str_glue("- Writing full dataset to parquet in downloads folder..."))
      .data_big %>% spark_write_parquet(path_out, mode = data_output_mode)
    })[[3]]

    # Remove Spark DataFrame
    # db_drop_table(sc, .title)

    # Loop Output
    tibble(action = str_glue("write_{.title}"),
           time = time_convert,
           timestamp = Sys.time(),
           environment = rs_env,
           size = file_size) %>%
      write_csv(str_glue("runtime_logs/{.title}.csv"))

  }


# Disconnect --------------------------------------------------------------

spark_disconnect(sc)
