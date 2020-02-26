
# Store Files -------------------------------------------------------------

# Files are around 2.5gb
# Storing them as compressed parquet files
# Storing a small sample for development in the project folder

# User Info ---------------------------------------------------------------

r_env <- "local"


# R Setup -----------------------------------------------------------------
pacman::p_load(tidyverse, sparklyr, foreach, rlang, tools, here)


source("R/0_config.R")
script_config <- master_config %>% pluck(r_env)


# Connect to Spark --------------------------------------------------------

sc_config <- spark_config()
sc_config$`sparklyr.shell.driver-memory`   <- script_config$spark_memory
sc_config$`sparklyr.shell.executor-memory` <- script_config$spark_memory
sc <- spark_connect("local", config = sc_config)


# Get List of Files -------------------------------------------------------

file_paths <-
  dir(script_config$filepath_raw, full.names = TRUE, pattern = ".csv") %>%
  enframe("index", "input") %>%
  mutate(title = input %>% file_path_sans_ext() %>% basename() %>% str_remove("-"),
         output =
           case_when(script_config$parquet_output_mode == "append"    ~ script_config$filepath_parquet,
                     script_config$parquet_output_mode == "overwrite" ~ str_glue('{script_config$filepath_parquet}/{title}') %>% as.character(),
                     TRUE ~ na_chr),
         output_small = str_replace(output, script_config$filepath_parquet, script_config$filepath_sample)) %>%
  select(index, title, input, output, output_small)



# Convert CSV to FST ------------------------------------------------------
# Not needed for anymore

# pacman::p_load(fst, vroom)

# file_list <-
#   dir(data_directory, full.names = TRUE) %>%
#   map(dir, full.names = TRUE) %>%
#   set_names(c("csv", "fst", "parquet")) %>%
#   enframe("format", "path") %>%
#   unnest(path) %>%
#   mutate(title = path %>% tools::file_path_sans_ext() %>% base::basename()) %>%
#   pivot_wider(names_from = format, values_from = path)

# foreach(i = 1:nrow(file_list)) %do% {
#   .title <- file_list$title[[i]]
#   .path_in <- file_list$csv[[i]]
#   .path_out <- str_glue("{data_directory}/fst/{.title}.fst")
#
#   .data_big <- vroom(.path_in, delim = ",")
#   .data_big %>% write_fst(.path_out)
#   remove(.data_big)
# }


# Data Schema -------------------------------------------------------------
# Valid up to March 2009
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


# Read and Write ----------------------------------------------------------

loop_read_write <-
  foreach(i = 1:nrow(file_paths), .errorhandling = "pass") %do% {
    .title          <- file_paths$title[[i]]
    .path_in        <- file_paths$input[[i]]
    .path_out       <- file_paths$output[[i]]
    .path_out_small <- file_paths$output_small[[i]]

    # Raw Data Size
    .data_size <- file.size(.path_in)

    # Create Directory
    if (!dir.exists(.path_out)) dir.create(.path_out)

    time_read_write <- fx_runtime(str_glue("Write: {.title}"), {
      # Read Big Data
      inform(str_glue("Reading: {.title}"))
      .data_big <- spark_read_csv(sc, path = .path_in, memory = FALSE, columns = spark_types, name = .title)

      # Write Big Data to Parquet
      inform(str_glue("- Writing full dataset to parquet in downloads folder..."))
      .data_big %>% spark_write_parquet(.path_out, mode = script_config$parquet_output_mode)
    })

    # Logs
    log_loop <-
      time_read_write %>%
      add_column(filesize = .data_size)

    # tibble(action    = str_glue("Write: {.title}"),
    #          filesize  = .data_size,
    #          n_row    = NA,
    #          runtime   = time_read_write,
    #          timestamp = Sys.time(),
    #          run_env   = script_config$r_env,
    #          run_ram   = script_config$spark_memory)

    inform("- Writing logs...")
    write_csv(log_loop, str_glue("runtime_logs/write_parquet_{Sys.time() %>% stringr::str_remove_all('-|:| ')}.csv"))

  }



# Small Data for Development ----------------------------------------------

dev_filepaths <-
  file_paths %>%
  slice(1)

time_dev_sample <- fx_runtime("Write: Development Sample", {
  dev_input <- dev_filepaths %>% pull(output) %>% spark_read_parquet(sc, path = ., memory = FALSE)
  dev_nrow <- dev_input %>% sdf_nrow()

  dev_output <-
    dev_input %>%
    sdf_sample(0.01, replacement = FALSE, seed = 42) %>%
    sample_n(size = min(30000, dev_nrow)) %>%
    sdf_persist()

  dev_output %>% spark_write_parquet(path = file.path(dev_filepaths$output_small, "parquet"), mode = "overwrite")
  dev_output %>% spark_write_csv(path = file.path(dev_filepaths$output_small, "csv"), mode = "overwrite")
})

time_dev_sample %>%
  add_column(n_row = sdf_nrow(dev_output)) %>%
  write_csv(str_glue("runtime_logs/write_sample_{Sys.time() %>% stringr::str_remove_all('-|:| ')}.csv"))


# tibble(action    = "Write: Development Sample",
#        runtime   = time_dev_sample,
#        timestamp = Sys.time(),
#        n_row     = sdf_nrow(dev_output),
#        run_env   = script_config$r_env,
#        run_ram   = script_config$spark_memory) %>%
#   write_csv(str_glue("runtime_logs/write_sample_{Sys.time() %>% stringr::str_remove_all('-|:| ')}.csv"))



# Disconnect --------------------------------------------------------------

spark_disconnect(sc)
