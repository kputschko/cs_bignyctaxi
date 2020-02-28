
# NYC Taxi - System Configuration -----------------------------------------

# rs_env <- "sas_server"
# rs_env <- "local_experis"
# rs_env <- "local"

# Master Configuration ----------------------------------------------------
# Can be overridden in the scripts

master_config <- list(
  local = list(
    r_env               = "local",
    spark_memory        = "5G",
    filepath_raw        = "C:/Users/exp01754/Downloads/r_bigdata/nyc_taxi/csv",
    filepath_parquet    = "C:/Users/exp01754/Downloads/r_bigdata/nyc_taxi/parquet",
    parquet_output_mode = "append",
    filepath_sample     = "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi/data-small/parquet",
    filepath_pipelines  = "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi/pipelines",
    filepath_models     = "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi/models",
    filepath_logs       = "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi/runtime_logs",
    input_pipeline      = "2020-02-26",
    input_model         = "2020-02-26",
    model_sample_fraction = 0.50
  ),
  sas_server = list(
    r_env               = "sas_server",
    spark_memory        = "5G",
    filepath_raw        = "/opt/project_area/shared/NYCTaxi_data",
    filepath_parquet    = "/opt/project_area/shared/NYCTaxi_data/parquet",
    parquet_output_mode = "append",
    filepath_sample     = "/home/kputschko/projects/cs_bignyctaxi/data-small/parquet",
    filepath_pipelines  = "/home/kputschko/projects/cs_bignyctaxi/pipelines",
    filepath_models     = "/home/kputschko/projects/cs_bignyctaxi/models",
    filepath_logs       = "/home/kputschko/projects/cs_bignyctaxi/runtime_logs",
    input_pipeline      = "2020-02-18",
    input_model         = "2020-02-18",
    model_sample_fraction = 1
  ),
  databricks = list(
    r_env               = "databricks",
    spark_memory        = "64G",
    filepath_raw        = "/opt/project_area/shared/NYCTaxi_data",
    filepath_parquet    = "/dbfs/FileStore/tables/parquet2",
    parquet_output_mode = "append",
    filepath_sample     = "/home/kputschko/projects/cs_bignyctaxi/data-small/parquet",
    filepath_pipelines  = "/home/kevin.putschko@experis.com/pipelines",
    filepath_models     = "/home/kevin.putschko@experis.com/models",
    filepath_logs       = "/home/kevin.putschko@experis.com/runtime_logs",
    input_pipeline      = "2020-02-26",
    input_model         = "2020-02-26",
    model_sample_fraction = 1
  )
)


# local_experis_old = list(
#   rs_env          = "experis_local",
#   sc_ram          = "1g",
#   input_csv       = "C:/Users/exp01754/Downloads/r_bigdata/nyc_taxi/csv",
#   output_parquet  = "C:/Users/exp01754/Downloads/r_bigdata/nyc_taxi/parquet",
#   output_sample   = "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi/data-small",
#   output_mode     = "append",           # End 2
#   input_sample    = "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi/data-small/parquet",
#   output_pipeline = "pipelines",        # End 3
#   input_parquet   = "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi/data-small/parquet",
#   input_pipeline  = "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi/pipelines",
#   output_model    = "models",           # End 4
#   input_basepath  = "C:/Users/exp01754/OneDrive/Data/cs_bignyctaxi",
#   input_model     = "models/2020-02-17"), # End 5


# Helper Functions --------------------------------------------------------

# ---- Capture Run Times
fx_runtime <- function(label, expr, ...){
  tibble(action = label,
         runtime = system.time(expr)[[3]],
         timestamp = Sys.time(),
         run_env = script_config$r_env,
         run_ram = script_config$spark_memory)
}

# fx_runtime("summary", {mtcars %>% summarise(mean(am))})



# ---- Create Directory if it Doesn't Exist
fx_dir_create <- function(filepath){
  if (!dir.exists(filepath)) {
    message("Creating Directory")
    dir.create(filepath, recursive = TRUE)
  }
}
