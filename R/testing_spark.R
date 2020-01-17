
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

# Import Data
data <- read_csv("data-small/yellow_tripdata_2009-01.csv")
sdata <- spark_read_csv(sc, path = "data-small/yellow_tripdata_2009-01.csv")

# Web Interface
spark_web(sc)


# Basic Analysis ----------------------------------------------------------

# --- Some Basics
data %>% ggplot(aes(x = Tip_Amt)) + geom_density()

sdata %>% select(Tip_Amt) %>% sample_n(500) %>% collect() %>%
  ggplot(aes(x = Tip_Amt)) + geom_density()

sdata %>% count(Passenger_Count) %>% collect() %>%
  ggplot(aes(x = str_pad(Passenger_Count, 3, pad = "0"), y = n)) + geom_col() +
  scale_y_log10(label = scales::comma) + scale_x_discrete(labels = sort(unique(.data$Passenger_Count)))

sdata %>% transmute(td = floor(Trip_Distance)) %>% count(td) %>% collect() %>%
  ggplot(aes(x = td, y = n)) + geom_line()


# --- View Queries with 'show_query()'
sdata %>%
  group_by(Payment_Type) %>%
  summarise(mean_total = mean(Total_Amt),
            mean_tip = mean(Tip_Amt)) %>%
  show_query()


# --- Hive Functions
# When a R/dplyr function isn't translated to a sparklyr function, use HIVE functions
# 'percentile' and 'array' aren't R or dplyr function, but are recognized HIVE functions

# All Hive Functions: https://therinspark.com/appendix.html#hive-functions

sdata %>% summarise(p = percentile(Total_Amt, array(0.25, 0.50, 0.75)))
sdata %>% summarise(p = percentile(Total_Amt, array(0.25, 0.50, 0.75))) %>% mutate(p = explode(p))

# --- Correlations
# 'ml_' functions run in the Spark backend
# The 'corrr' package runs in the Spark backend as well

sdata %>% select(Trip_Distance, Total_Amt) %>% ml_corr()
sdata %>% select(Trip_Distance, Total_Amt) %>% corrr::correlate() %>% corrr::shave()


# --- Plots
# We have to either summarise data in Spark and bring down a smaller df to plot
# Or we can use the 'dbplot' package to get some basic plots in Spark,
# The dbplot package works a lot like ggplot2
pacman::p_load(dbplot)

sdata %>% dbplot_histogram(Total_Amt) + labs(title = "A Title")
sdata %>% db_compute_bins(Total_Amt)

sdata %>% dbplot_raster(Total_Amt, Trip_Distance, resolution = 10)
sdata %>% db_compute_raster(Total_Amt, Trip_Distance, resolution = 10)

sdata %>% dbplot_raster(Total_Amt, Trip_Distance, resolution = 100)
sdata %>% db_compute_raster(Total_Amt, Trip_Distance, resolution = 100)



# Modeling ----------------------------------------------------------------

# --- a pseudo process
# sdf_random_split(..., training = 0.80, testing = 0.20, seed = 42)
# sdf_describe()
# dbplot_histogram()
# sdf_crosstab()

# --- Standardize Data
# note all this should be based on the analysis/train set
# meaning we would scale holdout set based on train set rather than based on its own data
# ft_normalizer()
# ft_standard_scaler(..., with_mean = TRUE, with_std = TRUE) - center around mean, scale w/ sd
# ft_one_hot_encoder() for dummy vars
# ft_string_indexer() to convert character cols to numeric

# When modeling in Spark after a series of transformations to the data,
# we should cache the data prior to modeling to get a 'checkpoint' on the data
# along with perhaps saving the data to disk as a parquet file
sdata_prep <- sdata %>% mutate(abc = 123) %>% compute("transformed_data")
# spark_write_parquet()

# --- Modeling
# text mining, neural networks, clusters, etc.
smodel_gb <- sdata_prep %>% ml_gradient_boosted_trees(Tip_Amt ~ Start_Lat + Start_Lon)
smodel_lm <- sdata_prep %>% ml_linear_regression(Tip_Amt ~ Start_Lat + Start_Lon)
smodel_gm <- sdata_prep %>% ml_generalized_linear_regression(Tip_Amt ~ Start_Lat + Start_Lon)

smodel_gb %>% summary()
smodel_lm %>% summary()
smodel_gm %>% summary()

# --- for glm type models (linear, logistic, generalized, etc.)
# ml_evaluate() to get assessment measurements on holdout data set
# inside ml_eval will be other functions for assessment like 'roc()' and 'area_under_roc()'

# --- other evaluators
# ml_regression_evaluator()
# ml_classification_eval()
# ml_clustering_evaluator()
# ml_binary_classification_evaluator()

# --- cross validation will be a manual process (https://therinspark.com/modeling.html#supervised-learning)

# --- other models can be set
# ml_generalized_linear_regression(..., family = "binomial")

# --- tidy results
# sparklyr::tidy()



# Prediction --------------------------------------------------------------

data %>%
  filter(Tip_Amt > 0) %>%
  sample_n(1) %>%
  select(Start_Lat, Start_Lon, act_Tip_Amt = Tip_Amt) %>%
  copy_to(sc, ., name = "new_data", overwrite = TRUE) %>%
  ml_predict(smodel, .) %>%
  rename(est_Tip_Amt = prediction)


# Pipeline ----------------------------------------------------------------
# A modeling pipeline will automate the tasks of data prep on new data
# This is useful when fine tuning models or doing cross validation

# Standard Flow
sdata %>%
  select(Tip_Amt, Trip_Distance, Total_Amt) %>%
  ft_vector_assembler(input_cols = "Trip_Distance", "v_d") %>%
  ft_standard_scaler(input_col = "v_d", output_col = "s_d", with_mean = TRUE, with_std = TRUE) %>%
  glimpse()

# Option 2: Define a transformation function
# ??? This doesn't make sense
fx_vec   <- ft_vector_assembler(sc, input_cols = "Trip_Distance", "v_d")
fx_scale <- ft_standard_scaler(sc, input_col = "v_d", output_col = "s_d", with_mean = TRUE, with_std = TRUE)

step_1 <- ml_transform(fx_vec, sdata)
step_2 <- ml_fit(fx_scale, step_1)
step_3 <- ml_transform(step_2, step_1)

# Option 3: Define a Pipeline
s_pipeline <-
  ml_pipeline(sc) %>%
  ft_vector_assembler(input_cols = "Trip_Distance", "v_d") %>%
  ft_standard_scaler(input_col = "v_d", output_col = "s_d", with_mean = TRUE, with_std = TRUE) %>%
  ft_vector_assembler(input_cols = c("Start_Lat", "Start_Lon"), output_col = "use_these") %>%
  ml_linear_regression(features_col = "use_these",
                       label_col = "Tip_Amt")



# Using a Pipeline --------------------------------------------------------
# In cross validation, for example

# cv <-
#   ml_cross_validator(sc,
#                      estimator = pipeline,
#                      estimator_param_maps = list(
#                        standard_scaler = list(with_mean = c(TRUE, FALSE)),
#                        logistic_regression = list(
#                          elastic_net_param = c(0.25, 0.75),
#                          reg_param = c(1e-2, 1e-3))),
#                      evaluator = ml_binary_classification_evaluator(sc, label_col = "not_working"),
#                      num_folds = 10)
#
# cv_model <- ml_fit(cv, data)
#
# ml_validation_metrics(cv_model) %>% arrange(-areaUnderROC)


# Save a Spark ML Pipeline to be used by other Spark instances in other languages (py, scala, java, etc.)
# ml_save(cv_model$best_model, "filepath", overwrite = TRUE)


# Inspect the model at a partiular stage in the pipeline
# model_reload <- ml_load(sc, "filepath")
# ml_stage(model_reload, "logistic_regression")

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


# Logs --------------------------------------------------------------------

spark_log(sc)
spark_log(sc, filter = "sparklyr")

# Disconnect --------------------------------------------------------------

spark_disconnect(sc)
spark_disconnect_all()  # When multiple connections are available
