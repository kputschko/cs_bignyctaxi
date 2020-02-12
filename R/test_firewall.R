
# Working with Firewall Data ----------------------------------------------

# Make sure the packages are installed.  Remove # to uncomment the package
# install.packages("tidyverse")
# install.packages("sparklyr")
# install.packages("dbplot")
# install.packages("broom")

# Load the packages
# Tidyverse is a family of packages useful for data maniuplation and programming
# This set of packages includes interpreters to various SQL languages
library(tidyverse)
library(sparklyr)
library(dbplot)
library(broom)


# Spark Configuration
# I'm leaving the configuration at their default settings
my_config <- spark_config()


# Connect to Spark
# I'm not sure what this would look like on a Hadoop Cluster already running Spark
# The documentation makes me think master = "yarn-client" would work
# Check the documentation with the ?spark_connect() command
sc <- spark_connect(master = "local", version = "2.3", config = my_config)


# Load a dataset into Spark
# The path here should reflect the full path to the data lake file
# Probably beginning with 'adl://...'
data_spark <- spark_read_csv(sc, name = "my_dataset", path = "data-small/firewall.csv")


# Explore Data
# Note the %>% is a pipe operator used in tidyverse packages,
# basically %>% can be read as "then"
# So, begin process with data_spark then sdf_describe() the data, etc.
data_spark %>% sdf_describe()
data_spark %>% dbplot_bar(Action)


# Manipulate Data
# Start with data_spark, then select these columns, filter this column, and mutate to create a new column
new_data <-
  data_spark %>%
  select(Action, resultcode) %>%
  filter(Action == "Establish") %>%
  mutate(new_column = str_to_lower(Action))


# View the SQL being generated in the background
# Tidyverse include methods to translate basic R functions like select, filter, mutate to
# SQL languages.  In this case, we can see what the Spark SQL looks like.
new_data %>% show_query()


# ML Clustering
# The result is a bit messy for now
model_clusters <- ml_kmeans(data_spark, ~SourceIP + DestinationIP, k = 4)
model_summary <- model_clusters %>% tidy()


# Storing Data for Later
# If it were going to be used in the future we could save the data and model
# Sparklyr can write files to csv, parquet, orc, jdbc, delta
# ml_save(model_clusters, path = "...")
# spark_write_csv(new_data, path = ...)
# spark_write_json(...)


# Disconnect Spark
spark_disconnect(sc)
