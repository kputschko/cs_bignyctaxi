
# Small Data --------------------------------------------------------------

pacman::p_load(tidyverse)


# Create Small Data Set ---------------------------------------------------
# Used for testing purposes
# Reads 2.5gb csv from Downloads folder, converts it to 1gb fst file for fast reads later
# and finally outputs a 1.5mb file to project directoy for use in testing

# pacman::p_load(tidyverse, fst)
# data_big <- read_csv("C:/Users/exp01754/Downloads/r_bigdata/nyc_taxi/yellow_tripdata_2009-01.csv")
# write_fst(data_big, "C:/Users/exp01754/Downloads/r_bigdata/nyc_taxi/yellow_tripdata_2009-01.fst")
# write_csv(data_big %>% slice(1:10000), "data-small/yellow_tripdata_2009-01.csv")
# rm(data_big)


# Explore Full Data -------------------------------------------------------
# Reads full files from the directory specified in the 'DownThemAll' operation

# filepaths <- dir("raw-data", full.names = TRUE, pattern = ".csv")
# data_small <- filepaths %>% map(read_csv, n_max = 10)
# filepaths_small <- str_replace(filepaths, "raw", "small")
# walk2(data_small, filepaths_small, write_csv)


# Upload to SQL via Python ------------------------------------------------
# Doesn't work!

# pacman::p_load(reticulate)
# conda_create("r-reticulate")
# py_install("pip")
# py_install("glob")
# py_install("d6tstack")
# glob <- import("glob")
# ds <- import("d6tstack")
# os <- import("os")
