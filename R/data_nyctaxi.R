
# Load NYC Taxi Data ------------------------------------------------------
# Create Small Data Set
# Used for testing purposes
# Reads 2.5gb csv from Downloads folder, converts it to 1gb fst file for fast reads later
# and finally outputs a 1.5mb file to project directoy for use in testing

pacman::p_load(tidyverse, fst)
data_big <- read_csv("C:/Users/exp01754/Downloads/r_bigdata/nyc_taxi/yellow_tripdata_2009-01.csv")
write_fst(data_big, "C:/Users/exp01754/Downloads/r_bigdata/nyc_taxi/yellow_tripdata_2009-01.fst")
write_csv(data_big %>% sample_n(10000), "data-small/yellow_tripdata_2009-01.csv")
rm(data_big)

