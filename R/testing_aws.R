
# Testing AWS -------------------------------------------------------------

# Free tier AWS account is created on 1/13/2020
# Limited to 5gb of storage
# Monitor free tier limits at https://console.aws.amazon.com/billing/home#/

# CSV: https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-01.csv

# The CloudyR Package Repository
# if (!require("drat")) {install.packages("drat"); library("drat")}
# drat::addRepo("cloudyr", "http://cloudyr.github.io/drat")

# The CloudyR AWS Package Group
# install.packages("awspack", repos = c(cloudyr = "http://cloudyr.github.io/drat", getOption("repos")))

# The PAWS Package
# install.packages("paws")

library(awspack)
library(drat)
library(paws)
pacman::p_load(tidyverse, reticulate)

