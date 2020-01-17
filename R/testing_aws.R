
# Testing AWS -------------------------------------------------------------

# Free tier AWS account is created on 1/13/2020
# Limited to 5gb of storage
# Monitor free tier limits at https://console.aws.amazon.com/billing/home#/

# Get the data here: https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-01.csv

# The CloudyR Package Repository
# https://cloudyr.github.io/packages/index.html

# if (!require("drat")) {install.packages("drat"); library("drat")}
# drat::addRepo("cloudyr", "http://cloudyr.github.io/drat")
# install.packages("awspack", repos = c(cloudyr = "http://cloudyr.github.io/drat", getOption("repos")))

# The PAWS Package
# install.packages("paws")

# The sagemaker package
# devtools::install_github("tmastny/sagemaker")

# A small guide: https://github.com/rikturr/icmla-aws-ml/blob/master/slides.pdf
# An AWS guide: https://aws.amazon.com/blogs/machine-learning/using-r-with-amazon-sagemaker/

library(awspack)
library(drat)
library(paws)
library(sagemaker)
pacman::p_load(tidyverse, reticulate)



# AWS Access --------------------------------------------------------------
# On exp01754 laptop

source("R/aws_access.R")

# SageMaker Package -------------------------------------------------------
# https://tmastny.github.io/sagemaker/index.html

sagemaker::sagemaker_install(pip = TRUE)
sagemaker::sagemaker_save_execution_role()

write_s3(mtcars, s3(s3_bucket(), "mtcars.csv"))

# Package - PAWS ----------------------------------------------------------
# Documentation is lacking in context.  I don't know how to get started

Sys.setenv(AWS_ACCESS_KEY_ID = key_a,
           AWS_SECRET_ACCESS_KEY = key_s,
           AWS_REGION = "us-east-2")

aws_ec2 <- paws::ec2()
# aws_ec2$
