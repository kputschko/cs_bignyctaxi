
# Small Data --------------------------------------------------------------

pacman::p_load(tidyverse)

filepaths <- dir("raw-data", full.names = TRUE, pattern = ".csv")
data_small <- filepaths %>% map(read_csv, n_max = 10)

# data_small %>% map(spec)
data_small %>% map(colnames) %>% enframe() %>% unnest(value) %>% count(value)
# data_small[1]

filepaths_small <- str_replace(filepaths, "raw", "small")
walk2(data_small, filepaths_small, write_csv)


# Upload to SQL via Python ------------------------------------------------

pacman::p_load(reticulate)

conda_create("r-reticulate")

py_install("pip")
py_install("glob")
py_install("d6tstack")

glob <- import("glob")
ds <- import("d6tstack")
os <- import("os")
