
# Load the Data -----------------------------------------------------------
# http://freerangestats.info/blog/2019/12/22/nyc-taxis-sql


# Packages ----------------------------------------------------------------

pacman::p_load(tidyverse, foreach)

# The Data ----------------------------------------------------------------
# Download csv from NYC website
# Ensure 200gb of space

url_table <-
  tibble(url = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata",
       year = 2009:2019,
       month = 1:12 %>% str_pad(2, "left", "0") %>% list()) %>%
  unnest_longer(month) %>%
  transmute(filename = str_glue("{year}-{month}.csv") %>% as.character(),
            url = str_c(url, filename, sep = "_"),
            html = str_glue("<a href='{url}'>{filename}</a>"))



# Original ----------------------------------------------------------------

# Note that this will keep going for 10 tries of the whole cycle if it
# missed some things on the first round.

download_if_fresh <- function(fn, destfile, mode = "wb", ...){
    status <- 987
      if(!destfile %in% list.files(recursive = TRUE)){
            status <- utils::download.file(fn, destfile = destfile, mode = mode, ...)

      }
      invisible(status)
}


for(i in 1:10){

  core_url <- "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_YYYY-MM.csv"
  all_years <- 2009:2019
  all_months <- str_pad(1:12, 2, "left", pad = "0")

  for(y in all_years){
    message(paste("Downloading", y))
    for(m in all_months){
      this_url <- gsub("YYYY", y, core_url)
      this_url <- gsub("MM", m, this_url)
      filename <- str_extract(this_url, "yellow.*\\.csv$")
      destfile <- paste0("raw-data/", filename)
      try({status <- download_if_fresh(this_url, destfile = destfile)})
      if(!status %in% c(0, 987)){
        # delete any partial download that did not complete:
        unlink(destfile)
      }
    }
  }

}
