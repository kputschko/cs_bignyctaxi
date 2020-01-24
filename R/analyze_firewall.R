
# Analyze Firewall Data ---------------------------------------------------


# Setup -------------------------------------------------------------------

pacman::p_load(tidyverse, sparklyr, broom, vroom)


# Connection --------------------------------------------------------------

# Start ZScaler VPN Service Prior to Connection
spark_connection <- spark_connect("local", version = "2.3")


# Data Schema -------------------------------------------------------------
# Get Data Schema, copy/paste with dput()

# metadata_firewall <-
#   vroom("data-small/firewall.csv", delim = ",") %>%
#   map(class) %>%
#   enframe() %>%
#   unnest(value) %>%
#   mutate(spark_value = recode(value,
#                               numeric = "double",
#                               logical = "boolean",
#                               character = "character",
#                               POSIXct = "timestamp",
#                               POSIXt = "timestamp")) %>%
#   select(name, spark_value) %>%
#   deframe()

# metadata_firewall <-
#   c(ident = "double", servername = "character", logTime = "timestamp",
#     protocol = "character", SourceIP = "character",
#     SourceIP_int8 = "boolean", SourcePort = "boolean", SourceIP_LocID = "boolean",
#     DestinationIP = "character", DestinationIP_int8 = "boolean",
#     DestinationPort = "boolean", DestinationIP_LocID = "boolean",
#     OriginalClientIP = "character", OriginalClientIP_int8 = "boolean",
#     OriginalClientIP_LocID = "boolean", SourceNetwork = "character",
#     DestinationNetwork = "character", Action = "character", resultcode = "character",
#     rule = "character", ApplicationProtocol = "character", bytessent = "double",
#     bytesrecvd = "double", connectiontime = "boolean", DestinationName = "boolean",
#     ClientUserName = "character", ClientAgent = "character", sessionid = "double",
#     connectionid = "double", Interface = "boolean", IPHeader = "boolean",
#     Payload = "boolean", GmtLogTime = "boolean", ipsScanResult = "boolean",
#     ipsSignature = "boolean", NATAddress = "boolean", FwcClientFqdn = "boolean",
#     FwcAppPath = "boolean", FwcAppSHA1Hash = "boolean", FwcAppTrusState = "boolean",
#     FwcAppInternalName = "boolean", FwcAppProductName = "boolean",
#     FwcAppProductVersion = "boolean", FwcAppFileVersion = "boolean",
#     FwcAppOrgFileName = "boolean", InternalServiceInfo = "boolean",
#     ipsApplicationProtocol = "boolean", FwcVersion = "boolean")


# Import ------------------------------------------------------------------

data_firewall <-
  spark_read_csv(spark_connection,
                 name = "firewall",
                 path = "data-small/firewall.csv",
                 infer_schema = FALSE,
                 # columns = metadata_firewall, # This doesn't work?
                 memory = TRUE,
                 repartition = 0,
                 overwrite = TRUE)


# Prepare -----------------------------------------------------------------

fx_to_formula <- function(y, x) {
  library(stringr)
  str_c(y, str_c(x, collapse = " + "), sep = " ~ ") %>% as.formula()
}

model_y <- "Action"
model_x <- c("ApplicationProtocol", "DestinationIP", "OriginalClientIP", "rule", "SourceIP")
model_f <- fx_to_formula(model_y, model_x)

data_prep <-
  data_firewall %>%
  select(one_of(model_y, model_x)) %>%
  compute("table_prep")

# Model -------------------------------------------------------------------

model_rf <- data_firewall %>% ml_random_forest(model_f)

model_rf %>% glance()
model_rf %>% tidy()

# Predict -----------------------------------------------------------------

ml_predict(model_rf, data_firewall %>% sample_n(1)) %>%
  select(one_of(model_y, model_x), prediction:probability_Failed) %>%
  collect() %>%
  view()


# Pipeline ----------------------------------------------------------------
# This isn't a good use case right now, need more furtive data

# spark_connection %>% ml_pipeline()

# Disconnect --------------------------------------------------------------

spark_disconnect(spark_connection)
