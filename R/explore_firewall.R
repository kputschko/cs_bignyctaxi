
# Analyzing Firewall Data -------------------------------------------------

pacman::p_load(tidyverse, vroom)


# Import ------------------------------------------------------------------

metadata_firewall <- cols(
  ident = col_double(),
  servername = col_character(),
  logTime = col_datetime(format = ""),
  protocol = col_character(),
  SourceIP = col_character(),
  SourceIP_int8 = col_logical(),
  SourcePort = col_logical(),
  SourceIP_LocID = col_logical(),
  DestinationIP = col_character(),
  DestinationIP_int8 = col_logical(),
  DestinationPort = col_logical(),
  DestinationIP_LocID = col_logical(),
  OriginalClientIP = col_character(),
  OriginalClientIP_int8 = col_logical(),
  OriginalClientIP_LocID = col_logical(),
  SourceNetwork = col_character(),
  DestinationNetwork = col_character(),
  Action = col_character(),
  resultcode = col_character(),
  rule = col_character(),
  ApplicationProtocol = col_character(),
  bytessent = col_double(),
  bytesrecvd = col_double(),
  connectiontime = col_logical(),
  DestinationName = col_logical(),
  ClientUserName = col_character(),
  ClientAgent = col_character(),
  sessionid = col_double(),
  connectionid = col_double(),
  Interface = col_logical(),
  IPHeader = col_logical(),
  Payload = col_logical(),
  GmtLogTime = col_logical(),
  ipsScanResult = col_logical(),
  ipsSignature = col_logical(),
  NATAddress = col_logical(),
  FwcClientFqdn = col_logical(),
  FwcAppPath = col_logical(),
  FwcAppSHA1Hash = col_logical(),
  FwcAppTrusState = col_logical(),
  FwcAppInternalName = col_logical(),
  FwcAppProductName = col_logical(),
  FwcAppProductVersion = col_logical(),
  FwcAppFileVersion = col_logical(),
  FwcAppOrgFileName = col_logical(),
  InternalServiceInfo = col_logical(),
  ipsApplicationProtocol = col_logical(),
  FwcVersion = col_logical()
)

data_firewall <- vroom("data-small/firewall.csv", delim = ",", col_types = metadata_firewall)

# Descriptives ------------------------------------------------------------

data_firewall %>% glimpse()
data_firewall %>% Hmisc::describe()

data_firewall %>% janitor::tabyl(Action)
data_firewall %>% count(SourceIP, Action, sort = TRUE)

data_firewall %>% filter(Action == "Denied")

library(rpart)
library(caret)
library(janitor)
library(broom)


model_data <-
  data_firewall %>%
  mutate(aid = ifelse(Action == "Denied", 1, 0)) %>%
  select(-Action, -connectionid, -sessionid, -resultcode)

model <- rpart(aid ~ ., data = model_data)

model %>% broom::glance()
varimp <- model %>% varImp() %>% as_tibble(rownames = "col") %>% filter(Overall > 0) %>% print()

data_firewall %>% count(ApplicationProtocol)
data_firewall %>% count(DestinationIP)
data_firewall %>% count(OriginalClientIP)
data_firewall %>% count(rule)
data_firewall %>% count(SourceIP, sort = TRUE)

model_data %>% tabyl(ApplicationProtocol, aid)
model_data %>% tabyl(DestinationIP, aid)

predict_data <- model_data %>% sample_n(10)
prediction <- predict(model, predict_data)

predict_data %>%
  select(one_of(varimp$col)) %>%
  add_column(pred = prediction)

