
# Work With Bob's Firewall Data -------------------------------------------

pacman::p_load(tidyverse, sparklyr, fst)

data_big <- read_csv("C:/Users/exp01754/Downloads/r_bigdata/bobs_firewall/firewall.csv")
write_fst(data_big, "C:/Users/exp01754/Downloads/r_bigdata/bobs_firewall/firewall.fst")
write_csv(data_big %>% sample_n(10000), "data-small/firewall.csv")
rm(data_big)
