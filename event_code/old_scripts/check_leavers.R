## check leavers
library(dplyr)
library(haven)
library(rstudioapi)

rm(list = ls())
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("../build_code/config.R")
}

individuals <- read_feather(paste0(derived_data, "/individuals_final.feather"))
leavers <- read_stata(paste0(derived_data, "/temp/leavers.dta"))

## targets
set.seed(1215)
targets <- leavers %>% filter(balanced_2_year_sample == 1)
target_sample <- targets %>% slice_sample(n = 10)

set.seed(1216)
target_sample_2 <- targets %>% slice_sample(n = 10)

## controls
controls <- leavers %>% filter(never_m_and_a == 1)
set.seed(1216)
control_sample <- controls %>% slice_sample(n = 10)


## check individuals
mini <- individuals %>% distinct(confirmed, entity_name, entity_uniqueid, year, contact_uniqueid, 
                                 firstname, lastname, title, title_standardized, entity_state, haentitytypeid)

check_hosp <- mini %>% filter(entity_uniqueid == 45896 & year == 2009)

check_contact <- mini %>% filter(contact_uniqueid %in% c(666999,	498693))

find_oth <- mini %>% 
  #filter(str_detect(entity_name, "summit") & entity_state == "GA")
  #filter(str_detect(entity_name, "powell") & entity_state == "WY")
  filter(contact_uniqueid %in% c(2306979,2355642,2316254,483665))
  #filter(str_detect(lastname, "woodward") & entity_state %in% c("WI")) 

## original df 
raw_individuals <- read_feather(paste0(derived_data, "/himss_entities_contacts_0517_v1.feather"))
raw_mini <- raw_individuals %>% distinct(entity_name, entity_uniqueid, year, contact_uniqueid, 
                                         firstname, lastname, title, title_standardized, entity_state, haentitytypeid)
check_raw <- raw_mini %>% filter(contact_uniqueid %in% c(1334214,1357260,85470))
  filter(firstname == "Maureen" & lastname == "Bryant")

## merge leavers with contact_uniqueid count
counts <- read.csv("contact_counts.csv") %>% select(-X)

get_ids <- leavers %>% mutate(contact_uniqueid = as.numeric(contact_lag1)) %>%
  left_join(individuals %>% distinct(contact_uniqueid, id), by = "contact_uniqueid") %>%
  left_join(counts , by = "id") %>% filter(value_count <= 2)