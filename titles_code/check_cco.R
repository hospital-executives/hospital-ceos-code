rm(list = ls())
library(rstudioapi)
library(purrr)
library(janitor)

# load data
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("../build_code/config.R")
  hospitals <- read_feather(paste0(derived_data, "/hospitals_with_xwalk.feather"))
  cleaned_individuals <- read_feather(paste0(derived_data, "/individuals_final.feather"))
  supp_path <- supplemental_data
  output_dir <- paste0(data_file_path, "/summary_stats/execs")
} else {
  args <- commandArgs(trailingOnly = TRUE)
  source("../build_code/config.R")
  hospitals <- read_feather(args[1])
  cleaned_individuals <- read_feather(args[2])
  supp_path <- args[3]
  output_dir <- args[4] 
}

all_ccos <- individuals %>%
  group_by(contact_uniqueid) %>%
  filter(any(title_standardized == "Chief Compliance Officer")) %>%
  ungroup() 

select_entities <- read_stata("cco_turnover.dta")

merged <- all_ccos %>% 
  left_join(select_entities %>% mutate(merger_cco_turnover = TRUE)) %>%
  # Create next_year for joining
  mutate(next_year = year + 1) %>%
  # Join to get the CCO at this entity in the next year
  left_join(
    all_ccos %>% 
      select(entity_uniqueid, year, next_year_cco = contact_uniqueid),  # adjust person_id to your actual column name
    by = c("entity_uniqueid" = "entity_uniqueid", "next_year" = "year")
  ) %>%
  # Join to count how many entities that CCO is at in the next year
  left_join(
    all_ccos %>%
      group_by(contact_uniqueid, year) %>%
      summarize(cco_entity_count_next_year = n_distinct(entity_uniqueid), .groups = "drop"),
    by = c("next_year_cco" = "contact_uniqueid", "next_year" = "year")
  )  

num = merged %>% group_by(entity_uniqueid) %>% filter(any(merger_cco_turnover)) %>%
  ungroup() %>% select(entity_uniqueid, year, cco_entity_count_next_year)
  
merged <- all_ccos %>% 
  left_join(select_entities %>% mutate(merger_cco_turnover = TRUE)) %>%
  # Create previous year for joining
  mutate(prev_year = year - 1) %>%
  # Join to get the CCO at this entity in the PREVIOUS year
  left_join(
    all_ccos %>% 
      select(entity_uniqueid, year, prev_cco = contact_uniqueid),  # adjust column name as needed
    by = c("entity_uniqueid" = "entity_uniqueid", "prev_year" = "year")
  ) %>%
  # Check if that previous CCO still exists as a contact at this entity in the current year
  left_join(
    individuals %>%  # or whatever your contacts table is called
      select(entity_uniqueid, year, contact_uniqueid) %>%
      mutate(prev_cco_still_at_entity = TRUE),
    by = c("entity_uniqueid" = "entity_uniqueid", 
           "year" = "year", 
           "prev_cco" = "contact_uniqueid")
  ) %>%
  # Convert NA to FALSE
  mutate(prev_cco_still_at_entity = coalesce(prev_cco_still_at_entity, FALSE))

merged %>%
  filter(merger_cco_turnover) %>%
  count(prev_cco_still_at_entity) %>%
  mutate(pct = n / sum(n) * 100)


## case study - christopherdoan, susandeaton, (2/9)

cco_avg_titles <- ccos %>%
  group_by(contact_uniqueid, year) %>%
  mutate(avg_annual_titles = n_distinct(title_standardized)) %>%
  ungroup() %>%
  group_by(contact_uniqueid) %>%
  mutate(avg_titles_per_cco = mean(avg_annual_titles)) %>%
  ungroup() %>%
  distinct(contact_uniqueid, avg_titles_per_cco) # average is 2.1 titles

title_pairs <- ccos %>%
  distinct(contact_uniqueid, year, title_standardized) %>%
  inner_join(
    ccos %>% distinct(contact_uniqueid, year, title_standardized),
    by = c("contact_uniqueid", "year"),
    relationship = "many-to-many"
  ) %>%
  filter(title_standardized.x < title_standardized.y) %>%  # avoid duplicates and self-pairs
  count(title_standardized.x, title_standardized.y, sort = TRUE) %>%
  rename(title_1 = title_standardized.x, title_2 = title_standardized.y) %>%
  filter(title_1 == "Chief Compliance Officer" | title_2 == "Chief Compliance Officer")

# View top co-occurring pairs
head(title_pairs, 20)