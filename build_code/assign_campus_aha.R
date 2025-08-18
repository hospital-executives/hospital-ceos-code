
## load and format data
library(dplyr)
library(stringr)
library(janitor)
library(haven)
library(data.table)
library(arrow)
library(geosphere)
library(rstudioapi)

rm(list = ls())

args <- commandArgs(trailingOnly = TRUE)
code_path <- args[1]
data_path <- paste0(args[2], "/_data/")

## set up scripts
if (rstudioapi::isAvailable()) {
  script_directory <- dirname(rstudioapi::getActiveDocumentContext()$path)
} else {
  script_directory <- code_path
}
config_path <- file.path(script_directory, "config.R")
source(config_path)
rm(script_directory, config_path)

## load hospitals 
hospital_df <- read_feather(paste0(derived_data,'/himss_aha_hospitals_final.feather'))

non_na_campus_aha_obs <- nrow(hospital_df %>% filter(!is.na(campus_aha)))

## match on system ids
hospital_df <- hospital_df %>% 
  rename(himss_sysid = system_id, 
         aha_sysid = sysid) %>%
  group_by(entity_aha, year) %>%
  mutate(entity_himss_sys =  ifelse(is.na(entity_aha), NA, himss_sysid)) %>%
  ungroup() %>%
  group_by(campus_aha, year) %>%
  mutate(
    campus_himss_sys = dplyr::first(
      entity_himss_sys[dplyr::near(entity_aha, campus_aha) & !is.na(entity_himss_sys)],
      default = NA_character_
    )
  ) %>% ungroup() %>%
  mutate(real_campus_aha = 
           ifelse(
             himss_sysid == campus_himss_sys, campus_aha, NA))

sys_matches <- hospital_df %>% filter(himss_sysid == campus_himss_sys)

## entity aha already assigned
assigned_obs <- hospital_df %>% filter(!is.na(campus_aha)) %>%
  filter((!is.na(entity_aha)))

## match on names
add_match <-  hospital_df %>% 
  select(campus_aha, entity_aha, entity_name, mname, entity_address, mlocaddr,
         year, entity_uniqueid, haentitytypeid) %>%
  mutate(
    add_sim = stringsim(entity_address, mlocaddr, method = "jw", p = 0.1), 
    name_sim = stringsim(entity_name, mname, method = "jw", p = 0.1),
    real_campus_aha = ifelse(name_sim >= .85, campus_aha, NA)) %>% 
  filter(is.na(entity_aha) & real_campus_aha == campus_aha & !is.na(campus_aha))

## aggregate all currently matched
curr_matches <- rbind(assigned_obs %>% 
                        distinct(entity_uniqueid, year, campus_aha),
                      add_match %>% 
                        distinct(entity_uniqueid, year,campus_aha),
                      sys_matches %>% 
                        distinct(entity_uniqueid, year,campus_aha)) %>%
  distinct()

## match on address + name
add_name_matches <- hospital_df %>% filter(!is.na(campus_aha)) %>% 
  anti_join(curr_matches)  %>% 
  select(campus_aha, entity_aha, entity_name, mname, entity_address, mlocaddr,
         year, entity_uniqueid, haentitytypeid) %>%  mutate(
           add_sim = stringsim(entity_address, mlocaddr, method = "jw", p = 0.1), 
           name_sim = stringsim(entity_name, mname, method = "jw", p = 0.1),
           real_campus_aha = ifelse(add_sim >= .95 & name_sim >= .75, campus_aha, NA)) %>% 
  filter(is.na(entity_aha) & real_campus_aha == campus_aha & !is.na(campus_aha))

post_add_matches <- rbind(curr_matches, 
                          add_name_matches %>% 
                          distinct(entity_uniqueid, year, campus_aha)) %>%
  distinct()

cat(nrow(post_add_matches)/non_na_campus_aha_obs)

not_geocoded <- hospital_df %>% filter(!is.na(campus_aha)) %>%
  anti_join(post_add_matches) %>%
  filter(geocoded == "False") %>% 
  select(mname, entity_name, mlocaddr, entity_address, ahanumber, 
         entity_aha, year, entity_uniqueid, campus_aha) %>%
  mutate(mname = as.character(mname), entity_name = as.character(entity_name),
         jw_sim = ifelse(
           !is.na(mname),
           1 - stringdist(entity_name, mname, method = "jw", p = 0.1),
           NA_real_
         )
  ) %>% filter(jw_sim >= .75)

post_geo_matches <- rbind(post_add_matches, 
                          not_geocoded %>% 
                            distinct(entity_uniqueid, year, campus_aha)) %>%
  distinct()
cat(nrow(post_geo_matches)/non_na_campus_aha_obs)

## group_by entity_uniqueid
cleaned_hospitals <- hospital_df %>%
  left_join(post_geo_matches %>% rename(real_campus_aha = campus_aha)) %>%
  group_by(entity_uniqueid) %>%
  mutate(
    campus_before = zoo::na.locf(real_campus_aha, na.rm = FALSE),
    campus_after  = zoo::na.locf(real_campus_aha, fromLast = TRUE, na.rm = FALSE),
    real_campus_aha = case_when(
      is.na(real_campus_aha) &
        ((!is.na(campus_before) & !is.na(campus_after) & campus_before == campus_after) | 
        (!is.na(campus_before) & is.na(campus_after))) ~ campus_before,
      is.na(real_campus_aha) &is.na(campus_before) & !is.na(campus_after) ~ campus_after,
        TRUE ~ real_campus_aha
    )
  ) %>% 
  filter(!is.na(real_campus_aha)) %>%
  distinct(entity_name, mname, campus_aha, entity_aha, mlocaddr, entity_address, mloccity, entity_city,
           real_campus_aha, year, entity_uniqueid) %>% 
  arrange(real_campus_aha, entity_uniqueid)

post_entity_matches <- rbind(post_geo_matches, 
                             cleaned_hospitals %>% 
                               distinct(entity_uniqueid, year, real_campus_aha) %>%
                               rename(campus_aha = real_campus_aha)) %>%
  distinct()
cat(nrow(post_entity_matches)/non_na_campus_aha_obs)

## look at remaining cases 
remaining <- hospital_df %>% filter(!is.na(campus_aha)) %>%
  anti_join(post_entity_matches) %>%
  distinct(entity_name, mname, campus_aha, entity_aha, mlocaddr, entity_address, mloccity, entity_city,
           himss_sysid, campus_himss_sys, year)  %>%  
  mutate(add_sim = stringsim(entity_address, mlocaddr, method = "jw", p = 0.1), 
        name_sim = stringsim(entity_name, mname, method = "jw", p = 0.1)
        )

check_na_mname <- remaining %>% filter(is.na(mname))