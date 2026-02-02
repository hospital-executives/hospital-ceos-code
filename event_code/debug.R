library(dplyr)
library(haven)
library(rstudioapi)

rm(list = ls())
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("../build_code/config.R")
}

event_df <- read_stata(paste0(derived_data, "/temp/event_data.dta"))
individuals <- read_feather(paste0(derived_data, "/individuals_final.feather"))
hospitals <- read_feather(paste0(derived_data, "/hospitals_final.feather"))

event_mini <- event_df %>% 
  select(aha_id, year, contact_uniqueid, contact_lag1, `_prev_ceo_merge`, ceo_turnover1,prev_ceo_aha_id, prev_left, entity_uniqueid) %>%
  mutate(contact_lag1 = ifelse(contact_lag1 == "", NA, contact_lag1)) %>%
  rename(entity_aha = aha_id) %>%
  filter(year != 2009)

## check leavers
leavers <- event_mini %>% filter(prev_left == 1) %>% 
  select(entity_aha, year, contact_lag1, prev_left, entity_uniqueid) %>%
  filter(!is.na(contact_lag1)) %>%
  mutate(contact_uniqueid = as.numeric(contact_lag1))

individuals_mini <- individuals %>% 
  select(entity_aha, contact_uniqueid, year, title, title_standardized, entity_name, haentitytypeid, entity_type, type, entity_uniqueid)

merged <- leavers %>% left_join(individuals_mini, by = c("contact_uniqueid")) %>% 
  filter(!is.na(title)) %>%
  filter(year.x < year.y | year.x == year.y) %>%
  mutate(gap = year.y - year.x) %>%
  group_by(entity_aha.x, year.x) %>%
  mutate(min_gap = min(gap)) %>%
  ungroup()

test_bug <- merged %>% filter(min_gap == 1)

%>%
  left_join(event_mini %>% distinct(entity_aha, prev_left, year) %>%
              rename(entity_aha.y = entity_aha, prev_left.y = prev_left)) %>%
  filter(prev_left.y == 1 | is.na(prev_left.y))

merged_aha <- merged %>% filter(!is.na(entity_aha.y))

check_indivs <- individuals %>%
  filter(contact_uniqueid == 86280) %>%
  select(firstname, lastname, title, title_standardized, year, entity_name, entity_aha)

## check single hospital systems
check_hosp <- individuals %>%
  filter(entity_aha ==    6933925  & all_leader_flag) %>%
  #filter(entity_uniqueid == 36867 & all_leader_flag) %>%
  select(confirmed, contact_uniqueid, firstname, lastname, title, title_standardized, year, entity_uniqueid, entity_name, entity_aha, haentitytypeid, entity_type)

check_sys <- individuals %>%
  filter(entity_uniqueid == 28170 & all_leader_flag) %>%
  select(contact_uniqueid, firstname, lastname, title, title_standardized, year, entity_uniqueid, entity_name, entity_aha, haentitytypeid, entity_type)

check_year <- individuals %>%
  filter(entity_uniqueid == 36867 & year == 2015) %>%
  select(confirmed, contact_uniqueid, firstname, lastname, title, title_standardized, year, entity_uniqueid, entity_name, entity_aha, haentitytypeid, entity_type)

## validate leavers
leavers_no_turnover <- event_mini %>% filter(`_prev_ceo_merge` == 1 & ceo_turnover1 == 0)

#### check for dropped obs + missing CEOs ####
hospitals_mini <- hospitals %>% select(entity_aha, entity_uniqueid, entity_name, haentitytypeid, year, is_hospital, all_ceo)
individuals_mini <- individuals %>% 
  select(entity_aha, contact_uniqueid, full_name, firstname, lastname, year, title, title_standardized, entity_uniqueid, all_leader_flag) 
merged_with_individuals <- hospitals_mini %>%
  left_join(individuals_mini, by = c("entity_uniqueid", "year", "entity_aha")) %>%
  filter(year > 2008) 

# doesnt merge - 19 AHAnumbers
all_na <- merged_with_individuals %>% 
  group_by(entity_uniqueid) %>%
  filter(all(is.na(contact_uniqueid))) %>% 
  ungroup() %>%
  filter(!is.na(entity_aha))

# never has hospital head - 61 AHAnumbers
missing_hospital_head <- merged_with_individuals %>%
  group_by(entity_uniqueid) %>%
  filter(!any(all_leader_flag)) %>%
  ungroup() %>%
  filter(!is.na(entity_aha))

# ever missing hospital head 
missing_hospital_head <- merged_with_individuals %>%
  group_by(entity_uniqueid, year) %>%
  filter(all(!all_leader_flag)) %>%
  ungroup() %>%
  filter(!is.na(entity_aha)) %>%
  left_join(aha_df %>% distinct(ahanumber, year, full_aha, cleaned_title_aha, first_aha, last_aha) %>%
            rename(entity_aha = ahanumber), by = c("entity_aha", "year"))

missing_mini <- missing_hospital_head %>%
  distinct(entity_aha, year, entity_name, all_ceo, full_name, firstname, lastname, full_aha, first_aha, last_aha) %>%
  mutate(
    jw_dist_firstname = stringdist(firstname, first_aha, method = "jw", p = 0.1),
    jw_dist_lastname = stringdist(lastname, last_aha, method = "jw", p = 0.1),
    jw_dist_full_name = stringdist(full_name, full_aha, method = "jw", p = 0.1),
  )

# check from individuals df
dropped <- event_mini %>% 
  group_by(entity_aha) %>%
  arrange(entity_aha, year) %>%
  mutate(
    actual_lag_year = lag(year),
    actual_lead_year = lead(year),
    lag_year = year - 1,
    lead_year = year + 1,
    contact_lag = lag(contact_uniqueid),
    contact_lead = lead(contact_uniqueid),
    gap = (actual_lag_year != lag_year & !is.na(actual_lag_year)) | (actual_lead_year != lead_year & !is.na(actual_lead_year))
  ) %>%
  filter(any(gap)) %>%
  ungroup()

gap <- dropped %>% 
  group_by(entity_aha) %>%
  filter(any(contact_lag == contact_lead & gap)) %>%
  ungroup()

# check from hospitals df


#### check gaps ####
raw_himss <- read_feather(paste0(derived_data, "/himss_entities_contacts_0517_v1.feather"))

check_raw <- raw_himss %>% filter(entity_uniqueid == 18861 & year == 2014) %>%
  distinct(firstname, lastname, title, title_standardized)

## check if people really never go to systems - not capturing
check_system <- individuals %>%
  group_by(contact_uniqueid) %>%
  filter(any(haentitytypeid == 8)) %>%
  ungroup()

system_mini <- check_system %>% 
  select(contact_uniqueid, year,  firstname, lastname, title, title_standardized, 
         entity_aha, entity_uniqueid, entity_name, haentitytypeid,all_leader_flag) %>%
  group_by(contact_uniqueid) %>%
  filter(any(!is.na(entity_aha) & all_leader_flag )) %>%
  ungroup() %>% 
  left_join(event_mini , by = c("entity_aha", "year", "entity_uniqueid")) 

sample_only <- system_mini %>%
  group_by(contact_uniqueid.x) %>%
  filter(any(!is.na(ceo_turnover1))) %>%
  mutate(
    last_year_type1 = if(any(haentitytypeid == 1, na.rm = TRUE)) {
      max(year[haentitytypeid == 1], na.rm = TRUE)
    } else {
      NA_real_
    },
    last_year_type8 = if(any(haentitytypeid == 8, na.rm = TRUE)) {
      max(year[haentitytypeid == 8], na.rm = TRUE)
    } else {
      NA_real_
    },
    first_turnover = if(any(ceo_turnover1 == 1, na.rm = TRUE)) {
      min(year[ceo_turnover1 == 1], na.rm = TRUE)
    } else {
      NA_real_
    }
  ) %>%
  filter(last_year_type8 > first_turnover) %>%
  ungroup() %>%
  filter(all_leader_flag | haentitytypeid == 8)
#### check prev same hospital
same_hosp <- individuals %>% filter(entity_aha ==      6540510             & all_leader_flag) %>%
  select(confirmed, contact_uniqueid, firstname, lastname, title, title_standardized, year, entity_uniqueid, entity_name, entity_aha, haentitytypeid, entity_type)

check_hosp <- individuals %>% filter(contact_uniqueid == 120512) %>%
  select(confirmed, contact_uniqueid, firstname, lastname, title, title_standardized, year, entity_uniqueid, entity_name, entity_aha, haentitytypeid, entity_type)

## check lower level management
selected_title <- individuals %>% filter(title_standardized == "Chief Compliance Officer")

hospital_level_title <- hosp_sample %>% left_join(selected_title %>% distinct(contact_uniqueid, year, entity_uniqueid))

cat(sum(is.na(hospital_level_title$contact_uniqueid)))