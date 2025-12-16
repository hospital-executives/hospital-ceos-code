library(dplyr)
library(haven)
library(rstudioapi)

rm(list = ls())
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("../build_code/config.R")
}

individuals <- read_feather(paste0(derived_data, "/individuals_final.feather"))
filled_indiv <- read_feather(paste0(derived_data, "/temp/updated_individuals.feather"))
hospitals <- read_feather(paste0(derived_data, "/hospitals_final.feather"))

ceo_ids <- filled_indiv %>% distinct(contact_uniqueid) %>% pull(contact_uniqueid)

mini <- individuals %>% filter(contact_uniqueid %in% ceo_ids & title_standardized != "CIO Reports to") %>%
  distinct(confirmed, entity_uniqueid, entity_aha, sysid, year, contact_uniqueid, all_leader_flag, 
           title, title_standardized,haentitytypeid, bdtot) %>%
  mutate(contact_uniqueid = as.character(contact_uniqueid)) %>%
  mutate(
    title_lower = tolower(gsub("[[:punct:]]", "", title)),
    char_ceo = str_detect(title_lower, "ceo|chief executive officer| c e o"),
    std_ceo = title_standardized == "CEO:  Chief Executive Officer",
  )

## helper dfs for flags
max_bdtot_lookup <- mini %>%
  filter(!is.na(bdtot)) %>%
  group_by(contact_uniqueid) %>%
  summarize(
    max_bdtot_ever = max(bdtot),
    year_of_max_bdtot = year[which.max(bdtot)],
    .groups = "drop"
  )

max_bdtot_by_year_lookup <-  mini %>%
  group_by(contact_uniqueid, year) %>%
  summarize(max_bdtot_this_year = max(bdtot), .groups = "drop") %>%
  group_by(contact_uniqueid) %>%
  arrange(year, .by_group = TRUE) %>%
  mutate(max_bdtot_next_year = lead(max_bdtot_this_year)) %>%
  ungroup() %>%
  select(contact_uniqueid, year, max_bdtot_next_year)

next_year_sys_lookup <- hospitals %>% distinct(entity_uniqueid, year, sysid) %>%
  group_by(entity_uniqueid) %>%
  arrange(year, .by_group = TRUE) %>%
  mutate(next_year_sys = lead(sysid)) %>%
  ungroup() %>% select(-sysid)

# df with all flags
mini_with_flags <- mini %>%
  arrange(contact_uniqueid, year) %>%
  # get indicators for if they're in the sample, at a non-hospital/hospital/system, or a leader
  group_by(contact_uniqueid) %>%
  mutate(
    exists_future = as.integer(year < max(year)),
    sys_future = sapply(year, function(y) {
      any(haentitytypeid[year > y] == 8, na.rm = TRUE)
    }),
    hosp_future = sapply(year, function(y) {
      any(haentitytypeid[year > y] == 1, na.rm = TRUE)
    }),
    non_hosp_future = sapply(year, function(y) {
      any(haentitytypeid[year > y] != 1, na.rm = TRUE)
    }),
    leader_future = sapply(year, function(y) {
      any(all_leader_flag[year > y] == 1, na.rm = TRUE)
    })
  ) %>%
  ungroup() %>%
  # get indicator for if they're ever at the same hospital
  group_by(contact_uniqueid, entity_uniqueid) %>%
  mutate(
    max_year_at_hospital = max(year),
    future_at_same_hospital = as.integer(year < max_year_at_hospital & exists_future == 1)
  ) %>%
  ungroup() %>%
  select(-max_year_at_hospital) %>%
  # get indicator for if they're ever at the same system
  group_by(contact_uniqueid, sysid) %>%
  mutate(
    max_year_at_sys = max(year),
    future_at_same_sys = as.integer(year < max_year_at_sys)
  ) %>%
  ungroup() %>%
  select(-max_year_at_sys) %>%
  # get maximum bdot in next available year
  left_join(max_bdtot_by_year_lookup, by = c("contact_uniqueid", "year")) %>%
  # get maximum bdtot 
  left_join(max_bdtot_lookup, by = "contact_uniqueid") %>%
  # get next year's sysid and create an indicator
  left_join(next_year_sys_lookup, by = c("entity_uniqueid", "year")) %>%
  group_by(contact_uniqueid) %>%
  mutate(
    has_match_next_year = as.integer(
      mapply(function(y, nys) {
        any(sysid[year == y + 1] == nys, na.rm = TRUE)
      }, year, next_year_sys)
    )
  ) %>%
  ungroup()

# prepare export
export <- mini_with_flags %>%
  distinct(entity_uniqueid, entity_aha, sysid, year, 
           contact_uniqueid, confirmed, char_ceo, std_ceo,
           exists_future, sys_future, hosp_future, non_hosp_future, leader_future,
           future_at_same_hospital, future_at_same_sys,
           max_bdtot_next_year, max_bdtot_ever, year_of_max_bdtot,
           next_year_sys, has_match_next_year) %>%
  mutate(contact_uniqueid = as.numeric(contact_uniqueid))

# clean backfilled entries 
backfilled_obs <- filled_indiv %>% 
  left_join(export, by = c("entity_uniqueid", "year", "contact_uniqueid")) %>%
  filter(is.na(char_ceo) & is.na(std_ceo) & contact_uniqueid > 0) %>%
  distinct(entity_uniqueid, year, contact_uniqueid) %>%
  mutate(
    entity_aha = case_when(
      entity_uniqueid == 12018 ~  6520040,
      entity_uniqueid == 12020 ~ 6520470 ,
      entity_uniqueid == 12717 ~ 6860315,
      entity_uniqueid == 18616 ~ 6540840,
      entity_uniqueid == 19850 ~ 6441850,
      entity_uniqueid == 20732 ~ 6410032,
      entity_uniqueid == 20740 ~ 6412110,
      entity_uniqueid == 20770 ~ 6412180,
      entity_uniqueid == 31610 ~ 6390765 ,
      entity_uniqueid == 31622 ~ 6390295,
      entity_uniqueid == 32924 ~ 6931140,
      entity_uniqueid == 32928 ~ 6930648,
      entity_uniqueid == 32929 ~ 6931130,
      entity_uniqueid == 32930 ~ 6932810,
      entity_uniqueid == 34671 ~ 6740138,
      entity_uniqueid == 34673 ~ 6740243,
      entity_uniqueid == 35226 ~ 6411340 ,
      entity_uniqueid == 46525 ~ 6742058,
      entity_uniqueid == 62856 ~  6740878,
      TRUE ~ NA
    )
  )

future_indiv <- individuals %>% filter(contact_uniqueid %in% backfilled_obs$contact_uniqueid) %>%
  distinct(entity_aha, haentitytypeid, all_leader_flag, contact_uniqueid, year, sysid)


## need to get sys in that year
aha_data <- read.csv("../build_code/temp/cleaned_aha.csv")
aha_xwalk <- aha_data %>% distinct(year, ahanumber, sysid) %>%
  rename(entity_aha = ahanumber)

merged <- backfilled_obs %>% 
  mutate(exists_future = 1,
         future_at_same_hospital = 1) %>%
  # get flags if change occurs in the next year
  left_join(future_indiv, by = "contact_uniqueid") %>%
  filter(year.x < year.y) %>%
  rename(year = year.x, entity_aha = entity_aha.x, 
         next_year_aha = entity_aha.y, next_year_sys = sysid)  %>%
  group_by(contact_uniqueid, year) %>%
  mutate(sys_future_1 = any(haentitytypeid == 8),
         hosp_future_1 = any(haentitytypeid == 1),
         non_hosp_future_1 = any(haentitytypeid != 1),
         any_future_leader_1 = any(all_leader_flag)) %>%
  # get flags if change occurs in the future
  select(-year.y) %>%
  left_join(export %>% 
              distinct(contact_uniqueid, year, sys_future, hosp_future, non_hosp_future, leader_future), 
            by = "contact_uniqueid") %>%
  filter(year.x < year.y) %>%
  rename(year = year.x) %>%
  group_by(contact_uniqueid, year) %>%
  mutate(
    sys_future = any(sys_future | sys_future_1, na.rm = TRUE),
    hosp_future = any(hosp_future | hosp_future_1, na.rm = TRUE),
    non_hosp_future = any(non_hosp_future | non_hosp_future_1, na.rm = TRUE),
    leader_future = any(leader_future | any_future_leader_1, na.rm = TRUE),
    exists_future = max(exists_future, na.rm = TRUE),
    future_at_same_hospital = max(future_at_same_hospital, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  # merge in sysid for that year
  left_join(aha_xwalk, by = c("entity_aha", "year")) %>% 
  mutate(equal_sysid = sysid == next_year_sys) %>%
  group_by(contact_uniqueid, sysid) %>%
  mutate(
    max_year_at_sys = if (any(equal_sysid, na.rm = TRUE)) max(year[equal_sysid], na.rm = TRUE) else NA_integer_,
    future_at_same_sys = as.integer(year < max_year_at_sys)
  )%>%
  mutate(contact_uniqueid = as.character(contact_uniqueid)) %>%
  # get maximum bdot in next available year
  left_join(max_bdtot_by_year_lookup, by = c("contact_uniqueid", "year")) %>%
  # get maximum bdtot 
  left_join(max_bdtot_lookup, by = "contact_uniqueid") %>%
  # get next year's sysid and create an indicator
  group_by(contact_uniqueid) %>%
  mutate(has_match_next_year = any(sysid == next_year_sys & !is.na(sysid) & !is.na(next_year_sys))) %>%
  ungroup()
  
missing_export <- merged %>%
  distinct(entity_uniqueid, entity_aha, sysid, year, 
           contact_uniqueid, 
           exists_future, sys_future, hosp_future, non_hosp_future, leader_future,
           future_at_same_hospital, future_at_same_sys,
           max_bdtot_next_year, max_bdtot_ever, year_of_max_bdtot,
           sysid, next_year_sys, has_match_next_year) %>%
  mutate(contact_uniqueid = as.numeric(contact_uniqueid)) %>%
  mutate(
    confirmed = NA, char_ceo = NA, std_ceo = NA, imputed = TRUE,
  ) %>%
  group_by(contact_uniqueid) %>%
  mutate(
    num_obs = n(),
    distinct_next_sys = n_distinct(next_year_sys, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(outliers = num_obs > 1 & distinct_next_sys > 1) %>%
  filter(
    (num_obs == 1) |
    (distinct_next_sys == 1 & !is.na(next_year_sys)) |
    (outliers & sysid != next_year_sys)
  ) %>%
  select(-num_obs, -distinct_next_sys, -outliers)

updated_export <- export %>% mutate(imputed = FALSE) %>% 
  anti_join(missing_export %>% distinct(entity_uniqueid, entity_aha, year, contact_uniqueid))

combined_export <- rbind(missing_export, updated_export)

final_export <- filled_indiv %>% 
  left_join(combined_export, by = c("entity_uniqueid", "year", "contact_uniqueid")) %>% 
  filter(char_ceo | std_ceo | contact_uniqueid < 0 | contact_uniqueid %in% backfilled_obs$contact_uniqueid) %>% 
  select(-char_ceo, -std_ceo) %>%
  distinct() %>%
  group_by(entity_uniqueid, year) %>%
  mutate(mult_contacts = n_distinct(contact_uniqueid) > 1,
         mult_obs = n() > 1) %>% 
  ungroup() 

cleaned <- final_export %>% 
  mutate(
    aha_id = ifelse(is.na(aha_id), entity_aha, aha_id),
    sysid = case_when(
      !is.na(sysid.x) ~ sysid.x,
      !is.na(sysid.y) ~ sysid.y,
      TRUE ~ NA
    )) %>% select(-sysid.x, -sysid.y) %>%
  group_by(contact_uniqueid) %>%
  mutate(confirmed = all(confirmed, na.rm = TRUE) & any(confirmed, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(next_year = year + 1)

mults <- cleaned %>% filter(mult_obs) %>% distinct() %>% arrange(aha_id, year)
cat(sum(cleaned$mult_obs))

write_dta(cleaned %>% select(-mult_obs), paste0(derived_data, "/temp/updated_trajectories.dta"))

# create individual level df
future_indiv <- individuals %>% filter(contact_uniqueid %in% backfilled_obs$contact_uniqueid) %>%
  distinct(contact_uniqueid, year, entity_uniqueid, 
           title, title_standardized, ceo_himss_title_exact,ceo_himss_title_fuzzy, all_leader_flag)

new_obs <- cleaned %>% filter(imputed) %>%
  left_join(future_indiv, by = c("entity_uniqueid", "contact_uniqueid")) %>%
  filter(year.x == year.y + 1 | year.x == year.y - 1) %>%
  filter(title_standardized == "CEO:  Chief Executive Officer") %>%
  mutate(std_ceo = title_standardized == "CEO:  Chief Executive Officer",
         prev_ceo = std_ceo & year.x == year.y + 1 , 
         post_ceo = std_ceo & year.x == year.y - 1) %>%
  group_by(entity_uniqueid, contact_uniqueid) %>%
  mutate(
   final_title = any(prev_ceo) & any(post_ceo)
  ) %>%
  ungroup() %>%
  distinct(contact_uniqueid, entity_aha, entity_uniqueid, year.x,
           final_title, confirmed) %>%
  mutate(title_standardized = "CEO:  Chief Executive Officer",
         ceo_himss_title_exact = TRUE,
         ceo_himss_title_fuzzy = TRUE,
         all_leader_flag = TRUE) %>%
  rename(year = year.x) %>% select(-final_title)

old_obs <- individuals %>% 
  select(contact_uniqueid, entity_aha, entity_uniqueid, year,confirmed,
         title_standardized, ceo_himss_title_exact, ceo_himss_title_fuzzy, all_leader_flag)

individuals_for_event <- rbind(new_obs, old_obs)

write_dta(individuals_for_event, paste0(derived_data, "/temp/updated_individual_titles.dta"))



## test future at same hosp
test <- mini %>%
  group_by(contact_uniqueid) %>%
  mutate(
    exists_future = as.integer(year < max(year))
  ) %>%
  ungroup() %>%
  group_by(contact_uniqueid, entity_uniqueid) %>%
  mutate(
    max_year_at_hospital = max(year),
    future_at_same_hospital = as.integer(year < max_year_at_hospital)
  ) %>%
  ungroup() %>%
  filter(future_at_same_hospital == 1)

view <- test %>% filter(contact_uniqueid ==   2365458) %>%
  distinct(entity_uniqueid, contact_uniqueid, year, future_at_same_hospital,exists_future) %>%
  arrange(entity_uniqueid, year)

check_indiv <- individuals %>% filter(contact_uniqueid ==      2363882  ) %>%
  distinct(entity_name, title, title_standardized, year, entity_aha, haentitytypeid)

## what trajectory outcomes do we want?

# prev ceo left sample (never exists_future)
# prev other hospital (never at the same hospital)
# prev same hospital (ever at same hospital)

# prev other "hospital" is actually other healthcare
# can be split into the following:
# ever at another hospital/never at another hospital => done
# ever at a larger/smaller hospital => done
# want (a) wherever they are next (b) where they are next that theyre CEO 
# (c) if theyre ever somewhere bigger and (d) if theyre ever somewhere bigger and CEO
# two system level splits: => need to get all future sysid that is a part of
