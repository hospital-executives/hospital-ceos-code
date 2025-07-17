## find entity_uniqueid in hospital level missing in final export

temp_export <- read_feather(paste0(derived_data,'/himss_aha_hospitals_final.feather'))
final_merged <- read_feather(paste0(derived_data,'/final_aha.feather'))
himss <- read_feather(paste0(derived_data, '/final_himss.feather'))

in_hospital <- temp_export %>% filter(year > 2008) %>% 
  distinct(entity_uniqueid) %>%
  pull(entity_uniqueid)
in_individual <- final_merged %>%
  filter(!is.na(mname)) %>%
  distinct(entity_uniqueid) %>%
  pull(entity_uniqueid)

missing_from_final <- setdiff(in_hospital, in_individual) # 58
only_missing_in_final <- setdiff(missing_from_final, missing_from_himss) # 74
# these missing you need to go back to the himss_to_aha.R code to debug

check_export <- temp_export %>% filter(entity_uniqueid %in% missing_from_final) %>%
  select(entity_name, year, mname, clean_aha, campus_aha)

in_himss <- unique(himss$entity_uniqueid)
missing_from_himss <- setdiff(in_hospital, in_himss) # 24
# these missing need to compare the two data sets and go further back in the build
# code

# there is one entity (15280) that does not exist in the "original" HIMSS
# data; this is because there is only one observation and there are no
# personnel listed.
og_himss <- read_feather("/Users/katherinepapen/Library/CloudStorage/Dropbox/hospital_ceos/_data/derived/himss_entities_contacts_0517_v1.feather")
check_df <- og_himss %>% filter(entity_uniqueid %in% missing_from_himss) 

# there are 19 entities that do not exist in the "original" 
# HIMSS once we have filtered for vacant positions (is.na(firstname))
filtered_entities <- unique(filtered_og_himss$entity_uniqueid)
not_in_filtered_himss <- setdiff(in_hospital, filtered_entities)
remaining_missing <- setdiff(missing_from_himss,not_in_filtered_himss)

# there are an additional 155 entities that only exist pre 2009, so these
# are dropped in the downstream HIMSS due to the missing contact_uniqueid
drop_pre_2009 <- check_df %>% mutate(year = as.numeric(year)) %>% 
  group_by(entity_uniqueid) %>%  filter(all(year < 2009)) %>% ungroup() %>% 
  distinct(entity_uniqueid) %>% pull(entity_uniqueid)
remaining_missing_2 <- setdiff(remaining_missing,drop_pre_2009)

# the remaining 5 entities only have personnel entries pre 2009

### check entities that exist in HIMSS "final" but not in merged data
only_missing_in_final <- setdiff(missing_from_final, missing_from_himss) # 74

check_missing <- himss %>% filter(entity_uniqueid %in% only_missing_in_final) %>%
  select(entity_uniqueid, entity_name, year, firstname,lastname, title_standardized, title)

himss <- read_feather(paste0(derived_data, '/final_himss.feather'))
himss_mini <- himss %>%
  select(himss_entityid, year, id, entity_uniqueid) %>%
  mutate(
    himss_entityid = as.numeric(himss_entityid),
    year = as.numeric(year)
  )

himss_to_aha_xwalk <- temp_export %>% distinct(himss_entityid, year, 
                                               clean_aha, campus_aha, py_fuzzy_flag) %>%
  mutate(ahanumber = clean_aha)

check_mini <- himss_mini %>% filter(entity_uniqueid == 12539) %>% distinct(year,entity_uniqueid)

check_ahanumber <- final_merged %>% filter(entity_uniqueid %in% only_missing_in_final) %>% 
  select(entity_uniqueid, ahanumber, year, firstname,lastname, madmin, mname)

get_himss_id <- himss %>% filter(entity_uniqueid == 10349) %>% distinct(year, himss_entityid)

check_xwalk <- himss_to_aha_xwalk %>% filter(entity_uniqueid == 22134) %>% distinct(ahanumber)
print(check_xwalk)

check_aha <- aha_data %>%
  mutate(
  ahanumber = as.numeric(str_remove_all(ahanumber, "[A-Za-z]"))) %>%
    filter(ahanumber == 6629025 & year > 2008)

check_final <- step4 %>% filter(entity_uniqueid == 22134) %>% #filter(entity_uniqueid == 22134 & year == 2009) %>%
  select(year, clean_aha,  madmin, mname, entity_name)

what <- step4 %>% filter(entity_name == "mercy west lakes" & year == 2009) %>% #filter(sys_aha == 6629025 & year == 2009) %>% #filter(str_detect(mname, "mercy") & year == 2009) %>%
  select(year, sys_aha, clean_aha,  madmin, mname, entity_name)

##
for (id_ in only_missing_in_final) {
  aha_id <- himss_to_aha_xwalk %>%
    filter(entity_uniqueid == id_) %>%
    distinct(ahanumber) %>%
    pull(ahanumber)
  
  # Skip if no aha_id found
  if (length(aha_id) == 0) {
    cat(id_, "NO AHA ID FOUND\n")
    next
  }
  
  check_aha <- aha_data %>%
    mutate(ahanumber = as.numeric(str_remove_all(ahanumber, "[A-Za-z]"))) %>%
    filter(ahanumber == aha_id & year > 2008)
  
  if (nrow(check_aha) != 0) {
  cat(id_, nrow(check_aha), "\n")}
}

## to check: 
#12539 2 
#12500 2 
#12501 2 
#22134 1 

check_himss <- final_merged %>% filter(entity_uniqueid == 22134) %>%
  distinct(entity_name, entity_address, year,mname, madmin, clean_aha) %>% filter(!is.na(firstname))


## check xwalk
check_xwalk <- himss_to_aha_xwalk %>% filter(entity_uniqueid == 22134)
check_export <- temp_export %>% filter(entity_uniqueid == 22134) %>%
  select(entity_name, entity_address, year, clean_aha, campus_aha)

check_aha <- aha_data %>%  mutate(
  ahanumber = as.numeric(str_remove_all(ahanumber, "[A-Za-z]"))) %>%
  filter(ahanumber %in% c(6620126,6620003,6629025)) %>%
  select(mname, mlocaddr, madmin, year, ahanumber)