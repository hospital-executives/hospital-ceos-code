## libraries
library(rstudioapi)
library(janitor)
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

temp_export <- read_feather(paste0(derived_data,'/himss_aha_xwalk.feather'))
export_xwalk <- temp_export %>% 
  distinct(himss_entityid, himss_sysid, campus_aha, entity_aha, latitude, longitude, 
           campus_fuzzy_flag, entity_fuzzy_flag, py_fuzzy_flag, unfiltered_campus_aha) %>%
  group_by(himss_entityid) %>%
  slice(1) %>% 
  ungroup() %>%
  mutate(ahanumber = campus_aha) %>%
  rename(geo_lat = latitude,
         geo_lon = longitude)

haentity <- read_feather(paste0(auxiliary_data,"/haentity.feather"))
aha_data <- read_feather(paste0(auxiliary_data, "/aha_data_clean.feather"))

merged_haentity <- haentity %>% select(-ahanumber) %>%
  mutate(himss_entityid = as.numeric(himss_entityid),
         entity_uniqueid = as.numeric(entity_uniqueid),
         year = as.numeric(year)) %>%
  left_join(export_xwalk) %>% 
  select(-any_of(setdiff(names(aha_data), c("year")))) %>%
  mutate(ahanumber = case_when(
    !is.na(entity_aha) ~ entity_aha,
    !is.na(campus_aha) ~ campus_aha,
    TRUE ~ unfiltered_campus_aha
  )) %>%
  left_join(aha_data) %>%
  mutate(is_hospital = !is.na(entity_aha))

write_feather(merged_haentity, paste0(derived_data,'/himss_aha_hospitals_final.feather'))
merged_haentity <- merged_haentity %>% clean_names() 
write_dta(merged_haentity, paste0(derived_data,'/himss_aha_hospitals_final.dta'))

## CREATE LEVEL EXPORT
himss <- read_feather(paste0(derived_data, '/final_himss.feather'))

update_titles <- himss %>%
  mutate(title_clean = str_to_lower(title),
         title_clean = str_replace_all(title_clean, "[[:punct:]]", ""),
         freeform_ceo = str_detect(title_clean, "ceo|chief executive") & 
           !str_detect(title_clean, "assistant") & 
           !str_detect(title_clean, "nurse|ambulatory") &
           !str_detect(title, "Senior & Community Services"),
         freeform_pres = title_clean == "president",
         freeform_admin = title_clean == "administrator" | title_clean == "executive director")  %>%
  group_by(entity_uniqueid, year) %>%
  mutate(no_ceo = !any(title_standardized == "CEO:  Chief Executive Officer"),
         ceo_flag = no_ceo & n_distinct(contact_uniqueid[freeform_ceo]) == 1,
         pres_flag =  no_ceo & n_distinct(contact_uniqueid[freeform_pres]) == 1 & !any(ceo_flag),
         admin_flag =  no_ceo & n_distinct(contact_uniqueid[freeform_admin]) == 1 & 
           !any(pres_flag) & !any(ceo_flag)) %>%
  ungroup() %>%
  mutate(hosp_has_ceo = case_when(
    title_standardized == "CEO:  Chief Executive Officer" ~ TRUE,
    freeform_ceo & ceo_flag ~ TRUE,
    freeform_pres & pres_flag ~ TRUE,
    freeform_admin & admin_flag ~ TRUE,
    TRUE ~ FALSE
  )) %>% select(-c(title_clean, freeform_ceo, freeform_pres, freeform_admin,
                   ceo_flag, pres_flag, admin_flag, no_ceo)) %>%
  group_by(contact_uniqueid, year) %>%
  mutate(num_titles = n_distinct(title_standardized)) %>%
  ungroup() %>%
  filter(!(title_standardized == "CIO Reports to" & num_titles > 1 & hosp_has_ceo == FALSE)) %>%
  select(-num_titles)

himss_mini <- update_titles %>% # confirmed
  select(himss_entityid, year, id, entity_uniqueid, hosp_has_ceo) %>%
  mutate(
    himss_entityid = as.numeric(himss_entityid),
    year = as.numeric(year)
  )

himss_to_aha_xwalk <- temp_export %>%
  distinct(himss_entityid, entity_uniqueid, year, entity_aha, campus_aha, 
           py_fuzzy_flag, latitude, longitude, unfiltered_campus_aha) %>%
  group_by(himss_entityid) %>%
  slice(1) %>% ungroup() %>%
  mutate(ahanumber = case_when(
    !is.na(entity_aha) ~ entity_aha,
    !is.na(campus_aha) ~ campus_aha,
    TRUE ~ unfiltered_campus_aha
  ))  %>%
  rename(geo_lat = latitude,
         geo_lon = longitude)


# Step 1: Convert both data frames to data.table
setDT(himss_mini)
setDT(himss_to_aha_xwalk)
setDT(aha_data)
setDT(himss)

# Step 3: Perform the left join using data.table syntax
merged_ahanumber <- merge(
  himss_mini,
  himss_to_aha_xwalk,
  by = c("himss_entityid", "year", "entity_uniqueid"),
  all.x = TRUE
)

## left join: 
merged_ahanumber <- himss_mini %>%
  left_join(
    himss_to_aha_xwalk,
    by = c("himss_entityid", "year", "entity_uniqueid")
  )

merged_aha <- merged_ahanumber %>%
  left_join(
    aha_data %>% mutate(
      ahanumber = as.numeric(str_remove_all(ahanumber, "[A-Za-z]")),
      year = as.numeric(year)),
    by = c("ahanumber", "year")
  )

himss_without_aha <-  himss %>%
  select(-all_of(setdiff(intersect(names(himss), names(merged_aha)), 
                         c("id", "year", "himss_entityid", "entity_uniqueid"))))

final_merged <- himss_without_aha %>%
  left_join(
    merged_aha %>% select(-c("year", "himss_entityid", "entity_uniqueid")),
    by = "id"
  )

final_merged <- final_merged %>%
  rename(ccn_aha = mcrnum,
         ccn_himss = medicarenumber) %>%
  mutate(
    campus_fuzzy_flag = case_when(
      is.na(py_fuzzy_flag) ~ NA, 
      TRUE ~ !is.na(campus_aha) & py_fuzzy_flag == 1
    ),
    entity_fuzzy_flag = case_when(
      is.na(py_fuzzy_flag) ~ NA, 
      TRUE ~ !is.na(entity_aha) & py_fuzzy_flag == 1
    ),
    is_hospital = !is.na(entity_aha),
    hosp_has_ceo = ifelse(is.na(hosp_has_ceo), FALSE, TRUE))

write_feather(final_merged,paste0(derived_data,'/final_aha.feather'))

final_merged <- final_merged %>% clean_names() 
write_dta(final_merged,paste0(derived_data, '/final_aha.dta'))

final_confirmed <- final_merged %>% filter(confirmed) %>% clean_names() 
write_feather(final_confirmed,paste0(derived_data,'/final_confirmed_aha.feather'))
write_dta(final_confirmed,paste0(derived_data, '/final_confirmed_aha.dta'))


