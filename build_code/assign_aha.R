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
mcr_aha_xwalk <- read_dta(paste0(supplemental_data, "/hospital_ownership.dta"))

xwalk_mcr <- mcr_aha_xwalk %>% 
  distinct(ahaid_noletter, mcrnum, year) %>% 
  mutate(mcrnum = as.numeric(mcrnum),
         xwalk_aha = as.numeric(ahaid_noletter)) %>%
  filter(!is.na(mcrnum) & !is.na(xwalk_aha)) %>%
  distinct(mcrnum, year, xwalk_aha) 

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
  left_join(
    xwalk_mcr %>% rename(entity_mcrnum = mcrnum),
    by = c("entity_aha" = "xwalk_aha", "year" = "year")
  ) %>%
  # 2) campus_aha -> campus_mcrnum
  left_join(
    xwalk_mcr %>% rename(campus_mcrnum = mcrnum),
    by = c("campus_aha" = "xwalk_aha", "year" = "year")
  ) %>%
  mutate(is_hospital = !is.na(entity_aha)) %>% distinct()

write_feather(merged_haentity, paste0(derived_data,'/hospitals_with_xwalk.feather'))

## CREATE INDIVIDUAL LEVEL EXPORT
himss <- read_feather(paste0(derived_data, '/final_himss.feather'))

update_titles <- himss %>%
  mutate(title_clean = str_to_lower(title),
         title_clean = str_replace_all(title_clean, "[[:punct:]]", ""),
         ceo_himss_title_exact = title_clean == "ceo" | title_clean == "chief executive officer",
         ceo_himss_title_fuzzy = str_detect(title_clean, "ceo|chief executive") & 
           !str_detect(title_clean, "assistant") & 
           !str_detect(title_clean, "nurse|ambulatory") &
           !str_detect(title, "Senior & Community Services"),
         freeform_pres = title_clean == "president",
         freeform_admin = title_clean == "administrator" |
           title_clean == "executive director" |
           str_detect(title_clean, "^president\\b.*\\b(coo|chief operating officer)") |
           str_detect(title_clean, "^vp\\b.*\\badministrator")) %>%
  group_by(entity_uniqueid, year) %>%
  mutate(no_ceo = !any(title_standardized == "CEO:  Chief Executive Officer"),
         ceo_flag = no_ceo & n_distinct(contact_uniqueid[ceo_himss_title_fuzzy]) == 1,
         pres_flag = no_ceo & n_distinct(contact_uniqueid[freeform_pres]) == 1 & !any(ceo_flag),
         admin_flag = no_ceo & n_distinct(contact_uniqueid[freeform_admin]) == 1 & 
           !any(pres_flag) & !any(ceo_flag)) %>%
  ungroup() %>%
  mutate(ceo_himss_title_general = case_when(
    title_standardized == "CEO:  Chief Executive Officer" ~ TRUE,
    ceo_himss_title_fuzzy & ceo_flag ~ TRUE,
    freeform_pres & pres_flag ~ TRUE,
    freeform_admin & admin_flag ~ TRUE,
    TRUE ~ FALSE
  )) %>% select(-c(title_clean, freeform_pres, freeform_admin,
                   ceo_flag, pres_flag, admin_flag, no_ceo)) %>%
  group_by(contact_uniqueid, year) %>%
  mutate(num_titles = n_distinct(title_standardized)) %>%
  ungroup() %>%
  filter(!(title_standardized == "CIO Reports to" & num_titles > 1 & ceo_himss_title_general == FALSE)) %>%
  select(-num_titles) 

himss_mini <- update_titles %>% # confirmed
  select(himss_entityid, year, id, entity_uniqueid, 
         ceo_himss_title_exact, ceo_himss_title_fuzzy, ceo_himss_title_general) %>%
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
  left_join(
    xwalk_mcr %>% rename(entity_mcrnum = mcrnum),
    by = c("entity_aha" = "xwalk_aha", "year" = "year")
  ) %>%
  # 2) campus_aha -> campus_mcrnum
  left_join(
    xwalk_mcr %>% rename(campus_mcrnum = mcrnum),
    by = c("campus_aha" = "xwalk_aha", "year" = "year")
  ) %>%
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
    ceo_himss_title_general = ifelse(is.na(ceo_himss_title_general), FALSE, ceo_himss_title_general),
    ceo_himss_title_exact = ifelse(is.na(ceo_himss_title_exact), FALSE, ceo_himss_title_exact),
    ceo_himss_title_fuzzy = ifelse(is.na(ceo_himss_title_fuzzy), FALSE, ceo_himss_title_fuzzy))

write_feather(final_merged,paste0(derived_data,'/individuals_with_xwalk.feather'))



