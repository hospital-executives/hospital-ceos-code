
## load and format data
library(dplyr)
library(stringr)
library(janitor)
library(haven)
library(data.table)
library(arrow)
library(geosphere)

## libraries
library(rstudioapi)

rm(list = ls())

## set up scripts
if (rstudioapi::isAvailable()) {
  script_directory <- dirname(rstudioapi::getActiveDocumentContext()$path)
} else {
  script_directory <- params$code_dir
}
config_path <- file.path(script_directory, "config.R")
source(config_path)
rm(script_directory, config_path)

#### Pull in AHA<>MCR Crosswalk 2 - it should be the case that after updating
#### zip codes that all assigned AHAs in haentityhosp are assigned here
raw_xwalk <- read.csv(paste0(auxiliary_data, "/aha_himss_xwalk.csv"))
xwalk2 <- raw_xwalk %>%
  mutate(ahanumber = filled_aha) %>%
  group_by(entity_uniqueid) %>%
  mutate(
    unique_ahas = n_distinct(ahanumber, na.rm = TRUE), #) %>%
    known_aha = ifelse(ahanumber == 1, first(na.omit(ahanumber)), NA),
    ahanumber = ifelse(is.na(ahanumber), known_aha, ahanumber)
  ) %>%
  ungroup() %>%
  select(-unique_ahas, -known_aha) %>%
  arrange(entity_uniqueid, year) %>%
  group_by(entity_uniqueid) %>%
  mutate(
    ahanumber_before = zoo::na.locf(ahanumber, na.rm = FALSE),
    ahanumber_after  = zoo::na.locf(ahanumber, fromLast = TRUE, na.rm = FALSE) ,
    ahanumber_filled = case_when(
      is.na(ahanumber) & !is.na(ahanumber_before) & !is.na(ahanumber_after) & ahanumber_before == ahanumber_after ~ ahanumber_before,
      is.na(ahanumber) & !is.na(ahanumber_before) & is.na(ahanumber_after) ~ ahanumber_before,
      is.na(ahanumber) & is.na(ahanumber_before) & !is.na(ahanumber_after) ~ ahanumber_after,
      TRUE ~ ahanumber
    )
  ) %>%
  select(-ahanumber_before, -ahanumber_after) %>%
  ungroup()


#### load AHA data #####
columns_to_keep <- c( 
  "ID", 
  "YEAR", 
  "MNAME", 
  "HOSPN", 
  "MCRNUM",
  # "CBSANAME", 
  "HRRNAME", 
  "HSANAME", 
  "MADMIN", 
  "SYSID", 
  "SYSNAME", 
  "CNTRL", 
  "SERV", 
  "HOSPBD", 
  "BDTOT", 
  "ADMTOT", 
  "IPDTOT", 
  "MCRDC", 
  "MCRIPD", 
  "MCDDC", 
  "MCDIPD", 
  "BIRTHS", 
  "FTEMD", 
  "FTERN", 
  "FTE",
  "LAT",
  "LONG",
  "MLOCADDR", #street address
  "MLOCCITY", #city
  "MLOCZIP" #zip code
)

# set file path using manual input at beginning of script
file_path <- paste0(supplemental_data,"/AHA_2004_2017.csv")
#import the CSV
aha_data <- read_csv(file_path, col_types = cols(.default = col_character()))

# Use problems() function to identify parsing issues
parsing_issues <- problems(aha_data)

# Check if any parsing issues were detected
if (nrow(parsing_issues) > 0) {
  print(parsing_issues)
} else {
  print("No parsing problems detected.")
}

aha_data <- aha_data %>%
  select(all_of(columns_to_keep)) %>% 
  rename_all(tolower) %>% 
  rename(ahanumber = id,
         latitude_aha = lat,
         longitude_aha = long) %>% 
  mutate(
    # Convert to UTF-8
    mloczip = stri_trans_general(mloczip, "latin-ascii"),
    # cbsaname = stri_trans_general(cbsaname, "latin-ascii"),
    mlocaddr = stri_trans_general(mlocaddr, "latin-ascii") %>% 
      str_replace_all("-", " ") %>%  # Replace hyphens with a space
      str_remove_all("[^[:alnum:],.\\s]") %>%  # Remove other punctuation except commas, periods, and spaces
      str_to_lower() %>%
      str_squish(),  # Remove extra spaces
    mname = stri_trans_general(mname, "latin-ascii") %>% 
      str_replace_all("-", " ") %>%  # Replace hyphens with a space
      str_remove_all("[^[:alnum:],.\\s]") %>%  # Remove other punctuation except commas, periods, and spaces
      str_to_lower() %>%
      str_squish(),  # Remove extra spaces
    # Extract first five digits of the zip code
    mloczip_five = str_extract(mloczip, "^\\d{5}")
  )

aha_data <- aha_data %>%
  mutate(ahanumber = str_remove_all(ahanumber, "[A-Za-z]"),
         ahanumber = as.numeric(ahanumber),
         year = as.numeric(year),
         mcrnum = str_remove_all(mcrnum, "[A-Za-z]"),
         mcrnum = as.numeric(mcrnum))

##### finish xwalk clean #####
combined_ha <- xwalk2 %>% 
  mutate(clean_name = entity_name %>%
               str_to_lower() %>%                            # lowercase
               str_replace_all("[[:punct:]]", "") %>%        # remove punctuation
               str_squish() ,                                 # remove extra whitespace
             address = entity_address %>%
               str_to_lower() %>%                            # lowercase
               str_replace_all("[[:punct:]]", "") %>%        # remove punctuation
               str_squish()
) 

## merge with aha
not_na <- combined_ha %>% filter(!is.na(ahanumber_filled)) %>%
  mutate(ahanumber = ahanumber_filled)
merged <- not_na %>% left_join(aha_data, 
                               by = c("ahanumber", "year")) %>%
  mutate(not_in_aha = if_else(is.na(mname) & is.na(mloczip), 1L, 0L))

## step 1 - assign aha id if mname == entity_name
step1 <- merged %>%
  mutate(sys_aha = ahanumber) %>%
  group_by(ahanumber, year) %>%
  mutate(
    # Count how many rows meet the match criteria
    n_match = sum(entity_name == mname & haentitytypeid == 1, na.rm = TRUE),
    
    # Mark the unique matching row (TRUE only if it's the one valid match)
    is_unique_match = (n_match == 1 & entity_name == mname & haentitytypeid == 1)
  ) %>%
  mutate(
    # Assign clean_aha: ahanumber to the unique match, 0 to others in same group if a match exists
    clean_aha = case_when(
      is_unique_match ~ ahanumber,
      n_match == 1 ~ 0,
      TRUE ~ NA_real_
    )
  ) %>% 
  ungroup() %>%
  select(-n_match, -is_unique_match)   %>%
  # if an aha number is assigned for a given year, set all other entity_aha to 0
  group_by(ahanumber,year) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

## step 1 - assign aha id if mloccity == entity_city and is unique

step1b <- step1 %>%
  mutate(mname = as.character(mname), entity_name = as.character(entity_name),
    jw_sim = ifelse(
      !is.na(mname),
      1 - stringdist(entity_name, mname, method = "jw", p = 0.1),
      NA_real_
    )
  ) %>%
  group_by(ahanumber, year) %>%
  mutate(
      valid_jw = jw_sim >= 0.8,
      max_sim = if (any(valid_jw, na.rm = TRUE)) max(jw_sim[valid_jw], na.rm = TRUE) else NA_real_,
      is_best = valid_jw & jw_sim == max_sim,
    # get entities in city
    # Count how many rows meet the match criteria
    n_match = sum(entity_city == mloccity & haentitytypeid == 1, na.rm = TRUE),
    
    # Mark the unique matching row (TRUE only if it's the one valid match)
    is_unique_match = (n_match == 1 & entity_city == mloccity),
    is_unique_match = is_unique_match &
      n_distinct(entity_uniqueid[is_unique_match], na.rm = TRUE) == 1,
  ) %>%
  mutate(
    clean_aha = if (all(is.na(clean_aha))) {
      case_when(
        is_unique_match & haentitytypeid == 1 & jw_sim == max_sim ~ ahanumber,
        is_unique_match & haentitytypeid == 1 & jw_sim != max_sim ~ 0,
        TRUE ~ NA_real_
      )
    } else {
      clean_aha  # leave it unchanged if any clean_aha is non-NA
    }
  ) %>%
  ungroup() %>%
  select(-n_match, -is_unique_match)   %>%
  ungroup() %>%
  group_by(ahanumber,year) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

## step 2 - if an ahanumber (a) has all NA clean_aha numbers and
## (b) has exactly one entity with haentitytypeid == 1 that entity should be
## assigned the ahanumber
step2 <- step1b %>%
  group_by(ahanumber, entity_uniqueid) %>%
  summarise(has_type1 = any(haentitytypeid == "1"), .groups = "drop") %>%
  group_by(ahanumber) %>%
  mutate(
    n_type1_entities = sum(has_type1),
    assign_aha = if_else(n_type1_entities == 1 & has_type1, TRUE, FALSE)
  ) %>%
  filter(assign_aha) %>%
  select(ahanumber, entity_uniqueid) %>% mutate(clean_aha = ahanumber) %>%
  right_join(step1b %>% mutate(ahanumber = sys_aha), by = c("ahanumber", "entity_uniqueid")) %>%
  mutate(clean_aha = coalesce(clean_aha.y, clean_aha.x)) %>%
  select(-clean_aha.x, -clean_aha.y) %>%
  group_by(ahanumber, year) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

## step 3 - assign ids by closest jw distance
step3 <- step2 %>%
  group_by(ahanumber, year) %>%
  mutate(
    n_best_type1 = sum(is_best & haentitytypeid == "1", na.rm = TRUE),
    any_type1 = any(haentitytypeid == "1", na.rm = TRUE),
    
    # find max jw_sim among haentitytypeid == "1" and valid_jw
    max_type1_sim = if (any(valid_jw & haentitytypeid == "1", na.rm = TRUE)) {
      max(jw_sim[valid_jw & haentitytypeid == "1"], na.rm = TRUE)
    } else NA_real_,
    
    is_best_type1 = valid_jw & haentitytypeid == "1" & jw_sim == max_type1_sim,
    is_best_type1 = is_best_type1 & n_distinct(entity_uniqueid[is_best_type1], na.rm = TRUE) == 1
    
  ) %>%
  mutate(
    clean_aha = case_when(

      # get best match across all cases and best match for typeid == 1
      is.na(clean_aha) & is_best & is_best_type1 & haentitytypeid == "1" ~ ahanumber,

      # get best match for typeid == 1, not necessarily across all cases
      is.na(clean_aha) & is_best_type1 ~ ahanumber,
      
      # get best match across all cases if typeid==1 matches are poor
      is.na(clean_aha) & is_best & max_type1_sim < 0.8 & n_best_type1 == 0 ~ ahanumber,
      
      # fill all other cases with 0/NA
      is.na(clean_aha) & valid_jw & !is_best ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup() %>%
  select(-n_best_type1) %>% 
  group_by(ahanumber,year) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

### clean up and backfill ahas
uid_clean <- step3 %>%
  filter(!is.na(clean_aha), clean_aha != 0) %>%
  select(entity_uniqueid, clean_aha_uid = clean_aha) %>%
  distinct()

# Step 2: Get clean_aha values by entity_name
name_clean <- step3 %>%
  filter(haentitytypeid == 1) %>%
  filter(!is.na(clean_aha), clean_aha != 0) %>%
  select(entity_name, clean_aha_name = clean_aha) %>%
  distinct()

# Step 3: Left join both, with entity_uniqueid taking priority
step4 <- step3 %>%
  left_join(uid_clean, by = "entity_uniqueid") %>%
  left_join(name_clean, by = "entity_name") %>%
  group_by(sys_aha, year) %>%
  mutate(
    fillable_group = all(is.na(clean_aha) | clean_aha == 0),
    fillable_group = fillable_group &
      n_distinct(entity_uniqueid[fillable_group], na.rm = TRUE) == 1,
    
    # get num distinct vals and vals for consistency
    uid_u    = n_distinct(clean_aha_uid,  na.rm = TRUE),
    name_u   = n_distinct(clean_aha_name, na.rm = TRUE),
    uid_val  = pluck(na.omit(clean_aha_uid),  1, .default = NA_real_),
    name_val = pluck(na.omit(clean_aha_name), 1, .default = NA_real_),
    
    # both sides are constant (0 or 1 unique non-NA), and if both present, they match
    valid_aha = uid_u <= 1 & name_u <= 1 &
      (uid_u == 0 | name_u == 0 | uid_val == name_val)
  ) %>%
  ungroup() %>%
  group_by(entity_uniqueid) %>%
  mutate(
    # Compute potential fill values
    new_aha = case_when(
      !fillable_group ~ clean_aha,
      is.na(mname)    ~ NA_real_,
      
      # prefer UID when the pair is consistent
      valid_aha & uid_u == 1                 ~ uid_val,
      valid_aha & uid_u == 0 & name_u == 1 & jw_sim >=.85 ~ name_val,
      
      # fallback: prioritize uid, then name, then existing
      TRUE ~ clean_aha #coalesce(na_if(clean_aha_uid, 0),
                     # na_if(clean_aha_name, 0),
                     # clean_aha)
    )
  ) %>%
  ungroup() %>%
  select(-fillable_group) %>%
  group_by(new_aha, year) %>%
  mutate(
    fillable_group = n_distinct(entity_uniqueid[new_aha], na.rm = TRUE) == 1
    ) %>%
  ungroup() %>%
  mutate(
    clean_aha = ifelse(fillable_group, new_aha, clean_aha)
    ) %>% #%>% filter(clean_aha == 6431790) %>% 
  #select(year, sys_aha, clean_aha, new_aha, clean_aha_uid,clean_aha_name, entity_name, mname, uid_u, name_u)
  select(-c(fillable_group,clean_aha_uid, valid_aha, uid_u, name_u, uid_val, 
            name_val)) 

step5 <- step4 %>% group_by(ahanumber, year) %>%
  mutate(
    n_match = sum(entity_name == mname & haentitytypeid == 2, na.rm = TRUE),
    is_unique_match = (n_match == 1 & entity_name == mname & haentitytypeid == 2)
  ) %>% 
  mutate(
    clean_aha = case_when(
      !is.na(clean_aha) ~ clean_aha,
      clean_aha == 0 ~ 0,
      is_unique_match ~ ahanumber,
      n_match == 1 ~ 0,
      TRUE ~ NA_real_
    )
  ) %>%  
  ungroup() %>%
  select(-n_match, -is_unique_match)  %>%
  group_by(ahanumber, year) %>%
  mutate(
    n_match = sum(entity_city == mloccity & haentitytypeid == 2, na.rm = TRUE),
    is_unique_match = (n_match == 1 & entity_city == mloccity & haentitytypeid == 2)
  ) %>%
  mutate(
    clean_aha = if (all(is.na(clean_aha))) {
      case_when(
        is_unique_match ~ ahanumber,
        n_match == 1 ~ 0,
        TRUE ~ NA_real_
      )
    } else {
      clean_aha
    }
  ) %>%
  ungroup() %>%
  select(-n_match, -is_unique_match)   %>%
  ungroup() %>%
  group_by(ahanumber, year) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

step6 <- step5 %>%
  group_by(ahanumber, entity_uniqueid) %>%
  summarise(has_type2 = any(haentitytypeid == "2"), .groups = "drop") %>%
  group_by(ahanumber) %>%
  mutate(
    n_type2_entities = sum(has_type2),
    assign_aha = if_else(n_type2_entities == 1 & has_type2, TRUE, FALSE),
    assign_aha = assign_aha &
      n_distinct(entity_uniqueid[assign_aha], na.rm = TRUE) == 1
  ) %>%
  filter(assign_aha) %>%
  select(ahanumber, entity_uniqueid) %>% mutate(clean_aha = ahanumber) %>%
  right_join(step5 %>% mutate(ahanumber = sys_aha), by = c("ahanumber", "entity_uniqueid")) %>%
  mutate(clean_aha = coalesce(clean_aha.y, clean_aha.x)) %>%
  select(-clean_aha.x, -clean_aha.y) %>%
  group_by(ahanumber, year) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

step7 <- step6 %>%
  group_by(ahanumber, year) %>%
  mutate(
    n_best_type2 = sum(is_best & haentitytypeid == "2", na.rm = TRUE),
    any_type2 = any(haentitytypeid == "2", na.rm = TRUE),
    
    # find max jw_sim among haentitytypeid == "1" and "2" valid_jw
    max_type2_sim = if (any(valid_jw & (haentitytypeid == "1" | haentitytypeid == "2"), na.rm = TRUE)) {
      max(jw_sim[valid_jw & (haentitytypeid == "1" | haentitytypeid == "2")], na.rm = TRUE)
    } else NA_real_,
  
    is_best_hospital = valid_jw & (haentitytypeid == "1" | haentitytypeid == "2") & jw_sim == max_type2_sim,
    is_best_hospital = is_best_hospital & 
      n_distinct(entity_uniqueid[is_best_hospital], na.rm = TRUE) == 1
    
  ) %>%
  mutate(
    clean_aha = case_when(
      is.na(clean_aha) & is_best & is_best_hospital & haentitytypeid == "1" ~ ahanumber,
      is.na(clean_aha) & is_best & is_best_hospital & haentitytypeid == "2" ~ ahanumber,
      is.na(clean_aha) & is_best_hospital ~ ahanumber,
      is.na(clean_aha) & is_best & max_type2_sim < 0.8 & n_best_type2 == 0 ~ ahanumber,
      is.na(clean_aha) & valid_jw & !is_best ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup() %>%
  select( -n_best_type2) %>% 
  group_by(ahanumber ,year ) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

step8 <- step7 %>% arrange(entity_uniqueid, year) %>%
  group_by(sys_aha, year) %>%
  mutate(
    fillable_group = all(is.na(clean_aha) | clean_aha == 0),
    fillable_group = fillable_group &
      n_distinct(entity_uniqueid[fillable_group], na.rm = TRUE) == 1
  ) %>%
  group_by(entity_uniqueid) %>%
  mutate(
    aha_before = zoo::na.locf(clean_aha, na.rm = FALSE),
    aha_after  = zoo::na.locf(clean_aha, fromLast = TRUE, na.rm = FALSE),
    clean_filled = case_when(
      is.na(clean_aha) & fillable_group & 
      !is.na(aha_before) & !is.na(aha_after) &
        aha_before == aha_after ~ aha_before,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup() %>%
  mutate(clean_aha = clean_filled) %>%
  select(-aha_before, -aha_after, -clean_filled)

step9 <- step8 %>%
  group_by(ahanumber, year) %>%
  mutate(still_unfilled = all(is.na(clean_aha)),
         still_unfilled = still_unfilled &
           n_distinct(entity_uniqueid[still_unfilled], na.rm = TRUE) == 1) %>%
  ungroup() %>% 
  arrange(entity_uniqueid, year) %>%  # Ensure correct ordering for locf
  group_by(entity_uniqueid) %>%
  mutate(
    aha_before = zoo::na.locf(clean_aha, na.rm = FALSE),
    aha_after  = zoo::na.locf(clean_aha, fromLast = TRUE, na.rm = FALSE)
  ) %>%
  ungroup() %>%
  group_by(ahanumber, year) %>%
  mutate(
    clean_filled = case_when(
      is.na(clean_aha) & still_unfilled & !is.na(aha_before) & !is.na(aha_after) & aha_before == aha_after ~ aha_before,
      is.na(clean_aha) & still_unfilled & !is.na(aha_before) & is.na(aha_after) ~ aha_before,
      is.na(clean_aha) & still_unfilled & is.na(aha_before) & !is.na(aha_after) ~ aha_after,
      TRUE ~ clean_aha
  )) %>%
  ungroup() %>%
  mutate(clean_aha = clean_filled) %>% select(-clean_filled,-aha_before, -aha_after )

step10 <- step9 %>% 
  group_by(entity_uniqueid) %>%
  mutate(
    aha_before = zoo::na.locf(clean_aha, na.rm = FALSE),
    aha_after  = zoo::na.locf(clean_aha, fromLast = TRUE, na.rm = FALSE)
  ) %>%
  ungroup() %>%
  group_by(ahanumber, year) %>%
  mutate(
    clean_filled = case_when(
      is.na(clean_aha) & !is.na(aha_before) & !is.na(aha_after) & aha_before == aha_after ~ aha_before,
      TRUE ~ clean_aha)) %>%
  ungroup() %>% 
  mutate(clean_aha = clean_filled) %>% select(-clean_filled,-aha_before, -aha_after )

### CHECK OUTPUT
temp_export <- step10 %>% 
  rename(str_ccn_himss = medicarenumber,
         ccn_himss = mcrnum.x,
         ccn_aha = mcrnum.y,
         campus_aha = sys_aha,
         py_fuzzy_flag = fuzzy_flag) %>%
  mutate(
    entity_aha = if_else(clean_aha == 0, NA_real_, clean_aha),
    campus_fuzzy_flag = case_when(
      is.na(py_fuzzy_flag) ~ NA, 
      TRUE ~ !is.na(campus_aha) & py_fuzzy_flag == 1
    ),
    entity_fuzzy_flag = case_when(
      is.na(py_fuzzy_flag) ~ NA, 
      TRUE ~ !is.na(clean_aha) & py_fuzzy_flag == 1
    )
  )


xwalk_export <- temp_export %>%
  distinct(year, entity_uniqueid, entity_name, mname, entity_address, mlocaddr,
           entity_zip, mloczip_five,
           ccn_himss, ccn_aha, campus_aha,entity_fuzzy_flag,campus_fuzzy_flag) %>%
  rename(
         name_himss = entity_name,
         name_aha = mname,
         add_himss = entity_address,
         add_aha = mlocaddr,
         zip_himss = entity_zip,
         zip_aha = mloczip_five) 

write_feather(xwalk_export, paste0(derived_data,'/himss_aha_xwalk.feather'))

# CREATE HIMSS MERGE
export_xwalk <- temp_export %>% distinct(himss_entityid, campus_aha, entity_aha, 
                                         latitude, longitude, 
                                         campus_fuzzy_flag, entity_fuzzy_flag, py_fuzzy_flag ) %>%
  group_by(himss_entityid) %>%
  slice(1) %>% 
  ungroup() %>%
  mutate(ahanumber = campus_aha) %>%
  rename(geo_lat = latitude,
         geo_lon = longitude)

haentity <- read_feather(paste0(auxiliary_data,"/haentity.feather"))

merged_haentity <- haentity %>% select(-ahanumber) %>%
  mutate(himss_entityid = as.numeric(himss_entityid),
         entity_uniqueid = as.numeric(entity_uniqueid),
         year = as.numeric(year)) %>%
  left_join(export_xwalk) %>% 
  select(-any_of(setdiff(names(aha_data), c("year", "ahanumber")))) %>%
  left_join(aha_data) %>%
  mutate(is_hospital = !is.na(entity_aha))

write_feather(merged_haentity, paste0(derived_data,'/himss_aha_hospitals_final.feather'))
merged_haentity <- merged_haentity %>% clean_names() 
#write_dta(merged_haentity, paste0(derived_data,'/himss_aha_hospitals_final.dta'))

## CREATE LEVEL EXPORT
himss <- read_feather(paste0(derived_data, '/final_himss.feather'))
himss_mini <- himss %>% # confirmed
  select(himss_entityid, year, id, entity_uniqueid) %>%
  mutate(
    himss_entityid = as.numeric(himss_entityid),
    year = as.numeric(year)
  )

himss_to_aha_xwalk <- temp_export %>% distinct(himss_entityid, entity_uniqueid, year, 
                                      clean_aha, campus_aha, py_fuzzy_flag, latitude, longitude) %>%
  group_by(himss_entityid) %>%
             slice(1) %>% ungroup() %>%
  mutate(ahanumber = campus_aha)  %>%
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
      TRUE ~ !is.na(clean_aha) & py_fuzzy_flag == 1
    ),
    is_hospital = !is.na(clean_aha),
    entity_aha = if_else(clean_aha == 0, NA_real_, clean_aha))
    

write_feather(final_merged,paste0(derived_data,'/final_aha.feather'))
#write_feather(final_merged,paste0(derived_data,'/final_confirmed_aha_update_530.feather'))

final_merged <- final_merged %>% clean_names() 
write_dta(final_merged,paste0(derived_data, '/final_aha.dta'))
#write_dta(final_merged,paste0(derived_data, '/final_confirmed_aha_update_530.dta'))

final_confirmed <- final_merged %>% filter(confirmed) %>% clean_names() 
write_feather(final_confirmed,paste0(derived_data,'/final_confirmed_aha.feather'))
write_dta(final_confirmed,paste0(derived_data, '/final_confirmed_aha.dta'))


