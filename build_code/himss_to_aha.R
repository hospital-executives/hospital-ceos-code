
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

#### Pull in AHA<>MCR Crosswalk 2 - it should be the case that after updating
#### zip codes that all assigned AHAs in haentityhosp are assigned here
raw_xwalk <- read.csv(paste0(auxiliary_data, "/aha_himss_xwalk_old.csv"))
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

write_feather(aha_data , paste0(auxiliary_data, "/aha_data_clean.feather"))

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

# step 0 - assign if fuzzy_flag = 0
step0 <- merged %>%
  group_by(ahanumber, year) %>%
  mutate(unique_flag = sum(exact_match) == 1) %>%
  ungroup() %>%
  mutate(sys_aha = ahanumber,
         clean_aha = ifelse(exact_match == 1 & !is.na(exact_match) & unique_flag, sys_aha, NA)) %>%
  select(-unique_flag)

## step 1 - assign aha id if mname == entity_name
step1 <- step0 %>%
  #mutate(sys_aha = ahanumber) %>%
  group_by(ahanumber, year) %>%
  mutate(
    # Count how many rows meet the match criteria
    n_match = sum(entity_name == mname & haentitytypeid == 1, na.rm = TRUE),
    
    # Mark the unique matching row (TRUE only if it's the one valid match)
    is_unique_match = (n_match == 1 & entity_name == mname & haentitytypeid == 1),
    
    empty_aha = all(is.na(clean_aha))
  ) %>%
  ungroup() %>%
  mutate(
    # Assign clean_aha: ahanumber to the unique match, 0 to others in same group if a match exists
    clean_aha = case_when(
      is_unique_match & empty_aha ~ ahanumber,
      n_match & empty_aha == 1 ~ 0,
      TRUE ~ clean_aha
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
      valid_jw = jw_sim >= 0.85,
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
  mutate(clean_aha = coalesce(
    clean_aha.y,
    if_else(jw_sim >= 0.85, clean_aha.x, NA)
  )) %>%
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


step4 <- step3 %>%
  mutate(
    entity_name = as.character(entity_name),
    mname = as.character(mname),
    mname_clean   = str_to_lower(str_replace_all(mname, "[[:punct:]]", "")),
    entity_clean  = str_to_lower(str_replace_all(entity_name, "[[:punct:]]", "")),
    name_in_flag  = str_detect(entity_clean, fixed(mname_clean)) | str_detect(mname_clean, fixed(entity_clean))
  ) %>%
  group_by(ahanumber, year) %>%
  mutate(
    fillable_group = all(is.na(clean_aha) | clean_aha == 0 | clean_aha != ahanumber),
    n_hospital_match = sum(name_in_flag & haentitytypeid == 1 & fillable_group, na.rm = TRUE),
    is_unique_match = (n_hospital_match == 1 & name_in_flag & haentitytypeid == 1 & fillable_group)
  ) %>%
  ungroup() %>%
  mutate(
    clean_aha = case_when(
      n_hospital_match == 1 & is_unique_match ~ ahanumber,
      n_hospital_match == 1 & !is_unique_match ~ 0,
      TRUE ~ clean_aha
    )) %>%
  select(-fillable_group) %>% 
  group_by(ahanumber, year) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()


### clean up and backfill ahas
uid_clean <- step4 %>%
  filter(!is.na(clean_aha), clean_aha != 0) %>%
  select(entity_uniqueid, clean_aha_uid = clean_aha) %>%
  distinct()

# Step 2: Get clean_aha values by entity_name
name_clean <- step4 %>%
  filter(haentitytypeid == 1) %>%
  filter(!is.na(clean_aha), clean_aha != 0) %>%
  select(entity_name, clean_aha_name = clean_aha) %>%
  distinct()

# Step 3: Left join both, with entity_uniqueid taking priority
step5 <- step4 %>%
  left_join(uid_clean, by = "entity_uniqueid") %>%
  left_join(name_clean, by = "entity_name") %>%
  group_by(sys_aha, year) %>%
  mutate(
    fillable_group = all(is.na(clean_aha) | clean_aha == 0 | clean_aha != sys_aha),
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

step6 <- step5 %>% arrange(entity_uniqueid, year) %>%
  group_by(sys_aha, year) %>%
  mutate(
    fillable_group = all(is.na(clean_aha) | clean_aha == 0 | clean_aha != sys_aha),
    
    # prioritize haentitytypeid == 1
    n_hospital_match = sum(haentitytypeid == 1 & fillable_group, na.rm = TRUE),
    is_unique_match = (n_hospital_match == 1 & haentitytypeid == 1 & fillable_group),
    
    fillable_group = fillable_group &
      n_distinct(entity_uniqueid[fillable_group], na.rm = TRUE) == 1
  ) %>%
  arrange(entity_uniqueid, year) %>%
  group_by(entity_uniqueid) %>%
  mutate(
    aha_before = zoo::na.locf(clean_aha, na.rm = FALSE),
    aha_after  = zoo::na.locf(clean_aha, fromLast = TRUE, na.rm = FALSE),
    clean_filled = case_when(
      is_unique_match & aha_before == aha_after ~ aha_before,
      is_unique_match & is.na(aha_after) ~ aha_before,
      is_unique_match & is.na(aha_before) ~ aha_after,
      is.na(clean_aha) & fillable_group & !is.na(aha_before) & !is.na(aha_after) & aha_before == aha_after ~ aha_before,
      is.na(clean_aha) & fillable_group & is.na(aha_before) ~ aha_after,
      is.na(clean_aha) & fillable_group & is.na(aha_after) ~ aha_before,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup() %>%
  mutate(clean_aha = clean_filled) %>%
  select(-fillable_group, -aha_before, -aha_after, - clean_filled) %>%
  # clean up rare cases where an entity_aha != sys_aha is assigned
  group_by(clean_aha, year) %>%
  mutate(multiple_assignment_flag = n_distinct(entity_uniqueid) > 1 & !is.na(clean_aha) & clean_aha != 0) %>%
  ungroup() %>%
  group_by(entity_uniqueid) %>%
  mutate(same_name_flag = any(jw_sim == 1)) %>%
  ungroup() %>%
  mutate(clean_aha = case_when(
    !multiple_assignment_flag ~ clean_aha,
    multiple_assignment_flag & same_name_flag & !is.na(same_name_flag) ~ clean_aha,
    multiple_assignment_flag & (!same_name_flag | is.na(same_name_flag)) ~ 0,
    TRUE ~ clean_aha
  )) %>% select(-multiple_assignment_flag, -same_name_flag)

step7 <- step6 %>% group_by(ahanumber, year) %>%
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
        is_unique_match & jw_sim >= .75 ~ ahanumber,
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

step8 <- step7 %>%
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
  right_join(step7 %>% mutate(ahanumber = sys_aha), by = c("ahanumber", "entity_uniqueid")) %>%
  mutate(clean_aha = ifelse(jw_sim >= .85, coalesce(clean_aha.y, clean_aha.x), clean_aha.y)) %>%
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

step9 <- step8 %>%
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

step10 <- step9 %>%
  group_by(ahanumber, year) %>%
  mutate(
    fillable_group = all(is.na(clean_aha) | clean_aha == 0 | clean_aha != ahanumber),
    fillable_group = fillable_group &
      n_distinct(entity_uniqueid[fillable_group], na.rm = TRUE) == 1
  ) %>%
  ungroup() %>%
  mutate(
    clean_aha = case_when(
      fillable_group == 1 & (haentitytypeid <= 3 | haentitytypeid == 6) & jw_sim >= .85 ~ ahanumber,
      TRUE ~ clean_aha
    )) %>%
  select(-fillable_group) %>% 
  group_by(ahanumber, year) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

step11 <- step10 %>% arrange(entity_uniqueid, year) %>%
  group_by(sys_aha, year) %>%
  mutate(
    fillable_group = all(is.na(clean_aha) | clean_aha == 0 | clean_aha != ahanumber),
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

step12 <- step11 %>% # ~ 91k before
  group_by(ahanumber, year) %>%
  mutate(still_unfilled  = all(is.na(clean_aha) | clean_aha == 0 | clean_aha != ahanumber),
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

remove_pattern <- regex(
  "\\b(st\\.?|saint|hosp\\w*|center\\w*|ctr\\w*|health|hlth|system)\\b",
  ignore_case = TRUE
)

step13 <- step12 %>%
  group_by(sys_aha, year) %>%
  mutate(
    to_fill = all(is.na(clean_aha) | clean_aha == 0 | clean_aha != sys_aha),
    num_hosp  = n_distinct(entity_uniqueid[to_fill & haentitytypeid == 1], na.rm = TRUE),
    num_entity = n_distinct(entity_uniqueid[to_fill],                     na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(
    entity_name_clean = str_squish(str_remove_all(entity_name, remove_pattern)),
    mname_clean       = str_squish(str_remove_all(mname,       remove_pattern)),
    clean_sim = stringsim(entity_name_clean, mname_clean, method = "jw", p = 0.1), 
    add_sim = stringsim(entity_address, mlocaddr, method = "jw", p = 0.1), 
    fill_flag = !is.na(mname) & (clean_sim >= .95 | (clean_sim >= .875 & add_sim >= .875)),
  ) %>% 
  group_by(sys_aha, year) %>%
  mutate(will_fill_hospital = n_distinct(entity_uniqueid[fill_flag & haentitytypeid == 1 & num_hosp == 1]) == 1,
         will_fill_entity = n_distinct(entity_uniqueid[fill_flag & num_entity == 1])
  ) %>%
  ungroup() %>%
  mutate(clean_aha = case_when(
    (is.na(clean_aha) | clean_aha == 0) & will_fill_hospital & haentitytypeid == 1 ~ sys_aha,
    to_fill & will_fill_entity == 1 ~ sys_aha,
    TRUE ~ clean_aha
  )) %>% select(- will_fill_hospital, -will_fill_entity)


#### Clean Campus AHA
hospital_df <- step13 %>% 
  # clean + create var names for export
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
  ) %>%
  # subset variables
  distinct(year, campus_aha, entity_aha, entity_uniqueid, himss_entityid,
           haentitytypeid, system_id, sysid,
           entity_name, mname, entity_address, mlocaddr,
           entity_zip, mloczip_five, latitude, longitude, geocoded,
           ccn_himss, ccn_aha,entity_fuzzy_flag,campus_fuzzy_flag, py_fuzzy_flag) 

# create var for "true" system id 
hospital_df <- hospital_df %>% 
  mutate(himss_sysid = as.numeric(system_id),
         entity_aha = as.numeric(entity_aha),
         campus_aha = as.numeric(campus_aha),
         campus_aha = ifelse(campus_aha != entity_aha & !is.na(entity_aha), entity_aha, campus_aha)) %>%
  group_by(entity_aha, year) %>%
  mutate(entity_himss_sys =  ifelse(is.na(entity_aha), NA, himss_sysid)) %>%
  ungroup() %>%
  group_by(campus_aha, year) %>%
  mutate(
    campus_himss_sys = dplyr::first(
      entity_himss_sys[dplyr::near(entity_aha, campus_aha) & !is.na(entity_himss_sys)],
      default = NA_real_
    )
  ) %>% ungroup() %>%
  mutate(real_campus_aha = 
           ifelse(
             himss_sysid == campus_himss_sys, campus_aha, NA))

# matched to campus since himss system ID matches "true" hospital
sys_matches <- hospital_df %>% filter(himss_sysid == campus_himss_sys)

# matched to campus since entity is assigned
assigned_matches <- hospital_df %>% filter(!is.na(campus_aha)) %>%
  filter((!is.na(entity_aha)))

name_matches <-  hospital_df %>% 
  select(campus_aha, entity_aha, entity_name, mname, entity_address, mlocaddr,
         year, entity_uniqueid, haentitytypeid, himss_entityid) %>%
  mutate(
    add_sim = stringsim(entity_address, mlocaddr, method = "jw", p = 0.1), 
    name_sim = stringsim(entity_name, mname, method = "jw", p = 0.1),
    real_campus_aha = ifelse(name_sim >= .85, campus_aha, NA)) %>% 
  filter(is.na(entity_aha) & real_campus_aha == campus_aha & !is.na(campus_aha))

## aggregate all currently matched
curr_matches <- rbind(assigned_matches %>% 
                        distinct(entity_uniqueid, year, campus_aha, himss_entityid),
                      name_matches %>% 
                        distinct(entity_uniqueid, year,campus_aha, himss_entityid),
                      sys_matches %>% 
                        distinct(entity_uniqueid, year,campus_aha, himss_entityid)) %>%
  distinct()

## matched on name + address similarity (lower name threshold)
add_name_matches <- hospital_df %>% filter(!is.na(campus_aha)) %>% 
  anti_join(curr_matches)  %>% 
  select(campus_aha, entity_aha, entity_name, mname, entity_address, mlocaddr,
         year, entity_uniqueid, haentitytypeid, himss_entityid) %>%  
  mutate(
           add_sim = stringsim(entity_address, mlocaddr, method = "jw", p = 0.1), 
           name_sim = stringsim(entity_name, mname, method = "jw", p = 0.1),
           real_campus_aha = ifelse(add_sim >= .95 & name_sim >= .75, campus_aha, NA)) %>% 
  filter(is.na(entity_aha) & real_campus_aha == campus_aha & !is.na(campus_aha))

post_add_matches <- rbind(curr_matches, 
                          add_name_matches %>% 
                            distinct(entity_uniqueid, year, campus_aha, himss_entityid)) %>%
  distinct()

## matched on name (lower name threshold) if campus_aha not assigned via geocoding
not_geocoded <- hospital_df %>% filter(!is.na(campus_aha)) %>%
  anti_join(post_add_matches) %>%
  filter(geocoded == "False") %>% 
  select(mname, entity_name, mlocaddr, entity_address, 
         entity_aha, year, entity_uniqueid, campus_aha, himss_entityid) %>%
  mutate(mname = as.character(mname), entity_name = as.character(entity_name),
         jw_sim = ifelse(
           !is.na(mname),
           1 - stringdist(entity_name, mname, method = "jw", p = 0.1),
           NA_real_
         )
  ) %>% filter(jw_sim >= .75)
post_geo_matches <- rbind(post_add_matches, 
                          not_geocoded %>% 
                            distinct(entity_uniqueid, year, campus_aha, himss_entityid)) %>%
  distinct()

## match on additional name similarities
name_sim_matches <- hospital_df %>% filter(!is.na(campus_aha)) %>%
  anti_join(post_geo_matches) %>%
  left_join(aha_data) %>%
  distinct(entity_name, mname, campus_aha, entity_aha, mlocaddr, entity_address, 
           mloccity, himss_sysid, campus_himss_sys, year, entity_uniqueid,
           himss_entityid)  %>%  
  mutate(
    entity_name = as.character(entity_name),
    mname = as.character(mname),
    
    # tokenize into words (letters/numbers only, lowercased)
    tokens1 = str_split(str_squish(str_to_lower(str_replace_all(entity_name, "[^\\p{L}\\p{N}]+", " "))), " "),
    tokens2 = str_split(str_squish(str_to_lower(str_replace_all(mname, "[^\\p{L}\\p{N}]+", " "))), " "),
    
    # choose "first word", skipping saint variants
    first_himss = map_chr(tokens1, ~{
      if (length(.x) == 0) return(NA_character_)
      if (.x[1] %in% c("st","st.","saint") && length(.x) > 1) .x[2] else .x[1]
    }),
    first_aha = map_chr(tokens2, ~{
      if (length(.x) == 0) return(NA_character_)
      if (.x[1] %in% c("st","st.","saint") && length(.x) > 1) .x[2] else .x[1]
    }),
    
    # JW similarity and flag
    jw_first = stringsim(first_aha, first_himss, method = "jw"),
    jw_first_flag  = !is.na(jw_first) & jw_first >= .95,
    
    # create variables and flag for mname is in entity_name
    mname_clean   = str_to_lower(str_replace_all(mname, "[[:punct:]]", "")),
    entity_clean  = str_to_lower(str_replace_all(entity_name, "[[:punct:]]", "")),
    name_in_flag  = str_detect(entity_clean, fixed(mname_clean))
  ) %>% filter(jw_first_flag|name_in_flag)

post_sim_matches <- rbind(post_geo_matches, 
                          name_sim_matches %>% 
                            distinct(entity_uniqueid, year, campus_aha, himss_entityid)) %>%
  distinct()

cleaned_hospitals <- hospital_df %>%
  left_join(post_sim_matches %>% rename(real_campus_aha = campus_aha)) %>%
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
  distinct(entity_name, mname, campus_aha, entity_aha, mlocaddr, entity_address,
           real_campus_aha, year, entity_uniqueid, himss_entityid) %>% 
  arrange(real_campus_aha, entity_uniqueid)

post_entity_matches <- rbind(post_sim_matches, 
                             cleaned_hospitals %>% 
                             distinct(entity_uniqueid, year, real_campus_aha, himss_entityid) %>%
                             rename(campus_aha = real_campus_aha)) %>%
  distinct()

### FINAL DF WITH CLEANED CAMPUS_AHA
xwalk_export <- hospital_df %>%
  rename(unfiltered_campus_aha = campus_aha) %>%
  left_join(post_entity_matches) %>%
  mutate(haentitytypeid = as.numeric(haentitytypeid),
        campus_aha = ifelse(haentitytypeid <= 3 | haentitytypeid == 6,campus_aha,NA)) %>%
  distinct(year, himss_entityid, himss_sysid, unfiltered_campus_aha, campus_aha, entity_aha,
           entity_uniqueid, entity_name, mname, entity_address, mlocaddr,
           entity_zip, mloczip_five, latitude, longitude,
           ccn_himss, ccn_aha,entity_fuzzy_flag,campus_fuzzy_flag, py_fuzzy_flag,
           haentitytypeid) %>%
  rename(
         name_himss = entity_name,
         name_aha = mname,
         add_himss = entity_address,
         add_aha = mlocaddr,
         zip_himss = entity_zip,
         zip_aha = mloczip_five) 

sys_xwalk <- xwalk_export %>% distinct(himss_sysid, year, campus_aha)

final_export <- xwalk_export %>% 
  left_join(sys_xwalk, by = c("himss_sysid", "year")) %>%
  filter(unfiltered_campus_aha == campus_aha.y) %>%
  mutate(entity_condition = haentitytypeid <= 3 | haentitytypeid == 6, 
         campus_aha = case_when(
           unfiltered_campus_aha == campus_aha.y & entity_condition ~ unfiltered_campus_aha,
           TRUE ~ campus_aha.x
         )) %>%
  select(-campus_aha.x, -campus_aha.y, -entity_condition) %>%
  distinct()

write_feather(final_export, paste0(derived_data,'/himss_aha_xwalk.feather'))

