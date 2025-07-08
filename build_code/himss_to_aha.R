## load and format data
library(dplyr)
library(stringr)
library(janitor)


#### Pull in AHA<>MCR Crosswalk 2 - it should be the case that after updating
#### zip codes that all assigned AHAs in haentityhosp are assigned here
raw_xwalk <- read.csv(paste0(auxiliary_data, "/xwalk_updated_529.csv"))
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

# set file path using manual input at beginning of script
file_path <- paste0(supplemental_data,"/",file_name_aha)
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
  #filter(entity_uniqueid == 10349) %>%
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
  ) %>% #select( clean_aha, sys_aha, year, mname, entity_name, entity_address, address)
  ungroup() %>%
  select(-n_match, -is_unique_match)   %>%
  group_by(entity_uniqueid) %>%
  mutate(
    override_aha = if (any(!is.na(clean_aha) & clean_aha != 0)) {
      # use the first non-zero, non-NA clean_aha
      clean_aha[which(!is.na(clean_aha) & clean_aha != 0)[1]]
    } else {
      NA_real_
    },
    clean_aha = override_aha
  ) %>% 
  ungroup() %>%
  select(-override_aha) %>% 
  group_by(ahanumber) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

step1b <- step1 %>%
  group_by(ahanumber, year) %>%
  mutate(
    # Count how many rows meet the match criteria
    n_match = sum(entity_city == mloccity & haentitytypeid == 1, na.rm = TRUE),
    
    # Mark the unique matching row (TRUE only if it's the one valid match)
    is_unique_match = (n_match == 1 & entity_city == mloccity & haentitytypeid == 1)
  ) %>%
  mutate(
    clean_aha = if (all(is.na(clean_aha))) {
      case_when(
        is_unique_match ~ ahanumber,
        n_match == 1 ~ 0,
        TRUE ~ NA_real_
      )
    } else {
      clean_aha  # leave it unchanged if any clean_aha is non-NA
    }
  ) %>%
  ungroup() %>%
  select(-n_match, -is_unique_match)   %>%
  group_by(entity_uniqueid) %>%
  mutate(
    override_aha = if (any(!is.na(clean_aha) & clean_aha != 0)) {
      # use the first non-zero, non-NA clean_aha
      clean_aha[which(!is.na(clean_aha) & clean_aha != 0)[1]]
    } else {
      NA_real_
    },
    clean_aha = override_aha
  ) %>%
  ungroup() %>%
  select(-override_aha) %>% 
  group_by(ahanumber) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

check <- step1 %>% group_by(ahanumber) %>%
  filter(any(!is.na(clean_aha))) %>%
  distinct(ahanumber, clean_aha, entity_uniqueid, entity_name, mname, haentitytypeid) 

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
  group_by(entity_uniqueid) %>%
  mutate(
    override_aha = if (any(!is.na(clean_aha) & clean_aha != 0)) {
      # use the first non-zero, non-NA clean_aha
      clean_aha[which(!is.na(clean_aha) & clean_aha != 0)[1]]
    } else {
      NA_real_
    },
    clean_aha = override_aha
  ) %>%
  ungroup() %>%
  select(-override_aha) %>% 
  group_by(ahanumber) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

check <- step2 %>% group_by(ahanumber) %>%
  filter(any(!is.na(clean_aha))) %>%
  distinct(ahanumber, clean_aha, entity_uniqueid, entity_name, mname, haentitytypeid) 

library(dplyr)
library(stringdist)

## step 3 - assign ids by if closest jw distance

step3 <- step2 %>%
  mutate(mname = as.character(mname), entity_name = as.character(entity_name)) %>%
  mutate(
    jw_sim = ifelse(
      !is.na(mname),
      1 - stringdist::stringdist(entity_name, mname, method = "jw", p = 0.1),
      NA_real_
    )
  ) %>%
  #group_by(a)
  group_by(ahanumber) %>%
  mutate(
    valid_jw = jw_sim >= 0.8,
    max_sim = if (any(valid_jw, na.rm = TRUE)) max(jw_sim[valid_jw], na.rm = TRUE) else NA_real_,
    is_best = valid_jw & jw_sim == max_sim,
    n_best_type1 = sum(is_best & haentitytypeid == "1", na.rm = TRUE),
    any_type1 = any(haentitytypeid == "1", na.rm = TRUE)
  ) %>%
  mutate(
    clean_aha = case_when(
      is.na(clean_aha) & is_best & haentitytypeid == "1" & n_best_type1 == 1 ~ ahanumber,
      is.na(clean_aha) & is_best & n_best_type1 == 0 ~ ahanumber,  # no haentitytypeid == 1 match
      is.na(clean_aha) & valid_jw & !is_best ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup() %>%
  select(-jw_sim, -valid_jw, -max_sim, -is_best, -n_best_type1) %>% 
  group_by(entity_uniqueid) %>%
  mutate(
    override_aha = if (any(!is.na(clean_aha) & clean_aha != 0)) {
      # use the first non-zero, non-NA clean_aha
      clean_aha[which(!is.na(clean_aha) & clean_aha != 0)[1]]
    } else {
      NA_real_
    },
    clean_aha = override_aha
  ) %>%
  ungroup() %>%
  select(-override_aha) %>% 
  group_by(ahanumber) %>%
  mutate(
    has_match = any(clean_aha == sys_aha, na.rm = TRUE),
    clean_aha = case_when(
      has_match & is.na(clean_aha) ~ 0,
      TRUE ~ clean_aha
    )
  ) %>%
  ungroup()

check <- step3 %>% group_by(ahanumber) %>%
  filter(any(!is.na(clean_aha))) %>%
  distinct(ahanumber, clean_aha, entity_uniqueid, entity_name, mname, haentitytypeid) 
cat(n_distinct(check$entity_uniqueid))

remaining <- step3 %>% group_by(ahanumber) %>%
  filter(all(is.na(clean_aha))) %>%
  distinct(year, ahanumber, clean_aha, entity_uniqueid, entity_name, mname, haentitytypeid,
           madmin, latitude_aha) 
cat(n_distinct(remaining$entity_uniqueid))

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
  #left_join(name_clean, by = "entity_name") %>%
  group_by(entity_uniqueid) %>%
  mutate(
    clean_aha = case_when(
      is.na(mname) ~ NA_real_,
      TRUE ~ {
        vals <- c(clean_aha, clean_aha_uid) #, clean_aha_name)
        if (any(vals != 0, na.rm = TRUE)) {
          # Prefer the first non-zero value
          vals[which(vals != 0 & !is.na(vals))[1]]
        } else {
          # Fall back to first non-NA (even if it's zero)
          coalesce(clean_aha, clean_aha_uid) #, clean_aha_name)
        }
      }
    )
  ) %>%
  select(-clean_aha_uid) #, -clean_aha_name) #%>%
  #mutate(
  #  clean_aha = if_else(!is.na(clean_aha) & clean_aha != 0 & clean_aha != ahanumber, 0, clean_aha)
  #) 
remove_terms <- "\\b(center|ctr|health|hlth|care|healthcare|system|clinic|hospital|university|rehabilitation)\\b"

check_dropped <- step4 %>%
  group_by(sys_aha) %>%
  mutate(all_zero = all(clean_aha == 0, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(
    entity_name_clean = str_squish(str_remove_all(str_to_lower(entity_name), remove_terms)),
    mname_clean = str_squish(str_remove_all(str_to_lower(mname), remove_terms))
  ) %>%
  filter(all_zero & !is.na(mname)) %>%
  filter(stringdist::stringdist(entity_name_clean, mname_clean, method = "jw") <= 0.25) %>%  # similarity > 0.5
  #filter(stringdist::stringdist(entity_address, mlocaddr, method = "jw") <= 0.25 |
         #  entity_zip_five == mloczip_five) %>%  
  distinct(entity_uniqueid, entity_name, mname, entity_address, mlocaddr, haentitytypeid, sys_aha)
  


### CHECK OUTPUT
temp_export <- step4 %>% filter(clean_aha != 0) %>% 
  rename(str_ccn_himss = medicarenumber,
         ccn_himss = mcrnum.x,
         ccn_aha = mcrnum.y,
         campus_aha = sys_aha,
         py_fuzzy_flag = fuzzy_flag) %>%
  mutate(
    campus_fuzzy_flag = case_when(
      is.na(py_fuzzy_flag) ~ NA, 
      TRUE ~ !is.na(campus_aha) & py_fuzzy_flag == 1
    ),
    entity_fuzzy_flag = case_when(
      is.na(py_fuzzy_flag) ~ NA, 
      TRUE ~ !is.na(clean_aha) & py_fuzzy_flag == 1
    )
  )


write_feather(temp_export, paste0(derived_data,'/himss_aha_hospitals_final.feather'))

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


library(janitor)

temp_export <- temp_export %>%
  clean_names() 
write_dta(temp_export, paste0(derived_data,'/himss_aha_hospitals_final.dta'))


## CREATE INDIVIDUAL LEVEL EXPORT
himss <- read_feather(paste0(derived_data, '/final_himss.feather'))
himss_mini <- himss %>%
  select(himss_entityid, year, id, entity_uniqueid) %>%
  mutate(
    himss_entityid = as.numeric(himss_entityid),
    year = as.numeric(year)
  )

himss_to_aha_xwalk <- temp_export %>% distinct(himss_entityid, year, clean_aha, campus_aha, py_fuzzy_flag) %>%
  mutate(ahanumber = clean_aha)

library(data.table)

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
    aha_data,
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
    )
  )

write_feather(final_merged,paste0(derived_data,'/final_confirmed_aha_final.feather'))
#write_feather(final_merged,paste0(derived_data,'/final_confirmed_aha_update_530.feather'))

library(janitor)

final_merged <- final_merged %>%
  clean_names() 
write_dta(final_merged,paste0(derived_data, '/final_confirmed_aha_final.dta'))
#write_dta(final_merged,paste0(derived_data, '/final_confirmed_aha_update_530.dta'))



## need to fix this case study: 
# case study 1: st. andrews home health
# case study 2: sturgis regional hospital
# case study 3: candler county hospital
# case study 4: dana farber - AHA: 6140583
# case study 5: aha 653032
test <- step3 %>%
  filter(ahanumber == 6140583) %>%
  #filter(str_detect(entity_name, regex("dana farber cancer institute", ignore_case = TRUE))) %>%
  select(year,clean_aha, ahanumber, entity_uniqueid,  mname, entity_name, entity_address, mlocaddr, 
         haentitytypeid, entity_city, mloccity, jw_sim, valid_jw, max_sim, is_best, n_best_type1)

check2 <- step2%>% 
  filter(ahanumber == 6110160) %>%
  select(haentitytypeid, clean_aha, entity_uniqueid, mname, entity_name, 
         entity_address, mlocaddr, entity_city, mloccity)

test <- step4 %>%
  #filter(str_detect(entity_name, regex("dana farber cancer institute", ignore_case = TRUE))) %>%
  distinct(year,clean_aha, ahanumber, entity_uniqueid,  mname, entity_name, entity_address, mlocaddr, 
         haentitytypeid, entity_city, mloccity)

jw <- test %>%
  mutate(
    jw_dist = stringdist(entity_name, mname, method = "jw", p = 0.1)
  ) %>%
   filter(clean_aha != 0)

find <- test %>% filter(entity_city == "Bradford")


##### to remove ###### 
confirm <- merged %>%
  group_by(ahanumber) %>%
  filter(n_distinct(entity_uniqueid) > 1 & n_distinct(entity_name) > 1) %>%
  filter(any(haentitytypeid %in% c("5", "7", "9", "10"))) %>%
  ungroup() %>%
  distinct(ahanumber, entity_uniqueid, entity_name, mname, haentitytypeid) %>%
  mutate(haentitytypeid = as.numeric(haentitytypeid)) %>%
  arrange(ahanumber, haentitytypeid)
cat(n_distinct(test$entity_uniqueid))
  
  # Step 1: Set of all AHA numbers before filtering
  all_ahas <- unique(merged$ahanumber)

# Initialize a list to store results
removed_ahas_by_type <- list()

# Loop over haentitytypeid values from 1 to 10
for (type_id in 1:10) {
  
  # Exclude this type
  filtered_df <- merged %>% filter(haentitytypeid != type_id)
  remaining_ahas <- unique(filtered_df$ahanumber)
  
  # Which AHA numbers are lost?
  removed_ahas <- setdiff(all_ahas, remaining_ahas)
  
  # Save results
  removed_ahas_by_type[[as.character(type_id)]] <- list(
    count_removed = length(removed_ahas),
    ahanumbers_removed = removed_ahas
  )
}

# View how many AHA numbers are removed per type
sapply(removed_ahas_by_type, function(x) x$count_removed)
