
old <- read_feather('/Users/loaner/Dropbox/hospital_ceos/_data/derived/final_confirmed_aha_update.feather')

old_ids <- old %>%
  filter(!is.na(mname)) %>%
  select(id, old_ahanumber = ahanumber, mname)

## If not NA mname want to check for discrepancies
not_na <- final_merged %>% filter(!is.na(mname), id %in% old_ids$id) %>%
  left_join(old_ids, by = "id") %>%
  select(entity_uniqueid, entity_name, mname, entity_address, mlocaddr, year,
         haentitytypeid, sys_aha, ahanumber, old_ahanumber, entity_zip)

discrepancices <- not_na %>%
  filter(ahanumber != old_ahanumber & mname != entity_name) %>% distinct()

## flagged - 54816,54830

# Step 2: Filter final_merged for NA mname and matched IDs
check <- final_merged %>% 
  filter(is.na(mname), id %in% old_ids$id) %>% #, !entity_uniqueid %in% test$entity_uniqueid) %>%
  select(-mname) %>%
  left_join(old_ids, by = "id") %>%
  select(entity_uniqueid, entity_name, mname, entity_address, year,
         haentitytypeid, sys_aha, ahanumber, old_ahanumber, entity_zip)

## two kinds of discrepancies


type_one <- check %>% filter(!is.na(ahanumber) & ahanumber != old_ahanumber)
type_two <- check %>% filter(is.na(ahanumber) & !is.na(old_ahanumber))

# Sample 5 unique ahanumber values
sampled_ahas <- sample(unique(check$old_ahanumber), 10)
sampled_df <- check %>% filter(old_ahanumber %in% sampled_ahas)

## to check later: 35606, 35608
## never assigned: 31113,31114,14012
## 34967 changes aha numbers x3 => need to adjust to account for this

verify <- step1 %>%
 # filter(sys_aha == 6440027) %>%
  filter(entity_uniqueid %in% c(34967)) %>%
  #filter(entity_name == "hutzel womens hospital") %>% 
  select(entity_uniqueid, entity_name, haentitytypeid, year,filled_aha, clean_aha, sys_aha)

debug <- final_merged %>%   filter(entity_uniqueid %in% c(11958,11961)) %>% 
  select(entity_uniqueid,entity_name, mname, year, sys_aha, clean_aha) #, clean_aha, sys_aha)


verify_output <- check %>%
  filter(entity_uniqueid == 11958 & old_ahanumber == 6410027) 

