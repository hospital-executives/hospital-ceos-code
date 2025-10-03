# clean_data.R

library(rstudioapi)
# load data
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("config.R")
  code_path <- code_directory
  data_path <- data_file_path
  himss_path <- paste0(derived_data, "/himss_entities_contacts_0517_v1.feather")
} else {
  args <- commandArgs(trailingOnly = TRUE)
  source("config.R")
  args <- commandArgs(trailingOnly = TRUE)
  code_path <- args[1]
  data_path <- paste0(args[2], "/_data/")
  himss_path <- args[3]
}

cat(paste0("CODE PATH: ", code_path))
cat(paste0("DATA PATH: ", data_path))
cat(paste0("HIMSS PATH: ", himss_path))

##### load files ##### 
df <- read_feather(paste0(himss_path)) %>%
  filter(year > 2008) %>%
  mutate(full_name = str_to_lower(str_replace_all(paste0(firstname, lastname), 
                                                  "[[:punct:]\\s]", ""))) %>%
  filter(!grepl("[A-Za-z]", entity_zip) & 
           !str_detect(entity_state, "AB|BC|PE|NB|NL|NS|MB|SK|PR")) 

variable_columns <- c("contact_uniqueid", "full_name", "firstname", "lastname", 
                      "entity_name", "entity_zip", "entity_state", "system_id", 
                      "year", "title", "phone", 'entity_address', 
                      'entity_uniqueid')
to_clean <- df[, c("id", variable_columns)]
to_merge <- df[, c("id", setdiff(names(df), c("id", variable_columns)))]

# set up one id m names
normalized_levenshtein_cutoff <- 0.3 # default is 0.3
source(paste0(code_path,"/", "helper_scripts/one_id_m_names_final.R"))
# set up one name m ids
source(paste0(code_path,"/", "helper_scripts/one_name_m_ids_final.R"))

########## algorithm(s) to consolidate "multiple" names ########## 

## runs basic algorithm to identify when multiple "names" 
## correspond to the same person 
## run time ~ 3.5 mins
one_id_m_names_output1 <- manual_one_id_m_names(to_clean) 
confirmed <- one_id_m_names_output1$confirmed
remaining <- one_id_m_names_output1$remaining 

## runs algorithm to reassign ids
## < 15 sec
one_id_m_names_output2 <- matching_one_id_m_names(df, confirmed, remaining) 
remaining2 <- one_id_m_names_output2$remaining
updated_ids <- one_id_m_names_output2$updated_ids

## update ids in confirmed data frame
# create df of confirmed observations with old ids
to_be_confirmed <- df %>%
  anti_join(remaining2)

# separate into ids that are ok vs require updating
ok <- to_be_confirmed %>%
  filter(!full_name %in% remaining$full_name &
           !contact_uniqueid %in% remaining$contact_uniqueid)
to_fix <- to_be_confirmed %>%
  filter(full_name %in% remaining$full_name |
           contact_uniqueid %in% remaining$contact_uniqueid)
fixed <- update_ids(to_fix)

# combined dfs to create confirmed df
confirmed_pt1 <- bind_rows(ok, fixed)
remaining_pt1 <- remaining2
outliers <- remaining_pt1

outliers <- outliers %>%
  select(-c(add_list, entity_name_list, entityid_list))
write.csv(outliers, paste0(data_path, "derived/auxiliary/outliers.csv"), row.names = FALSE)


all_objects <- ls()
objects_to_keep <- c("df", "outliers", "code_path", "data_path", "to_clean", 
                     "to_merge")
objects_to_remove <- setdiff(all_objects, objects_to_keep)
rm(list = objects_to_remove)
source(paste0(code_path,"/", "helper_scripts/one_name_m_ids_final.R"))

########## algorithm(s) to consolidate "multiple" ids ########## 

## core matching algorithm
one_name_m_ids_output1 <- matching_one_name_m_ids(to_clean, outliers) 
one_m_remaining1 <- one_name_m_ids_output1$remaining
one_m_confirmed1 <- one_name_m_ids_output1$confirmed

## zip code algorithm - omitted
# zip_output <- zip_algo(one_m_remaining1, one_m_confirmed1, outliers) 
# zip_confirmed <- zip_output$confirmed
# zip_remaining <- zip_output$remaining

## frequency algorithm
# generate frequencies
frequency_inputs <- frequency_processing(one_m_confirmed1, one_m_remaining1, outliers)
frequency_df <- frequency_inputs$frequency_df
quantile_sample <- frequency_inputs$quantile_sample
one_id <- frequency_inputs$one_id
mult_id <- frequency_inputs$mult_id

# separate into state categories
one_state <- mult_id %>%
  filter(!mult_states & num_states == 1)
one_state_per_year <- mult_id %>%
  filter(!mult_states & num_states > 1)
mult_state_per_year <- mult_id %>%
  filter(mult_states)

# run frequency matches
one_state_output <- group_by_quantile(one_state, first_percentile_1 = 0.75, 
                                      last_percentile_1 = 0.5,
                                      first_percentile_2 = 0.9,
                                      last_percentile_2 = 0.25)
one_state_remaining <- one_state_output$remaining
one_state_confirmed <- one_state_output$confirmed

one_state_per_year_output <- group_by_quantile(one_state_per_year, 
                                               first_percentile_1 = 0.75, 
                                               last_percentile_1 = 0.5)
one_state_per_year_remaining <- one_state_per_year_output$remaining
one_state_per_year_confirmed <- one_state_per_year_output$confirmed

mult_state_per_year_output <- group_by_quantile(mult_state_per_year, 
                                                first_percentile_1 = 0.15, 
                                                last_percentile_1 = 0.1)
mult_state_per_year_remaining <- mult_state_per_year_output$remaining
mult_state_per_year_confirmed <- mult_state_per_year_output$confirmed

## generate final dataframes
all_objects <- ls()
objects_to_keep <- c("df", "outliers", "code_path", 'data_path',
                     "one_m_confirmed1", "one_id", "one_state_confirmed", 
                     "one_state_per_year_confirmed",
                     "mult_state_per_year_confirmed", "one_state_remaining",
                     "one_state_per_year_remaining",
                     "mult_state_per_year_remaining", "mult_id","to_merge")
objects_to_remove <- setdiff(all_objects, objects_to_keep)
rm(list = objects_to_remove)

outliers$contact_uniqueid <- as.character(outliers$contact_uniqueid)
one_ids_clean <- one_id %>%
  anti_join(outliers, by = "contact_uniqueid")
one_ids_remaining <- one_id %>%
  filter(contact_uniqueid %in% outliers$contact_uniqueid)

final_confirmed <- bind_rows(one_m_confirmed1, one_ids_clean,
                             one_state_confirmed, one_state_per_year_confirmed,
                             mult_state_per_year_confirmed) %>%
  left_join(to_merge, by = "id")
final_remaining <- bind_rows(one_ids_remaining, one_state_remaining,
                             one_state_per_year_remaining,
                             mult_state_per_year_remaining) %>%
  left_join(to_merge, by = "id")

one_name_m_ids_remaining <- final_remaining %>% #18,979 remaining
  group_by(full_name) %>%
  filter(num_ids>1)

final_confirmed <- final_confirmed %>%
  select(-c(add_list, entity_name_list, entityid_list)) %>%
  mutate(nan_flag = ifelse(firstname == "Nan", TRUE, FALSE),
         firstname = ifelse(nan_flag, "Nancy", FALSE))
write_feather(final_confirmed, paste0(data_path,"derived/auxiliary/", "r_confirmed.feather"))

final_remaining <- final_remaining %>%
  select(-c(add_list, entity_name_list, entityid_list)) %>%
  mutate(nan_flag = ifelse(firstname == "Nan", TRUE, FALSE),
         firstname = ifelse(nan_flag, "Nancy", FALSE))
write_feather(final_remaining, paste0(data_path,"derived/auxiliary/", "r_remaining.feather"))

