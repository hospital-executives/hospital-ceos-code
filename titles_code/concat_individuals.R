rm(list = ls())
library(rstudioapi)
library(purrr)
library(janitor)
library(haven)

# load data
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("../build_code/config.R")
  hospitals <- read_feather(paste0(derived_data, "/hospitals_with_xwalk.feather"))
  individuals <- read_feather(paste0(derived_data, "/individuals_final.feather"))
  supp_path <- supplemental_data
  output_dir <- paste0(data_file_path, "/summary_stats/execs")
} else {
  args <- commandArgs(trailingOnly = TRUE)
  source("../build_code/config.R")
  hospitals <- read_feather(args[1])
  individuals <- read_feather(args[2])
  supp_path <- args[3]
  output_dir <- args[4] 
}

### get info for hosp sample only
hospital_xwalk <- read_stata(paste0(derived_data, "/temp/merged_ma_sysid_xwalk.dta"))
type_df <- read_stata(paste0(derived_data, "/temp/merged_ma_nonharmonized.dta")) %>%
  distinct(entity_uniqueid, year, type)

#### load necessary himss data ###
folders <- list.dirs(path = raw_data, full.names = TRUE, recursive = FALSE)

year_range <- 2005:2017 
file_prefixes <- c("HAEntityContact_", "HAEntity_", "ContactSource_")

# Loop through each file prefix
for (prefix in file_prefixes) {
  temp_dfs <- list()  # Temporary list to store dataframes for this prefix
  
  # Loop through each year
  for (year in year_range) {
    if (year == 2017){
      file_path <- paste0("dbo", prefix, year, ".csv")
    } else {
      file_path <- paste0(prefix, year, ".csv")  # Construct the file name
    }
    full_path <- file.path(raw_data, as.character(year), file_path)  # Full file path
    
    #Check if the file exists, import
    if (file.exists(full_path)) {
      #Importing all as character. 
      #There are some differences within year 
      #(e.g. most phone numbers will be ##########, but some will be ###-###-####. 
      #Without converting to character, we see data loss when the mismatched value is not imported 
      #e.g. ###-###-#### would be NA
      df <- read_csv(full_path, show_col_types = FALSE, col_types = cols(.default = col_character())) %>%
        # Add a year value and and make all vars lowercase
        mutate(year = year) %>%  # Add the year as a new column
        rename_with(tolower)
      
      #add a comment
      # In some years the same fields data storage type changed. Dynamically adjust data types for combination: Convert numeric and logical columns to character.
      # df <- df %>%
      #   mutate(across(where(is.numeric), ~if_else(is.na(.), as.character(.), as.character(.)))) %>%
      #   mutate(across(where(is.logical), as.character))
      
      temp_dfs[[length(temp_dfs) + 1]] <- df
    }
  }
  
  # Combine all dataframes, now with consistent data types
  combined_df <- bind_rows(temp_dfs)
  
  # Dynamically assign the combined dataframe to a variable in the global environment
  var_name <- tolower(str_remove(prefix, "_$"))
  assign(var_name, combined_df, envir = .GlobalEnv)
}

#### clean vacancies data ####
normalize_title_std <- function(x) {
  x %>%
    as.character() %>%
    str_replace_all("\u00A0", " ") %>%        # NBSP -> space
    str_replace_all("[[:space:]]+", " ") %>%  # collapse whitespace
    str_squish()
}

vacancies_df <- haentitycontact %>% 
  rename(contactsourceid = hacontactsourceid) %>%
  left_join(contactsource, by = c("contactsourceid", "year")) %>%
  left_join(haentity %>% 
              rename(entity_uniqueid = uniqueid) %>%
              distinct(entity_uniqueid, year, haentityid), by = c("year", "haentityid")) %>%
  distinct(entity_uniqueid, year, status, name) %>%
  filter(!is.na(name)) %>%
  mutate(
    name = case_when(
      name == "CEO"                 ~ "CEO:  Chief Executive Officer",
      name == "CFO"                 ~ "CFO:  Chief Financial Officer",
      name == "COO"                 ~ "COO:  Chief Operating Officer",
      name == "CIO"                 ~ "CIO:  Chief Information Officer",
      name == "CTO - Dir of Tech"   ~ "Director of Technology",
      name == "IT Security Officer" ~ "CSIO/IT Security Officer",
      TRUE                          ~ name
    )
  ) %>%
  mutate(name = normalize_title_std(name))

### first merge data together
individuals_mini <- individuals %>% 
  distinct(contact_uniqueid, confirmed, firstname, lastname, full_name, title_standardized, title, entity_uniqueid, year) %>%
  mutate(
    title_lower = tolower(gsub("[[:punct:]]", "", title)),
    char_ceo = str_detect(title_lower, "ceo|chief executive officer| c e o"),
    title_standardized_key = normalize_title_std(title_standardized)
  ) %>%
  group_by(entity_uniqueid, year) %>%
  mutate(has_ceo = any(title_standardized_key == "CEO: Chief Executive Officer")) %>%
  ungroup() %>%
  mutate(flag = !has_ceo & char_ceo)

# Split: keep originals as-is, duplicate flagged rows with CEO key
ceo_dupes <- individuals_mini %>%
  filter(flag) %>%
  mutate(title_standardized_key = "CEO: Chief Executive Officer")

individuals_mini <- bind_rows(individuals_mini, ceo_dupes) %>%
  select(-title_lower, -char_ceo, -has_ceo, -flag) %>%
  group_by(entity_uniqueid, year, title_standardized) %>%
  filter(n_distinct(contact_uniqueid) == 1) %>%
  ungroup()

# Get hospital level chars
hosp_chars <- hospitals %>% distinct(entity_name, entity_uniqueid, year, type, haentitytypeid, is_hospital)

entity_level <- vacancies_df %>% 
  filter(year > 2008) %>%
  filter(status %in% c("Contact Corporate", "Active")) %>%
  mutate(entity_uniqueid = as.numeric(entity_uniqueid),
         title_standardized_key = normalize_title_std(name)) %>%
  left_join(individuals_mini, by = c('entity_uniqueid', 'year', 'title_standardized_key')) %>%
  left_join(hosp_chars)

write_dta(entity_level, paste0(derived_data, "/temp/individuals_contact_corporate.dta"))

valid_hospitals <- hospitals %>% filter(is_hospital) %>% distinct(entity_uniqueid, year) %>% filter(year > 2008)
check_merge <- valid_hospitals %>% left_join(entity_level) %>% filter(is.na(full_name))
check_merge %>%
  filter(status == "Active") %>%
  distinct(title_standardized_key) %>%
  pull(title_standardized_key) %>%
  cat(sep = "\n")

## check for julia
indiv_entities <- individuals %>% distinct(entity_uniqueid) %>% pull(entity_uniqueid)
export_not_in_indiv <- entity_level %>% filter(is_hospital) %>%
  mutate(part_of_sample = type %in% c("General Medical","General Medical & Surgical","Critical Access")) %>%
  group_by(entity_uniqueid) %>%
  filter(any(part_of_sample)) %>%
  ungroup() %>% filter(!(entity_uniqueid %in% indiv_entities))
cat(n_distinct(export_not_in_indiv$entity_uniqueid))