rm(list = ls())
library(rstudioapi)
library(purrr)
library(janitor)

# load data
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("config.R")
  hospitals <- read_feather(paste0(derived_data, "/hospitals_with_xwalk.feather"))
  cleaned_individuals <- read_feather(paste0(derived_data, "/individuals_final.feather"))
  supp_path <- supplemental_data
  output_dir <- paste0(data_file_path, "/summary_stats/execs")
} else {
  args <- commandArgs(trailingOnly = TRUE)
  source("config.R")
  hospitals <- read_feather(args[1])
  individuals <- read_feather(args[2])
  supp_path <- args[3]
  output_dir <- args[4] 
}

## load necessary himss data
folders <- list.dirs(path = raw_data, full.names = TRUE, recursive = FALSE)

year_range <- 2005:2017 
file_prefixes <- c("HAEntityContact_", "HAEntity_", "ContactSource_")

# Loop through each file prefix
for (prefix in file_prefixes) {
  temp_dfs <- list()  # Temporary list to store dataframes for this prefix
  
  # Loop through each year
  for (year in year_range) {
    file_path <- paste0(prefix, year, ".csv")  # Construct the file name
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

vacancies_df <- haentitycontact %>% 
  rename(contactsourceid = hacontactsourceid) %>%
  left_join(contactsource, by = c("contactsourceid", "year")) %>%
  left_join(haentity %>% 
            rename(entity_uniqueid = uniqueid) %>%
            distinct(entity_uniqueid, year, haentityid), by = c("year", "haentityid")) %>%
  distinct(entity_uniqueid, year, status, name) %>%
  filter(!is.na(name)) 

test_titles <- vacancies_df %>%
  group_by(entity_uniqueid, year, name) %>%
  filter(n_distinct(status) > 1) %>%
  ungroup()

view <- test_titles %>% arrange(entity_uniqueid, year, name)

vacancies_wide <- vacancies_df %>%
  filter(!is.na(entity_uniqueid) & name != "CIO Reports to") %>%
  mutate(name_clean = tolower(gsub("^_|_$", "", gsub("_+", "_", gsub("[ :\\-]", "_", name))))) %>%
  pivot_wider(
    id_cols = c(entity_uniqueid, year),
    names_from = name_clean,
    values_from = status
  )

cleaned_vacancies <- vacancies_wide %>%
  mutate(
    ceo_flag = rowSums(!is.na(select(., matches("ceo", perl = TRUE)))) > 0,
    cfo_flag = rowSums(!is.na(select(., matches("cfo", perl = TRUE)))) > 0,
    coo_flag = rowSums(!is.na(select(., matches("coo", perl = TRUE)))) > 0,
    cio_flag = rowSums(!is.na(select(., matches("cio", perl = TRUE)))) > 0
  ) %>%
  rowwise() %>%
  mutate(
    all_ceo = if (ceo_flag) list(na.omit(c_across(matches("ceo(?!_flag$)", perl = TRUE)))) else list(NA),
    all_cfo = if (cfo_flag) list(na.omit(c_across(matches("cfo(?!_flag$)", perl = TRUE)))) else list(NA),
    all_coo = if (coo_flag) list(na.omit(c_across(matches("\\bcoo\\b|coo_chief", perl = TRUE)))) else list(NA),
    all_cio = if (cio_flag) list(na.omit(c_across(matches("cio(?!_flag$)", perl = TRUE)))) else list(NA)
  ) %>%
  ungroup() %>%
  select(-ends_with("_flag")) %>%
  mutate(
    all_ceo = map(all_ceo, unique),
    all_cfo = map(all_cfo, unique),
    all_coo = map(all_coo, unique),
    all_cio = map(all_cio, unique)
  ) %>%
  # Remove "Position does not exist" if there are other observations
  mutate(
    all_ceo = map(all_ceo, ~if(length(.x) > 1) .x[.x != "Position does not exist"] else .x),
    all_cfo = map(all_cfo, ~if(length(.x) > 1) .x[.x != "Position does not exist"] else .x),
    all_coo = map(all_coo, ~if(length(.x) > 1) .x[.x != "Position does not exist"] else .x),
    all_cio = map(all_cio, ~if(length(.x) > 1) .x[.x != "Position does not exist"] else .x)
  )

## merge on aha flag
collapsed_individuals <- cleaned_individuals %>%
  group_by(entity_uniqueid, year) %>%
  filter(any(aha_leader_flag)) %>%
  ungroup %>% 
  distinct(entity_uniqueid, year) %>%
  mutate(flagged_leader_in_aha = TRUE)

export <- cleaned_vacancies %>% 
  mutate(entity_uniqueid = as.numeric(entity_uniqueid)) %>%
  left_join(collapsed_individuals, by = c("entity_uniqueid", "year")) %>%
  mutate(flagged_leader_in_aha = ifelse(is.na(flagged_leader_in_aha), FALSE, flagged_leader_in_aha)) %>%
  # convert all csuite flags from list to character string
  rowwise() %>%
  mutate(
    all_ceo = if(length(all_ceo) <= 1) as.character(all_ceo) else list(all_ceo),
    all_cfo = if(length(all_cfo) <= 1) as.character(all_cfo) else list(all_cfo),
    all_coo = if(length(all_coo) <= 1) as.character(all_coo) else list(all_coo),
    all_cio = if(length(all_cio) <= 1) as.character(all_cio) else list(all_cio)
  ) %>%
  ungroup()

# export dfs as feather + dta files
write_feather(export, paste0(derived_data, "/himss_title_master.feather"))
export <- export %>% clean_names() %>%
  rename(cmio = chief_medical_information_officer,
         patient_accounting_head = patient_accounting_revenue_cycle_head,
         cnis = cnis_chief_nursing_informatics_officer)
write_dta(export, paste0(derived_data, "/himss_title_master.dta"))

