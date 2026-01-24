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
  cleaned_individuals <- read_feather(args[2])
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
    ceo_flag = rowSums(!is.na(select(., matches("ceo")))) > 0,
    cfo_flag = rowSums(!is.na(select(., matches("cfo")))) > 0,
    coo_flag = rowSums(!is.na(select(., matches("coo")))) > 0,
    cio_flag = rowSums(!is.na(select(., matches("cio")))) > 0,
    cno_flag = rowSums(!is.na(select(., matches("nursing_head")))) > 0,
    cmo_flag = rowSums(!is.na(select(., matches("medical_staff_chief")))) > 0,
    cco_flag = rowSums(!is.na(select(., matches("chief_compliance_officer")))) > 0
  ) %>%
  rowwise() %>%
  mutate(
    all_ceo = if (ceo_flag) list(na.omit(c_across(matches("ceo") & !ends_with("_flag")))) else list(NA_character_),
    all_cfo = if (cfo_flag) list(na.omit(c_across(matches("cfo") & !ends_with("_flag")))) else list(NA_character_),
    all_coo = if (coo_flag) list(na.omit(c_across(matches("coo") & !ends_with("_flag")))) else list(NA_character_),
    all_cio = if (cio_flag) list(na.omit(c_across(matches("cio") & !ends_with("_flag")))) else list(NA_character_),
    all_cno = if (cno_flag) list(na.omit(c_across(matches("nursing_head")))) else list(NA_character_),
    all_cmo = if (cmo_flag) list(na.omit(c_across(matches("medical_staff_chief")))) else list(NA_character_),
    all_cco = if (cco_flag) list(na.omit(c_across(matches("chief_compliance_officer")))) else list(NA_character_)
  ) %>%
  ungroup() %>%
  select(-ends_with("_flag")) %>%
  mutate(
    across(starts_with("all_"), ~map(.x, unique))
  ) %>%
  mutate(
    across(starts_with("all_"), ~map(.x, function(vals) {
      if (length(vals) > 1) vals[vals != "Position does not exist"] else vals
    }))
  )

## merge on aha flag
collapsed_individuals <- cleaned_individuals %>%
  group_by(entity_uniqueid, year) %>%
  filter(any(aha_leader_flag)) %>%
  ungroup %>% 
  distinct(entity_uniqueid, year) %>%
  mutate(flagged_leader_in_aha = TRUE)

# Helper function to collapse values with priority logic
collapse_csuite <- function(vals) {
  # Handle NULL or empty
  
  if (is.null(vals) || length(vals) == 0) return(NA_character_)
  
  # Remove NAs
  vals <- vals[!is.na(vals)]
  if (length(vals) == 0) return(NA_character_)
  
  # Get unique values
  vals <- unique(vals)
  
  # Priority: Active > Vacant > Position does not exist > first value
  
  if ("Active" %in% vals) {
    return("Active")
  } else if ("Vacant" %in% vals) {
    return("Vacant")
  } else if (length(vals) == 1) {
    return(vals)
  } else {
    # Multiple other values - pick first (or you could paste them)
    return(vals[1])
  }
}

export <- cleaned_vacancies %>% 
  mutate(entity_uniqueid = as.numeric(entity_uniqueid)) %>%
  left_join(collapsed_individuals, by = c("entity_uniqueid", "year")) %>%
  mutate(flagged_leader_in_aha = ifelse(is.na(flagged_leader_in_aha), FALSE, flagged_leader_in_aha)) %>%
  mutate(
    all_ceo = map_chr(all_ceo, collapse_csuite),
    all_cfo = map_chr(all_cfo, collapse_csuite),
    all_coo = map_chr(all_coo, collapse_csuite),
    all_cio = map_chr(all_cio, collapse_csuite),
    all_cmo = map_chr(all_cmo, collapse_csuite),
    all_cno = map_chr(all_cno, collapse_csuite),
    all_cco = map_chr(all_cco, collapse_csuite)
  )

# export dfs as feather + dta files
write_feather(export, paste0(derived_data, "/himss_title_master.feather"))
export <- export %>% clean_names() %>%
  rename(cmio = chief_medical_information_officer,
         patient_accounting_head = patient_accounting_revenue_cycle_head,
         cnis = cnis_chief_nursing_informatics_officer)
write_dta(export, paste0(derived_data, "/himss_title_master.dta"))

## merge on hospitals df
selected_titles <- export %>% distinct(entity_uniqueid, year,
                                       all_ceo,all_cfo, all_coo, all_cio,
                                       head_of_facility, flagged_leader_in_aha)

merged_hospitals <- hospitals %>% 
  left_join(selected_titles, by = c("entity_uniqueid", "year"))

write_feather(merged_hospitals, paste0(derived_data, "/hospitals_final.feather"))
write_dta(merged_hospitals, paste0(derived_data, "/hospitals_final.dta"))