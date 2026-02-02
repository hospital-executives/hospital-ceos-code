rm(list = ls())
library(rstudioapi)
library(purrr)
library(janitor)

# load data
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("../build_code/config.R")
  hospitals <- read_feather(paste0(derived_data, "/hospitals_with_xwalk.feather"))
  cleaned_individuals <- read_feather(paste0(derived_data, "/individuals_final.feather"))
  supp_path <- supplemental_data
  output_dir <- paste0(data_file_path, "/summary_stats/execs")
} else {
  args <- commandArgs(trailingOnly = TRUE)
  source("../build_code/config.R")
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
      file_path <- paste0("dbo_", prefix, year, ".csv")
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
  ) %>% mutate(entity_uniqueid = as.numeric(entity_uniqueid))

### merge on sample
# load upstream data
hospital_xwalk <- read_stata(paste0(derived_data, "/temp/merged_ma_sysid_xwalk.dta"))
type <- read_stata(paste0(derived_data, "/temp/merged_ma_nonharmonized.dta")) %>%
  distinct(entity_uniqueid, year, type)

hosp_sample <- hospital_xwalk %>% left_join(type, by = c("entity_uniqueid", "year")) %>%
  filter(is_hospital == 1) %>%
  mutate(
    partofsample = type %in% c("General Medical","General Medical & Surgical","Critical Access")
  ) %>%
  group_by(entity_uniqueid) %>%
  mutate(ever_partofsample = any(partofsample)) %>%
  ungroup() %>%
  filter(ever_partofsample) %>%
  distinct(entity_uniqueid, year)

# get only titles in our sample
merged <- hosp_sample %>% 
  left_join(vacancies_wide, by = c("entity_uniqueid", "year")) %>% 
  filter(year != 2017)

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(ggplot2)
  library(scales)
})

YEAR_COL <- "year"

plot_title_values_over_time <- function(df, title_col, year_col = YEAR_COL, label = NULL) {
  stopifnot(title_col %in% names(df), year_col %in% names(df))
  if (is.null(label)) label <- title_col
  
  d <- df %>%
    dplyr::transmute(
      year = .data[[year_col]],
      value_raw = .data[[title_col]]
    ) %>%
    dplyr::filter(!is.na(year)) %>%
    dplyr::mutate(
      value = dplyr::if_else(is.na(value_raw), NA_character_, stringr::str_squish(as.character(value_raw)))
    )
  
  counts <- d %>%
    dplyr::mutate(value = dplyr::if_else(is.na(value), "(NA)", value)) %>%
    dplyr::count(year, value, name = "n") %>%
    dplyr::arrange(value, year) %>%
    dplyr::filter(n > 0) %>%
    dplyr::mutate(value = droplevels(factor(value)))
  
  ggplot(counts, aes(x = year, y = n, color = value, group = value)) +
    geom_line(linewidth = 1) +
    geom_point(size = 1.6) +
    scale_y_continuous(labels = scales::comma) +
    labs(
      title = paste0(label, ": distribution of values over time"),
      x = "Year",
      y = "Count",
      color = "Value"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      legend.position = "right",
      panel.grid.minor = element_blank()
    )
}


outdir = '/Users/katherinepapen/Library/CloudStorage/Dropbox/Apps/Overleaf/Hospital CEOs/notes/Title descriptives/figures'

title_map <- c(
  # ---- TIER 1: Business ----
  "CEO:  Chief Executive Officer"              = "ceo_chief_executive_officer",
  "CFO:  Chief Financial Officer"              = "cfo_chief_financial_officer",
  "COO:  Chief Operating Officer"              = "coo_chief_operating_officer",
  
  # ---- TIER 1: Clinical ----
  "Medical Staff Chief"                        = "medical_staff_chief",
  "Chief Nursing Head"                         = "chief_nursing_head",
  
  # ---- TIER 1: IT/Legal/HR ----
  "CIO:  Chief Information Officer"             = "cio_chief_information_officer",
  "Chief Compliance Officer"                   = "chief_compliance_officer",
  "CSIO/IT Security Officer"                   = "csio/it_security_officer",
  "Chief Medical Information Officer"          = "chief_medical_information_officer",
  "CNIS:  Chief Nursing Informatics Officer"   = "cnis_chief_nursing_informatics_officer"
  # "Chief Experience/Patient Engagement Officer" = ???  (no column in your colnames list)
  
  # ---- TIER 2: Business ----
  ,"Business Office Head"                      = "business_office_head",
  "Marketing Head"                             = "marketing_head",
  "Purchasing Head"                            = "purchasing_head",
  "Patient Accounting/Revenue Cycle Head"      = "patient_accounting/revenue_cycle_head",
  
  # ---- TIER 2: Clinical ----
  "Quality Head"                               = "quality_head",
  "OB Head"                                    = "ob_head",
  "Cardiology Head"                            = "cardiology_head",
  "ER Director"                                = "er_director",
  "OR Head"                                    = "or_head",
  "Ambulatory Care Head"                       = "ambulatory_care_head",
  "Patient Safety Head"                        = "patient_safety_head",
  "Pathology Chief"                            = "pathology_chief",
  "Laboratory Director"                        = "laboratory_director",
  "Pharmacy Head"                              = "pharmacy_head",
  "Radiology Med Dir"                          = "radiology_med_dir",
  
  # ---- TIER 2: IT/Legal/HR ----
  "IT Director"                                = "it_director",
  "HR Head"                                    = "hr_head",
  "HIM Director"                               = "him_director",
  "Facility Management Head"                   = "facility_management_head",
  "Director of Technology"                     = "director_of_technology",
  "Clinical Systems Director"                  = "clinical_systems_director"
)


plots <- list()

for (pretty in names(title_map)) {
  
  col <- unname(title_map[[pretty]])
  if (!col %in% names(merged)) {
    message("Skipping (missing column in merged): ", pretty, " -> ", col)
    next
  }
  
  message("Plotting: ", pretty, " (", col, ")")
  
  p <- plot_title_values_over_time(merged, title_col = col, label = pretty)
  
  safe_name <- gsub("[^A-Za-z0-9]+", "_", tolower(pretty))
  
  ggsave(
    filename = file.path(outdir, paste0("values_over_time_", safe_name, ".png")),
    plot     = p,
    width    = 10,
    height   = 6,
    units    = "in",
    dpi      = 300
  )
  
  plots[[pretty]] <- p
}
