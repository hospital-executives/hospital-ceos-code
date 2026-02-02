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

### get info for hosp sample only
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


#### create mappings ####
title_tier_group_map <- tibble::tribble(
  ~title_standardized,                         ~tier,    ~group,
  
  # ---- TIER 1: Business ----
  "CEO:  Chief Executive Officer",              "TIER 1", "Business",
  "CFO:  Chief Financial Officer",              "TIER 1", "Business",
  "COO:  Chief Operating Officer",              "TIER 1", "Business",
  
  # ---- TIER 1: Clinical ----
  "Medical Staff Chief",                        "TIER 1", "Clinical",
  "Chief Nursing Head",                         "TIER 1", "Clinical",
  
  # ---- TIER 1: IT/Legal/HR ----
  "CIO:  Chief Information Officer",             "TIER 1", "IT/Legal/HR",
  "Chief Compliance Officer",                   "TIER 1", "IT/Legal/HR",
  "CSIO/IT Security Officer",                   "TIER 1", "IT/Legal/HR",
  "Chief Medical Information Officer",          "TIER 1", "IT/Legal/HR",
  "CNIS:  Chief Nursing Informatics Officer",   "TIER 1", "IT/Legal/HR",
  "Chief Experience/Patient Engagement Officer","TIER 1", "IT/Legal/HR",
  
  # ---- TIER 2: Business ----
  "Business Office Head",                       "TIER 2", "Business",
  "Marketing Head",                             "TIER 2", "Business",
  "Purchasing Head",                            "TIER 2", "Business",
  "Patient Accounting/Revenue Cycle Head",      "TIER 2", "Business",
  
  # ---- TIER 2: Clinical ----
  "Quality Head",                               "TIER 2", "Clinical",
  "OB Head",                                    "TIER 2", "Clinical",
  "Cardiology Head",                            "TIER 2", "Clinical",
  "ER Director",                                "TIER 2", "Clinical",
  "OR Head",                                    "TIER 2", "Clinical",
  "Ambulatory Care Head",                       "TIER 2", "Clinical",
  "Patient Safety Head",                        "TIER 2", "Clinical",
  "Pathology Chief",                            "TIER 2", "Clinical",
  "Laboratory Director",                        "TIER 2", "Clinical",
  "Pharmacy Head",                              "TIER 2", "Clinical",
  "Radiology Med Dir",                          "TIER 2", "Clinical",
  
  # ---- TIER 2: IT/Legal/HR ----
  "IT Director",                                "TIER 2", "IT/Legal/HR",
  "HR Head",                                    "TIER 2", "IT/Legal/HR",
  "HIM Director",                               "TIER 2", "IT/Legal/HR",
  "Facility Management Head",                   "TIER 2", "IT/Legal/HR",
  "Director of Technology",                     "TIER 2", "IT/Legal/HR",
  "Clinical Systems Director",                  "TIER 2", "IT/Legal/HR"
) %>%
  mutate(title_standardized_key = normalize_title_std(title_standardized)) %>%
  select(title_standardized_key, tier, group) %>%
  distinct()

#### get when titles are missing from df entirely ####
# Create the complete panel of all name/year combinations
valid_vacancies <- hosp_sample %>% left_join(vacancies_df %>% mutate(entity_uniqueid = as.numeric(entity_uniqueid)))

full_panel <- expand.grid(
  name = unique(valid_vacancies$name),
  year = 2009:2016  # or whatever your year range should be
)

# Join in the actual data
panel_filled <- full_panel %>%
  left_join(valid_vacancies)%>%
  group_by(name, year) %>%
  summarise(has_active = any(status == "Active", na.rm = TRUE), .groups = "drop")

# Filter to name/year combos with no active status
no_active <- panel_filled %>%
  filter(!has_active) %>%
  select(name, year)

summary_df <- vacancies_df %>%
  mutate(
    title_standardized_key = normalize_title_std(name),
    title_standardized     = normalize_title_std(name),
    status = ifelse(str_detect(status, "Exist|exist"), "DNE", status)
  ) %>%
  left_join(title_tier_group_map, by = "title_standardized_key") %>%
  filter(!is.na(tier) & status %in% c("DNE", "Vacant", "Active")) %>%
  # Create clean tier_group variable: e.g., "business1", "clinical2", "itlegalhr1"
  mutate(
    tier_num = str_extract(tier, "\\d"),
    group_clean = tolower(group) %>% str_replace_all("[^a-z]", ""),  # removes spaces, slashes
    status_clean = tolower(status) %>% str_replace_all(" ", "_"),
    tier_group = paste0(group_clean, tier_num)
  ) %>%
  # Count by entity, year, tier_group, and status
  count(entity_uniqueid, year, tier_group, status_clean) %>%
  # Pivot wider
  pivot_wider(
    names_from = c(tier_group, status_clean),
    values_from = n,
    values_fill = 0,
    names_sep = "_"
  )

## get individuals 
indiv_summary <- cleaned_individuals %>% 
  distinct(entity_uniqueid, year, title_standardized, full_name, contact_uniqueid, confirmed) %>%
  mutate(title_standardized_key = normalize_title_std(title_standardized)) %>%
  left_join(title_tier_group_map, by = "title_standardized_key") %>%
  filter(!is.na(tier)) %>%
  # Create clean tier_group variable
  mutate(
    tier_num = str_extract(tier, "\\d"),
    group_clean = tolower(group) %>% str_replace_all("[^a-z]", ""),
    tier_group = paste0(group_clean, tier_num)
  ) %>%
  group_by(entity_uniqueid, year, tier_group) %>%
  # when mismatch for contact_uniqueid and name, id was correct in the 4 cases I checked
  summarize(
    n_people = n_distinct(contact_uniqueid, na.rm = TRUE),
    n_roles  = n_distinct(title_standardized, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  pivot_wider(
    names_from = tier_group,
    values_from = c(n_people, n_roles),
    values_fill = 0,
    names_glue = "{tier_group}_{.value}"
  )

## get number of roles in each tier for each year
# Step 1: Calculate the number of existing titles per tier_group per year
# First, add tier_group info to the title mapping
title_tier_with_group <- title_tier_group_map %>%
  mutate(
    tier_num = str_extract(tier, "\\d"),
    group_clean = tolower(group) %>% str_replace_all("[^a-z]", ""),
    tier_group = paste0(group_clean, tier_num)
  ) %>%
  select(title_standardized_key, tier_group)

# Get base count of titles per tier_group
base_counts <- title_tier_with_group %>%
  count(tier_group, name = "total_roles")

# Count titles that don't exist per tier_group per year
no_active_by_tier <- no_active %>%
  mutate(title_standardized_key = normalize_title_std(name)) %>%
  inner_join(title_tier_with_group, by = "title_standardized_key") %>%
  count(year, tier_group, name = "n_missing")

# Create panel of year x tier_group with existing role counts
years_in_data <- sort(unique(as.numeric(merged$year)))
tier_groups <- unique(base_counts$tier_group)

roles_exist_by_year <- expand.grid(
  year = years_in_data,
  tier_group = tier_groups,
  stringsAsFactors = FALSE
) %>%
  left_join(base_counts, by = "tier_group") %>%
  left_join(no_active_by_tier, by = c("year", "tier_group")) %>%
  mutate(
    n_missing = replace_na(n_missing, 0),
    n_exist = total_roles - n_missing
  ) %>%
  select(year, tier_group, n_exist) %>%
  pivot_wider(
    names_from = tier_group,
    values_from = n_exist,
    names_prefix = "n_exist_"
  )


## merge hospitals + individuals
merged <- summary_df %>% mutate(entity_uniqueid = as.numeric(entity_uniqueid)) %>%
  left_join(indiv_summary, by = c('entity_uniqueid', 'year'))

merged <- merged %>%
  mutate(year = as.numeric(year)) %>%
  left_join(roles_exist_by_year, by = "year") %>%
  mutate(
    # Share active (using dynamic denominators)
    business1_sh_active    = business1_active / n_exist_business1,
    business2_sh_active    = business2_active / n_exist_business2,
    clinical1_sh_active    = clinical1_active / n_exist_clinical1,
    clinical2_sh_active    = clinical2_active / n_exist_clinical2,
    itlegalhr1_sh_active   = itlegalhr1_active / n_exist_itlegalhr1,
    itlegalhr2_sh_active   = itlegalhr2_active / n_exist_itlegalhr2,
    
    # Share vacant
    business1_sh_vacant    = business1_vacant / n_exist_business1,
    business2_sh_vacant    = business2_vacant / n_exist_business2,
    clinical1_sh_vacant    = clinical1_vacant / n_exist_clinical1,
    clinical2_sh_vacant    = clinical2_vacant / n_exist_clinical2,
    itlegalhr1_sh_vacant   = itlegalhr1_vacant / n_exist_itlegalhr1,
    itlegalhr2_sh_vacant   = itlegalhr2_vacant / n_exist_itlegalhr2,
    
    # Share DNE
    business1_sh_dne       = business1_dne / n_exist_business1,
    business2_sh_dne       = business2_dne / n_exist_business2,
    clinical1_sh_dne       = clinical1_dne / n_exist_clinical1,
    clinical2_sh_dne       = clinical2_dne / n_exist_clinical2,
    itlegalhr1_sh_dne      = itlegalhr1_dne / n_exist_itlegalhr1,
    itlegalhr2_sh_dne      = itlegalhr2_dne / n_exist_itlegalhr2,
    
    # People per role ratio (unchanged)
    business1_people_per_role    = if_else(business1_n_roles > 0, business1_n_people / business1_n_roles, NA_real_),
    business2_people_per_role    = if_else(business2_n_roles > 0, business2_n_people / business2_n_roles, NA_real_),
    clinical1_people_per_role    = if_else(clinical1_n_roles > 0, clinical1_n_people / clinical1_n_roles, NA_real_),
    clinical2_people_per_role    = if_else(clinical2_n_roles > 0, clinical2_n_people / clinical2_n_roles, NA_real_),
    itlegalhr1_people_per_role   = if_else(itlegalhr1_n_roles > 0, itlegalhr1_n_people / itlegalhr1_n_roles, NA_real_),
    itlegalhr2_people_per_role   = if_else(itlegalhr2_n_roles > 0, itlegalhr2_n_people / itlegalhr2_n_roles, NA_real_)
  )

merged_export <- merged %>%
  select(
    entity_uniqueid,
    year,
    # Share active
    business1_sh_active, business2_sh_active,
    clinical1_sh_active, clinical2_sh_active,
    itlegalhr1_sh_active, itlegalhr2_sh_active,
    # Share vacant
    business1_sh_vacant, business2_sh_vacant,
    clinical1_sh_vacant, clinical2_sh_vacant,
    itlegalhr1_sh_vacant, itlegalhr2_sh_vacant,
    # Share DNE
    business1_sh_dne, business2_sh_dne,
    clinical1_sh_dne, clinical2_sh_dne,
    itlegalhr1_sh_dne, itlegalhr2_sh_dne,
    # People per role
    business1_people_per_role, business2_people_per_role,
    clinical1_people_per_role, clinical2_people_per_role,
    itlegalhr1_people_per_role, itlegalhr2_people_per_role
  )

write_dta(merged_export, paste0(derived_data, "/positions_by_tier.dta"))