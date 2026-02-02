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

cco_switch <- vacancies_df %>%
  mutate(
    title_standardized_key = normalize_title_std(name),
    title_standardized     = normalize_title_std(name),
    status = ifelse(str_detect(status, "Exist|exist"), "DNE", status)
  ) %>%
  filter(name == "Chief Compliance Officer" & status %in% c("DNE", "Vacant", "Active", NA)) %>%
  arrange(entity_uniqueid, year) %>%
  group_by(entity_uniqueid) %>%
  mutate(
    prev_status = lag(status),
    cco_switched_to_active = as.integer(
      status == "Active" & (is.na(prev_status) | prev_status %in% c("DNE", NA))
    )
  ) %>%
  ungroup() %>%
  distinct(entity_uniqueid, year, cco_switched_to_active)

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
  ) %>% left_join(cco_switch %>% mutate(entity_uniqueid = as.numeric(entity_uniqueid))) %>%
  filter(!is.na(entity_uniqueid))

write_dta(merged_export, paste0(derived_data, "/positions_by_tier.dta"))


##### run regressions at the role level #####
long_vacancies <- valid_vacancies %>%
  mutate(
    title_standardized_key = normalize_title_std(name),
    title_standardized     = normalize_title_std(name),
    status = ifelse(str_detect(status, "Exist|exist"), "DNE", status),
    non_report = status == "Not Reported",
    contact_corporate = status == "Contact Corporate",
    unclear = non_report | contact_corporate
  ) %>%
  filter(title_standardized_key)

###### merge in for profit status
xwalk_raw = read_dta(paste0(derived_data, "/temp/merged_ma_sysid_xwalk.dta")) 

xwalk <- xwalk_raw %>% 
  distinct(entity_uniqueid, entity_uniqueid_parent, year, sysid_orig,sysid_ma, forprofit)

# First merge: get parent profit status
sysprofit <- read_dta(paste0(derived_data, "/temp/systems_nonharmonized_withprofit.dta")) %>%
  select(entity_uniqueid, year, forprofit) %>%
  rename(
    entity_uniqueid_parent = entity_uniqueid,
    forprofit_parent = forprofit
  )

# Merge on entity_uniqueid_parent and year (left join keeps all from main df)
xwalk <- xwalk %>%
  left_join(sysprofit, by = c("entity_uniqueid_parent", "year"))

# Second merge: pull in forprofit_imputed for the entity itself
sys_forprofit_imputed <- read_dta(paste0(derived_data, "/temp/systems_nonharmonized_withprofit.dta")) %>%
  select(entity_uniqueid, year, forprofit_imputed)

get_mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

xwalk <- xwalk %>%
  left_join(sys_forprofit_imputed, by = c("entity_uniqueid", "year")) %>%
  mutate(
    # Replace forprofit with forprofit_imputed where available (this only changes things for parents)
    forprofit = if_else(!is.na(forprofit_imputed), forprofit_imputed, forprofit)
  ) %>%
  select(-forprofit_imputed) %>%
  group_by(entity_uniqueid) %>%
  mutate(sysid_orig = ifelse(sysid_orig == "", get_mode(sysid_orig), sysid_orig)) %>%
  ungroup() %>%
  group_by(sysid_orig, year) %>%
  mutate(num_hosp = n_distinct(entity_uniqueid)) %>%
  ungroup() %>%
  mutate(
    no_sys = sysid_orig == ""
  ) %>%
  group_by(sysid_orig) %>%
  mutate(
    modal_num_hosp = get_mode(num_hosp),
    indep_hospital = modal_num_hosp == 1 | no_sys | sysid_orig == 2,
    small_system = modal_num_hosp < 10 & !indep_hospital & !no_sys,
    large_system = modal_num_hosp >= 10 & !indep_hospital & !no_sys
  ) %>%
  ungroup()

#
regression_df <- long_vacancies %>% left_join(xwalk)

### run regressions

library(broom)
library(ggplot2)

# Prepare the data
reg_data <- regression_df %>%
  # Exclude cases where all system status indicators are FALSE
  filter(indep_hospital | small_system | large_system) %>%
  # Create factor variables
  mutate(
    profit_status = factor(if_else(forprofit_parent == 1, "For-Profit", "Not-for-Profit"),
                           levels = c("For-Profit", "Not-for-Profit")),
    system_status = case_when(
      indep_hospital ~ "Independent",
      small_system ~ "Small System",
      large_system ~ "Large System"
    ),
    system_status = factor(system_status, levels = c("Independent", "Small System", "Large System")),
    year = factor(year)
  )

# Get list of roles
roles <- unique(reg_data$title_standardized)

# ---- Regression 1: Profit Status ----
profit_results <- map_dfr(roles, function(role) {
  role_data <- reg_data %>% filter(title_standardized == role)
  
  # Skip if insufficient variation in key variables
  if (n_distinct(role_data$profit_status) < 2) return(NULL)
  if (n_distinct(role_data$year) < 2) return(NULL)
  if (nrow(role_data) < 10) return(NULL)  # optional: require minimum obs
  
  tryCatch({
    model <- lm(non_report ~ profit_status + year, data = role_data)
    
    tidy(model, conf.int = TRUE) %>%
      filter(term == "profit_statusNot-for-Profit") %>%
      mutate(role = role)
  }, error = function(e) NULL)
})

# ---- Regression 2: System Status ----
system_results <- map_dfr(roles, function(role) {
  role_data <- reg_data %>% filter(title_standardized == role)
  
  # Skip if insufficient variation in key variables
  if (n_distinct(role_data$system_status) < 2) return(NULL)
  if (n_distinct(role_data$year) < 2) return(NULL)
  if (nrow(role_data) < 10) return(NULL)  # optional: require minimum obs
  
  tryCatch({
    model <- lm(non_report ~ system_status + year, data = role_data)
    
    tidy(model, conf.int = TRUE) %>%
      filter(term %in% c("system_statusSmall System", "system_statusLarge System")) %>%
      mutate(
        role = role,
        term = str_remove(term, "system_status")
      )
  }, error = function(e) NULL)
})

# ---- Forest Plot 1: Profit Status ----
profit_plot <- profit_results %>%
  mutate(role = fct_reorder(role, estimate)) %>%
  ggplot(aes(x = estimate, y = role)) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "gray50") +
  geom_errorbarh(aes(xmin = conf.low, xmax = conf.high), height = 0.2) +
  geom_point(size = 2) +
  labs(
    title = "Effect of Not-for-Profit Status on Non-Reporting",
    subtitle = "Baseline: For-Profit",
    x = "Coefficient (95% CI)",
    y = NULL
  ) +
  theme_minimal() +
  theme(
    axis.text.y = element_text(size = 8),
    panel.grid.minor = element_blank()
  )

# ---- Forest Plot 2: System Status ----
system_plot <- system_results %>%
  mutate(
    role = fct_reorder(role, estimate, .fun = mean),
    term = factor(term, levels = c("Small System", "Large System"))
  ) %>%
  ggplot(aes(x = estimate, y = role, color = term, shape = term)) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "gray50") +
  geom_errorbarh(aes(xmin = conf.low, xmax = conf.high), height = 0.2, 
                 position = position_dodge(width = 0.5)) +
  geom_point(size = 2, position = position_dodge(width = 0.5)) +
  scale_color_manual(values = c("Small System" = "#1b9e77", "Large System" = "#d95f02")) +
  labs(
    title = "Effect of System Size on Non-Reporting",
    subtitle = "Baseline: Independent Hospital",
    x = "Coefficient (95% CI)",
    y = NULL,
    color = NULL,
    shape = NULL
  ) +
  theme_minimal() +
  theme(
    axis.text.y = element_text(size = 8),
    panel.grid.minor = element_blank(),
    legend.position = "bottom"
  )

# Display plots
profit_plot
system_plot

# Optionally save
figures_folder = '/Users/katherinepapen/Library/CloudStorage/Dropbox/Apps/Overleaf/Hospital CEOs/notes/Title descriptives/figures'
ggsave(paste0(figures_folder, "/profit_status_forest.png"), profit_plot, width = 10, height = 8)
ggsave(paste0(figures_folder, "/system_status_forest.png"), system_plot, width = 10, height = 10)

#### also run with "unclear" as the outcome ####
# ---- Regression 3: Profit Status (Unclear) ----
# ---- Regression 3: Profit Status (Unclear) ----
profit_results_unclear <- map_dfr(roles, function(role) {
  role_data <- reg_data %>% 
    filter(title_standardized == role) %>%
    filter(as.numeric(as.character(year)) > 2011)
  
  # Skip if insufficient variation in key variables
  if (n_distinct(role_data$profit_status) < 2) return(NULL)
  if (n_distinct(role_data$year) < 2) return(NULL)
  if (nrow(role_data) < 10) return(NULL)
  
  tryCatch({
    model <- lm(unclear ~ profit_status + year, data = role_data)
    
    tidy(model, conf.int = TRUE) %>%
      filter(term == "profit_statusNot-for-Profit") %>%
      mutate(role = role)
  }, error = function(e) NULL)
})

# ---- Regression 4: System Status (Unclear) ----
system_results_unclear <- map_dfr(roles, function(role) {
  role_data <- reg_data %>% 
    filter(title_standardized == role) %>%
    filter(as.numeric(as.character(year)) > 2011)
  
  # Skip if insufficient variation in key variables
  if (n_distinct(role_data$system_status) < 2) return(NULL)
  if (n_distinct(role_data$year) < 2) return(NULL)
  if (nrow(role_data) < 10) return(NULL)
  
  tryCatch({
    model <- lm(unclear ~ system_status + year, data = role_data)
    
    tidy(model, conf.int = TRUE) %>%
      filter(term %in% c("system_statusSmall System", "system_statusLarge System")) %>%
      mutate(
        role = role,
        term = str_remove(term, "system_status")
      )
  }, error = function(e) NULL)
})

# ---- Forest Plot 3: Profit Status (Unclear) ----
profit_plot_unclear <- profit_results_unclear %>%
  mutate(role = fct_reorder(role, estimate)) %>%
  ggplot(aes(x = estimate, y = role)) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "gray50") +
  geom_errorbarh(aes(xmin = conf.low, xmax = conf.high), height = 0.2) +
  geom_point(size = 2) +
  labs(
    title = "Effect of Not-for-Profit Status on Unclear Reporting",
    subtitle = "Baseline: For-Profit",
    x = "Coefficient (95% CI)",
    y = NULL
  ) +
  theme_minimal() +
  theme(
    axis.text.y = element_text(size = 8),
    panel.grid.minor = element_blank()
  )

# ---- Forest Plot 4: System Status (Unclear) ----
system_plot_unclear <- system_results_unclear %>%
  mutate(
    role = fct_reorder(role, estimate, .fun = mean),
    term = factor(term, levels = c("Small System", "Large System"))
  ) %>%
  ggplot(aes(x = estimate, y = role, color = term, shape = term)) +
  geom_vline(xintercept = 0, linetype = "dashed", color = "gray50") +
  geom_errorbarh(aes(xmin = conf.low, xmax = conf.high), height = 0.2, 
                 position = position_dodge(width = 0.5)) +
  geom_point(size = 2, position = position_dodge(width = 0.5)) +
  scale_color_manual(values = c("Small System" = "#1b9e77", "Large System" = "#d95f02")) +
  labs(
    title = "Effect of System Size on Unclear Reporting",
    subtitle = "Baseline: Independent Hospital",
    x = "Coefficient (95% CI)",
    y = NULL,
    color = NULL,
    shape = NULL
  ) +
  theme_minimal() +
  theme(
    axis.text.y = element_text(size = 8),
    panel.grid.minor = element_blank(),
    legend.position = "bottom"
  )

# Display plots
profit_plot_unclear
system_plot_unclear

# Optionally save
ggsave(paste0(figures_folder, "/profit_status_unclear_forest.png"), profit_plot_unclear, width = 10, height = 8)
ggsave(paste0(figures_folder, "/system_status_unclear_forest.png"), system_plot_unclear, width = 10, height = 10)