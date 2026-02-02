suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(purrr)
  library(haven)
  library(arrow)   # read_feather / write_feather if you want
  library(rstudioapi)
})

rm(list = ls())

if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("../build_code/config.R")
}

# ----------------------------
# Load data
# ----------------------------
individuals <- read_feather(paste0(derived_data, "/individuals_final.feather"))
hospitals   <- read_feather(paste0(derived_data, "/hospitals_final.feather"))

# ----------------------------
# Flag gaps
# ----------------------------

# Define the C-suite roles we care about
csuite_roles <- c(
  "CEO:  Chief Executive Officer",
  "CIO:  Chief Information Officer",
  "CFO:  Chief Financial Officer",
  "COO:  Chief Operating Officer",
  "Medical Staff Chief", 
  "Chief Nursing Head",
  "Chief Compliance Officer"
)

# Step 1: Filter to C-suite roles
csuite_data <- individuals %>%
  mutate(
    title_lower = tolower(gsub("[[:punct:]]", "", title)),
    char_ceo = str_detect(title_lower, "ceo|chief executive officer| c e o")
  ) %>%
  filter(title_standardized %in% csuite_roles | char_ceo)

# Step 2: For each person-hospital-role combo, find gaps
# A gap exists at year X+1 if person is observed at X and X+2 but not X+1

gap_flags <- csuite_data %>%
  # Get distinct person-hospital-role-year combinations
  distinct(contact_uniqueid, entity_uniqueid, title_standardized, year, confirmed, char_ceo, all_leader_flag) %>%
  group_by(entity_uniqueid, year) %>%
  mutate(has_ceo = any(title_standardized == "CEO:  Chief Executive Officer")) %>%
  ungroup() %>%
  group_by(entity_uniqueid, year, contact_uniqueid) %>%
  mutate(person_is_ceo = any(title_standardized == "CEO:  Chief Executive Officer")) %>%
  ungroup() %>%
  # Drop rows where: char_ceo == "CEO" AND there's a CEO AND this person isn't it
  filter(!(char_ceo & has_ceo & !person_is_ceo)) %>%
  mutate(title_standardized = ifelse(char_ceo, "CEO:  Chief Executive Officer", title_standardized)) %>%
  # Group by person-hospital-role
  group_by(contact_uniqueid, entity_uniqueid, title_standardized) %>%
  arrange(year) %>%
  mutate(
    # Check if this year + 2 exists in the data for this person-hospital-role
    year_plus_2_exists = (year + 2) %in% year,
    # Check if this year + 1 is missing
    year_plus_1_missing = !((year + 1) %in% year)
  ) %>%
  # Flag the gap year (year + 1) when both conditions are true
  filter(year_plus_2_exists & year_plus_1_missing) %>%
  mutate(gap_year = year + 1) %>%
  ungroup() %>%
  # Create the gap flag dataset at hospital-role-year level
  distinct(entity_uniqueid, title_standardized, gap_year, contact_uniqueid, confirmed) %>%
  rename(year = gap_year) %>%
  mutate(
    gap_flag = TRUE,
    gap_unconfirmed = !confirmed  # Flag if the person creating the gap is unconfirmed
  ) %>%
  select(entity_uniqueid, title_standardized, year, gap_flag, gap_unconfirmed, 
         gap_person_id = contact_uniqueid)


# ----------------------------
# Flag vacancies
# ----------------------------
hospitals <- hospitals %>%
  mutate(
    vacant_ceo = all_ceo == "Vacant",
    vacant_cfo = all_cfo == "Vacant",
    vacant_coo = all_coo == "Vacant",
    vacant_cmo = all_cmo == "Vacant",
    vacant_cno = all_cno == "Vacant",
    vacant_cco = all_cco == "Vacant",
    vacant_cio = all_cio == "Vacant"
  )

# ----------------------------
# Compute turnover in confirmed individuals
# ----------------------------
# Step 1: Create entity-role-year level dataset
role_holders <- csuite_data %>%
  group_by(entity_uniqueid, year) %>%
  mutate(has_ceo = any(title_standardized == "CEO:  Chief Executive Officer")) %>%
  ungroup() %>%
  mutate(
    title_standardized = ifelse(!has_ceo & char_ceo, "CEO:  Chief Executive Officer", title_standardized)
  ) %>%
  group_by(entity_uniqueid, year, title_standardized) %>%
  filter(n_distinct(contact_uniqueid) == 1) %>% # this drops like 5 hospitals
  ungroup()

# Step 2: Calculate turnover by comparing year Y to year Y+1
turnover_data <- role_holders %>%
  arrange(entity_uniqueid, title_standardized, year) %>%
  group_by(entity_uniqueid, title_standardized) %>%
  mutate(
    # Get next year's info
    next_year = lead(year),
    next_contact = lead(contact_uniqueid),
    next_confirmed = lead(confirmed),
    # Check if next observation is actually year + 1 (not a gap)
    is_consecutive = next_year == year + 1
  ) %>%
  mutate(
    turnover = case_when(
      !is_consecutive ~ NA_real_,
      !confirmed | !next_confirmed ~ NA_real_,
      contact_uniqueid != next_contact ~ 1,
      contact_uniqueid == next_contact ~ 0
    )
  ) %>%
  ungroup() %>%
  distinct(entity_uniqueid, year, title_standardized, turnover, contact_uniqueid, char_ceo, all_leader_flag)

updated_turnover <- turnover_data %>%
  group_by(entity_uniqueid, year, title_standardized) %>%
  filter(n() == 1) %>%
  ungroup() %>%
  distinct(entity_uniqueid, year, title_standardized, turnover)

non_std_turnover <- turnover_data %>%
  mutate(std_ceo = title_standardized == "CEO:  Chief Executive Officer") %>%
  filter((char_ceo & !std_ceo & title_standardized != "CIO Reports to")) %>%
  distinct(entity_uniqueid, year, turnover) %>%
  rename(non_std_turnover = turnover)

# ----------------------------
# Merge hospitals df -- use individuals + vacancies
# ----------------------------
# Step 1: Create a mapping from title_standardized to short role names
role_mapping <- c(
  "CEO:  Chief Executive Officer" = "ceo",
  "CIO:  Chief Information Officer" = "cio",
  "CFO:  Chief Financial Officer" = "cfo",
  "COO:  Chief Operating Officer" = "coo",
  "Medical Staff Chief" = "cmo",
  "Chief Nursing Head" = "cno",
  "Chief Compliance Officer" = "cco"
)

# Step 2: Pivot turnover_data to wide format
turnover_wide <- updated_turnover %>%
  mutate(role = role_mapping[title_standardized]) %>%
  filter(!is.na(role)) %>%
  select(entity_uniqueid, year, role, turnover) %>%
  pivot_wider(
    names_from = role,
    values_from = turnover,
    names_prefix = "turnover_"
  )

# Step 3: Pivot gap_flags to wide format
gap_wide <- gap_flags %>%
  mutate(role = role_mapping[title_standardized]) %>%
  select(entity_uniqueid, year, role, gap_flag) %>%
  pivot_wider(
    names_from = role,
    values_from = gap_flag,
    names_prefix = "gap_"
  )

# Step 4: Merge onto hospitals_df
hospitals_df <- hospitals %>%
  left_join(turnover_wide, by = c("entity_uniqueid", "year")) %>%
  left_join(gap_wide, by = c("entity_uniqueid", "year")) 

# Step 5: Fill in gap years with FALSE (no turnover)
# Logic: gaps should have turnover == 0, then default to individuals df
# If a position changes from active to vacant or vice versa, turnover should be TRUE
hospitals_df <- hospitals_df %>%
  arrange(entity_uniqueid, year) %>%
  group_by(entity_uniqueid) %>%
  mutate(
    # Check for specific transitions: Vacant <-> Active
    vacancy_change_ceo = (all_ceo == "Vacant" & lag(all_ceo) == "Active") | 
      (all_ceo == "Active" & lag(all_ceo) == "Vacant"),
    vacancy_change_cfo = (all_cfo == "Vacant" & lag(all_cfo) == "Active") | 
      (all_cfo == "Active" & lag(all_cfo) == "Vacant"),
    vacancy_change_coo = (all_coo == "Vacant" & lag(all_coo) == "Active") | 
      (all_coo == "Active" & lag(all_coo) == "Vacant"),
    vacancy_change_cmo = (all_cmo == "Vacant" & lag(all_cmo) == "Active") | 
      (all_cmo == "Active" & lag(all_cmo) == "Vacant"),
    vacancy_change_cno = (all_cno == "Vacant" & lag(all_cno) == "Active") | 
      (all_cno == "Active" & lag(all_cno) == "Vacant"),
    vacancy_change_cco = (all_cco == "Vacant" & lag(all_cco) == "Active") | 
      (all_cco == "Active" & lag(all_cco) == "Vacant"),
    vacancy_change_cio = (all_cio == "Vacant" & lag(all_cio) == "Active") | 
      (all_cio == "Active" & lag(all_cio) == "Vacant")
  ) %>%
  ungroup() %>%
  mutate(
    turnover_ceo = case_when(
      gap_ceo == TRUE ~ 0,
      vacancy_change_ceo == TRUE ~ 1,
      !is.na(turnover_ceo) ~ turnover_ceo,
      TRUE ~ NA_real_
    ),
    turnover_cfo = case_when(
      gap_cfo == TRUE ~ 0,
      vacancy_change_cfo == TRUE ~ 1,
      !is.na(turnover_cfo) ~ turnover_cfo,
      TRUE ~ NA_real_
    ),
    turnover_coo = case_when(
      gap_coo == TRUE ~ 0,
      vacancy_change_coo == TRUE ~ 1,
      !is.na(turnover_coo) ~ turnover_coo,
      TRUE ~ NA_real_
    ),
    turnover_cmo = case_when(
      gap_cmo == TRUE ~ 0,
      vacancy_change_cmo == TRUE ~ 1,
      !is.na(turnover_cmo) ~ turnover_cmo,
      TRUE ~ NA_real_
    ),
    turnover_cno = case_when(
      gap_cno == TRUE ~ 0,
      vacancy_change_cno == TRUE ~ 1,
      !is.na(turnover_cno) ~ turnover_cno,
      TRUE ~ NA_real_
    ),
    turnover_cco = case_when(
      gap_cco == TRUE ~ 0,
      vacancy_change_cco == TRUE ~ 1,
      !is.na(turnover_cco) ~ turnover_cco,
      TRUE ~ NA_real_
    ),
    turnover_cio = case_when(
      gap_cio == TRUE ~ 0,
      vacancy_change_cio == TRUE ~ 1,
      !is.na(turnover_cio) ~ turnover_cio,
      TRUE ~ NA_real_
    )
  )

# Check results
hospitals_df %>%
  summarise(
    across(starts_with("turnover_"), ~mean(.x == 1, na.rm = TRUE), .names = "{.col}_count")
  )

export <- hospitals_df %>% select(year, entity_uniqueid,starts_with("turnover_"), starts_with("vacant"), starts_with("vacancy_change"))

write_dta(hospitals_df, paste0(derived_data, "/hospitals_with_turnover.dta"))