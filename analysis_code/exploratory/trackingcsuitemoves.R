###########
# Exploratory analysis: identifying hospital c-suite promotions
# Author: Maggie Shi
# Last edited: 4/25/2025
###########

library(dplyr)
library(stringr)
library(haven)
library(purrr)
library(feather)
library(tidyr)
library(knitr)
library(data.table)

# Load data
dropbox_base <- switch(
  Sys.info()[["user"]],
  "maggieshi" = "/Users/maggieshi/Dropbox",
  "mengdishi" = "/Users/mengdishi/Dropbox",
  stop("Unrecognized user")
)
df <- read_dta(file.path(dropbox_base, "hospital_ceos/_data/derived/final_confirmed.dta"))

# Sort 
df <- df %>%
  arrange(contact_uniqueid, year)

# Create key indicators:
# - CEO flag
# - Whether entity is a hospital
# - Whether individual is CEO at a hospital
# - Whether individual is in the hospital C-suite
# - Whether individual has MD credentials
df <- df %>% mutate(
  CEO = str_detect(title_standardized, "CEO:"),
  hospital = entity_type %in% c("Hospital", "Single Hospital Health System", "IDS/RHA"), 
  hospital_CEO = CEO & hospital,
  hospital_c_suite = c_suite & hospital,
  md = str_detect(credentials, "MD")
)
# Convert to data.table so that collapse can run faster 
setDT(df)

# Ensure the relevant variables are logical
df[, hospital_CEO := hospital_CEO == 1]
df[, c_suite := c_suite == 1]
df[, md := md == 1]

# Compute ever_* flags by group
df[, `:=`(
  ever_hospital_CEO = any(hospital_CEO),
  ever_hospital_csuite = any(c_suite),
  ever_MD = any(md)
), by = contact_uniqueid]


# Filter to individuals who have ever been in a c-suite role
# remove "CIO Reports to" as a role
df <- df %>%
  filter(ever_hospital_csuite & title_standardized != "CIO Reports to")

# convert back to tibble so we can use dplyr
df <- as_tibble(df)

n_distinct(df$contact_uniqueid) # 49,898 unique individuals 
###########
# STEP 1: Identify *first-time* c-suite promotions 
# Definition: first year a person is observed with c-suite role
###########
# Create a dataset with one row per person-year, listing all titles and entity types they worked at as well as if they were a c-suite exec that year
roles_by_year <- df %>%
  group_by(contact_uniqueid, year) %>%
  summarize(roles_this_year = list(unique(title_standardized)), 
            entity_types_this_year = list(unique(entity_type)),
            any_hospital_c_suite = max(hospital_c_suite),
            any_hospital = max(hospital),
            .groups = "drop") %>%
  arrange(contact_uniqueid, year)

# For each person, identify their role history
# - roles_prev_year = roles held in previous year
# - roles_all_prior = all roles in prior years (cumulative)
# - roles_gained = newly acquired roles this year
# - roles_lost = roles dropped this year
# - entity_types_prev_year = entity types worked at in the previous year
# - entity_types_all_prior = all entity types ever worked at in prior years (cumulative)
# - entity_types_gained = newly entered entity types this year
# - entity_types_lost = entity types exited this year
# - csuite_prev_year = C-suite status in the previous year
# - gained_csuite = indicator for entering the C-suite this year (was not C-suite last year, now is)
# - hosp_prev_year = were they in a hospital in the previous year
# - gained_hosp = indicator for entering a hospital this yaer

roles_by_year <- roles_by_year %>%
  mutate(roles_this_year = map(roles_this_year, as.character)) %>%
  group_by(contact_uniqueid) %>%
  mutate(
    roles_prev_year = lag(roles_this_year),
    roles_all_prior = lag(accumulate(roles_this_year, union) %>% map(identity)),
    roles_gained = map2(roles_this_year, roles_prev_year, ~ setdiff(.x, .y)),
    roles_lost   = map2(roles_prev_year, roles_this_year, ~ setdiff(.x, .y)),
    entity_types_prev_year = lag(entity_types_this_year),
    entity_types_all_prior = lag(accumulate(entity_types_this_year, union) %>% map(identity)),
    entity_types_gained = map2(entity_types_this_year, entity_types_prev_year, ~ setdiff(.x, .y)),
    entity_types_lost   = map2(entity_types_prev_year, entity_types_this_year, ~ setdiff(.x, .y)),
    csuite_prev_year = lag(any_hospital_c_suite),
    gained_csuite = if_else(any_hospital_c_suite == 1 & !is.na(csuite_prev_year) & csuite_prev_year == 0, 1, 0),
    hosp_prev_year = lag(any_hospital),
    gained_hosp = if_else(any_hospital == 1 & !is.na(hosp_prev_year) & hosp_prev_year == 0, 1, 0),
  ) %>%
  ungroup()


###########
# STEP 2: Distinguish between internal vs. external promotions to c-suite
# first, subset to c-suite promotions that occur in year t
# Then define internal vs. external promotion:
  # Internal: I am c-suite at hospital A in year t and I was at hospital A in year t-1, but not as c-suite.
  # External: I am c-suite at hospital B in year t and I was not at hospital B in year t-1, but not as c-suite
# Among external promotions, distinguish between across-hospital and from another entity type to hospital
  # From non-hospital: I am c-suite at hospital A in year t and I was at SNF B in year t-1
  # From hospital: I am c-suite at hospital B in year t and I was at hospital A in year t-1, but not as c-suite.
# the _consec definition restricts moves defined by cases where we have observations in 2 consecutive years. the other definition does not make this restriction and just compares to the most recent year
###########
# Get all entities person worked at in a year (any role)
all_entities_by_year <- df %>%
  mutate(entity_uniqueid = as.character(entity_uniqueid)) %>%
  group_by(contact_uniqueid, year) %>%
  summarize(entities = list(unique(entity_uniqueid)), .groups = "drop")


# Get csuite entities 
csuite_entities_by_year <- df %>%
  filter(hospital_c_suite) %>%
  mutate(entity_uniqueid = as.character(entity_uniqueid)) %>%
  group_by(contact_uniqueid, year) %>%
  summarize(csuite_entities = list(unique(entity_uniqueid)), .groups = "drop")


# Join both sets of entities to roles_by_year and compute promotion type
roles_by_year <- roles_by_year %>%
  left_join(all_entities_by_year, by = c("contact_uniqueid", "year")) %>%
  left_join(csuite_entities_by_year, by = c("contact_uniqueid", "year")) %>%
  group_by(contact_uniqueid) %>%
  arrange(year, .by_group = TRUE) %>%
  mutate(
    # Coerce everything to list-columns of character
    entities = map(entities, ~ if (is.null(.x)) character(0) else as.character(.x)),
    csuite_entities = map(csuite_entities, ~ if (is.null(.x)) character(0) else as.character(.x)),
    
    year_prev = lag(year),
    entities_prev = lag(entities),
    # FIXED: ensure accumulate returns a proper list-column
    entities_prior_all = lag(accumulate(entities, union) %>% map(identity)), 
    entities_prev = map(entities_prev, ~ if (is.null(.x)) character(0) else .x),
    entities_prior_all = map(entities_prior_all, ~ if (is.null(.x)) character(0) else .x),
    
    # internal vs. external: t-1 only
    promotion_type_consec = case_when(
      gained_csuite & year_prev == year - 1 & map2_lgl(csuite_entities, entities_prev, ~ any(.x %in% .y)) ~ "internal",
      gained_csuite & year_prev == year - 1 & map2_lgl(csuite_entities, entities_prev, ~ !any(.x %in% .y)) ~ "external",
      gained_csuite & year_prev != year - 1 ~ "missing year",
      TRUE ~ NA_character_
    ),
    # internal vs. external: any prior year
    promotion_type = case_when(
      gained_csuite  & map2_lgl(csuite_entities, entities_prev, ~ any(.x %in% .y)) ~ "internal",
      gained_csuite  & map2_lgl(csuite_entities, entities_prev, ~ !any(.x %in% .y)) ~ "external",
      TRUE ~ NA_character_
    ),
    # within external, from hospital vs. not: t-1 only
    ext_promotion_type_consec = case_when(
      promotion_type_consec == "external" & map_lgl(entity_types_prev_year, ~ any(.x %in% c("Single Hospital System", "Hospital", "IDS/RHA"))) ~ "from another hospital",
      promotion_type_consec == "external" & map_lgl(entity_types_prev_year, ~ !any(.x %in% c("Single Hospital System", "Hospital", "IDS/RHA"))) ~ "from non-hospital",
      TRUE ~ NA_character_
    ),
    # within external, from hospital vs. not: any prior year 
    ext_promotion_type = case_when(
      promotion_type == "external" & map_lgl(entity_types_prev_year, ~ any(.x %in% c("Single Hospital System", "Hospital", "IDS/RHA"))) ~ "from another hospital",
      promotion_type == "external" & map_lgl(entity_types_prev_year, ~ !any(.x %in% c("Single Hospital System", "Hospital", "IDS/RHA"))) ~ "from non-hospital",
      TRUE ~ NA_character_
    ),
  ) %>%
  ungroup()


# Tabulate each separately
roles_by_year %>% filter(!is.na(promotion_type_consec)) %>% count(promotion_type_consec)
roles_by_year %>% filter(!is.na(promotion_type)) %>% count(promotion_type)
roles_by_year %>% filter(!is.na(ext_promotion_type)) %>% count(ext_promotion_type)
roles_by_year %>% filter(!is.na(ext_promotion_type_consec)) %>% count(ext_promotion_type_consec)

##############
# STEP 3: create summary stats table 
##############
### Summarize counts of promotions, by type
summary_stats <- tibble(
  promotion_type = c("internal", "external", "external"),
  ext_promotion_type = c(NA, "from another hospital", "from non-hospital"),
  definition = c(
    "First time entering C-suite at a hospital previously worked at",
    "First time entering C-suite at a hospital different from prior hospital",
    "First time entering C-suite at a hospital from a non-hospital entity"
  ),
  example = c(
    "Year t−1: CFO at Hospital A → Year t: COO at Hospital A",
    "Year t−1: CFO at Hospital A → Year t: COO at Hospital B",
    "Year t−1: Director at SNF A → Year t: COO at Hospital B"
  )
)

# Step 2: Calculate n_transitions and n_individuals
promotion_transitions <- roles_by_year %>%
  filter(gained_csuite == 1)

# Correct denominator: total individuals who ever held C-suite
total_individuals <- roles_by_year %>%
  filter(any_hospital_c_suite == 1) %>%
  distinct(contact_uniqueid) %>%
  nrow()

# Count transitions
transition_counts <- promotion_transitions %>%
  mutate(
    ext_promotion_type = ifelse(is.na(ext_promotion_type), NA_character_, ext_promotion_type)
  ) %>%
  group_by(promotion_type, ext_promotion_type) %>%
  summarize(
    n_transitions = n(),
    n_individuals = n_distinct(contact_uniqueid),
    .groups = "drop"
  )

# Step 3: Join counts back to summary_stats
summary_stats <- summary_stats %>%
  left_join(transition_counts, by = c("promotion_type", "ext_promotion_type")) %>%
  mutate(
    share_of_individuals = n_individuals / total_individuals
  )

kable(summary_stats, format = "markdown")

### What are the most common roles to be transitioning from?
# Top 10 roles held before internal C-suite promotion (unique individuals)
internal_promotion_roles_persons <- roles_by_year %>%
  filter(gained_csuite == 1, promotion_type == "internal") %>%
  select(contact_uniqueid, roles_prev_year) %>%
  unnest(roles_prev_year) %>%
  distinct(contact_uniqueid, roles_prev_year) %>%
  count(roles_prev_year, sort = TRUE) %>%
  slice_head(n = 10)

kable(internal_promotion_roles_persons, format = "markdown", caption = "Top 10 Prior Roles Before Internal C-Suite Promotion (Unique Individuals)")

# Top 10 roles held before external C-suite promotion (unique individuals)
external_promotion_roles_persons <- roles_by_year %>%
  filter(gained_csuite == 1, promotion_type == "external") %>%
  select(contact_uniqueid, roles_prev_year) %>%
  unnest(roles_prev_year) %>%
  distinct(contact_uniqueid, roles_prev_year) %>%
  count(roles_prev_year, sort = TRUE) %>%
  slice_head(n = 10)

kable(external_promotion_roles_persons, format = "markdown", caption = "Top 10 Prior Roles Before External C-Suite Promotion (Unique Individuals)")



### What are the most common entity types to be promoted from (if not a hospital)
# Top 10 entity types promoted from (external from non-hospital) (unique individuals)
nonhospital_promotion_entities_persons <- roles_by_year %>%
  filter(gained_csuite == 1, ext_promotion_type == "from non-hospital") %>%
  select(contact_uniqueid, entity_types_prev_year) %>%
  unnest(entity_types_prev_year) %>%
  distinct(contact_uniqueid, entity_types_prev_year) %>%
  count(entity_types_prev_year, sort = TRUE) %>%
  slice_head(n = 10)

kable(nonhospital_promotion_entities, format = "markdown", caption = "Top 10 Entity Types Prior to C-Suite Promotion from Non-Hospital Entities")

kable(nonhospital_promotion_entities_persons, format = "markdown", caption = "Top 10 Entity Types Prior to C-Suite Promotion from Non-Hospital (Unique Individuals)")

# Top 10 roles held among all hospital C-suite execs (unique individuals)
current_c_suite_roles_persons <- roles_by_year %>%
  filter(any_hospital_c_suite == 1) %>%
  select(contact_uniqueid, roles_this_year) %>%
  unnest(roles_this_year) %>%
  distinct(contact_uniqueid, roles_this_year) %>%
  count(roles_this_year, sort = TRUE) %>%
  slice_head(n = 10)

kable(current_c_suite_roles_persons, format = "markdown", caption = "Top 10 Roles Held Among Hospital C-Suite Executives (Unique Individuals)")

###########
# WIP
# STEP 4: Identify within-c-suite transitions
# Only for people who were already in the c-suite
# Classify move types:
# - add a new role
# - completely change roles -- TODO
# the _consec definition restricts moves defined by cases where we have observations in 2 consecutive years. the other definition does not make this restriction and just compares to the most recent year
###########
top_csuite_roles <- c("CEO:  Chief Executive Officer", "Chief Nursing Head", "CFO: Chief Financial Officer", "Chief Compliance Officer", "CIO: Chief Information Officer", "COO: Chief Operating Officer", "Chief Medical Officer")
all_csuite_roles <- c("CEO:  Chief Executive Officer", "Chief Nursing Head", "CFO: Chief Financial Officer", "Chief Compliance Officer", "CIO: Chief Information Officer", "COO: Chief Operating Officer", "Chief Medical Officer", "CNIS:  Chief Nursing Informatics Officer", "Chief Medical Information Officer", "Chief Experience/Patient Engagement Officer")


# Step 1: Restrict roles_prev_year and roles_this_year to C-suite only
# roles_by_year <- roles_by_year %>%
#   mutate(
#     roles_prev_year_csuite = map(roles_prev_year, ~ intersect(.x, top_csuite_roles)),
#     roles_this_year_csuite = map(roles_this_year, ~ intersect(.x, top_csuite_roles))
#   )

# Step 2: Flag person-years with a real transition 
roles_by_year <- roles_by_year %>%
  mutate(
    real_transition = map2_lgl(roles_prev_year, roles_this_year, ~ !setequal(.x, .y))
  )

# Step 3: Then expand ONLY real transitions
transition_pairs <- roles_by_year %>%
  filter(real_transition) %>%
  mutate(
    from_role = map(roles_prev_year, unique),
    to_role = map(roles_this_year, unique)
  ) %>%
  unnest(from_role) %>%
  unnest(to_role) %>%
  filter(from_role != to_role)

# Step 5: Summarize actual transitions
transition_summary <- transition_pairs %>%
  group_by(from_role, to_role) %>%
  summarize(
    n_transitions_add = n(),
    .groups = "drop"
  )

