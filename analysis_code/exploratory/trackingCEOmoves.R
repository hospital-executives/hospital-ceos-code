###########
# Exploratory analysis: identifying hospital CEO promotions
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

# this is an example of a change

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
  hospital = entity_type %in% c("Hospital", "Single Hospital Health System"), 
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


# Filter to individuals who have ever been a CEO
# remove "CIO Reports to" as a role
df <- df %>%
  filter(ever_hospital_CEO & title_standardized != "CIO Reports to")

# convert back to tibble so we can use dplyr
df <- as_tibble(df)

# # create combined system ID, since entity_parentid is missing for single hospital systems and IDS/RHA
#   # entity_parentid is only missing in IDS/RHA and Single Hospital Health Systems
#   df %>%
#     filter(is.na(system_id)) %>%
#     count(entity_type)
# 
# df <- df %>%
#   mutate(
#     combined_parentid = if_else(!is.na(entity_parentid), entity_parentid, entity_uniqueid)
#   )
# 

###########
# STEP 1: Identify *first-time* CEO promotions 
# Definition: first year a person is observed with CEO title
###########
# Create a dataset with one row per person-year, listing all titles
roles_by_year <- df %>%
  group_by(contact_uniqueid, year) %>%
  summarize(roles_this_year = list(unique(title_standardized)), .groups = "drop") %>%
  arrange(contact_uniqueid, year)

# For each person, identify their role history
# - roles_prev_year = roles held in previous year
# - roles_all_prior = all roles in prior years (cumulative)
# - roles_gained = newly acquired roles this year
# - roles_lost = roles dropped this year
roles_by_year <- roles_by_year %>%
  mutate(roles_this_year = map(roles_this_year, as.character)) %>%
  group_by(contact_uniqueid) %>%
  mutate(
    roles_prev_year = lag(roles_this_year),
    roles_all_prior = lag(accumulate(roles_this_year, union) %>% map(identity)),
    roles_gained = map2(roles_this_year, roles_prev_year, ~ setdiff(.x, .y)),
    roles_lost   = map2(roles_prev_year, roles_this_year, ~ setdiff(.x, .y))
  ) %>%
  ungroup()

# Create an indicator for first-time CEO promotion
# This occurs if the person gains the CEO role in a year that is not their first observation
roles_by_year <- roles_by_year %>%
  group_by(contact_uniqueid) %>%
  arrange(year, .by_group = TRUE) %>%
  mutate(gained_ceo = map_lgl(
    roles_gained,
    ~ "CEO:  Chief Executive Officer" %in% .x 
  ) 
  & row_number() != 1)

###########
# STEP 2: Distinguish between internal vs. external promotions to CEO
# first, subset to CEO promotions that occur in year t
# then define internal vs. external promotion:
  # Internal: I am CEO at hospital A in year t and I was at hospital A in year t-1 (not as CEO)
  # External: I am CEO at hospital B in year t and I was not at hospital B in year t-1.

# the _consec definition restricts moves defined by cases where we have observations in 2 consecutive years. the other definition does not make this restriction and just compares to the most recent year
###########
# Get all entities person worked at in a year (any role)
all_entities_by_year <- df %>%
  mutate(entity_uniqueid = as.character(entity_uniqueid)) %>%
  group_by(contact_uniqueid, year) %>%
  summarize(entities = list(unique(entity_uniqueid)), .groups = "drop")


# Get CEO entities 
ceo_entities_by_year <- df %>%
  filter(title_standardized == "CEO:  Chief Executive Officer") %>%
  mutate(entity_uniqueid = as.character(entity_uniqueid)) %>%
  group_by(contact_uniqueid, year) %>%
  summarize(ceo_entities = list(unique(entity_uniqueid)), .groups = "drop")


# Join both sets of entities to roles_by_year and compute promotion type
roles_by_year <- roles_by_year %>%
  left_join(all_entities_by_year, by = c("contact_uniqueid", "year")) %>%
  left_join(ceo_entities_by_year, by = c("contact_uniqueid", "year")) %>%
  group_by(contact_uniqueid) %>%
  arrange(year, .by_group = TRUE) %>%
  mutate(
    # Coerce everything to list-columns of character
    entities = map(entities, ~ if (is.null(.x)) character(0) else as.character(.x)),
    ceo_entities = map(ceo_entities, ~ if (is.null(.x)) character(0) else as.character(.x)),
    
    year_prev = lag(year),
    entities_prev = lag(entities),
    # FIXED: ensure accumulate returns a proper list-column
    entities_prior_all = lag(accumulate(entities, union) %>% map(identity)), 
    entities_prev = map(entities_prev, ~ if (is.null(.x)) character(0) else .x),
    entities_prior_all = map(entities_prior_all, ~ if (is.null(.x)) character(0) else .x),
    
    # Definition 1: t-1 only
    promotion_type_consec = case_when(
      gained_ceo & year_prev == year - 1 & map2_lgl(ceo_entities, entities_prev, ~ any(.x %in% .y)) ~ "internal",
      gained_ceo & year_prev == year - 1 & map2_lgl(ceo_entities, entities_prev, ~ !any(.x %in% .y)) ~ "external",
      gained_ceo & year_prev != year - 1 ~ "missing year",
      TRUE ~ NA_character_
    ),
    # Definition 2: any prior year
    promotion_type = case_when(
      gained_ceo  & map2_lgl(ceo_entities, entities_prev, ~ any(.x %in% .y)) ~ "internal",
      gained_ceo  & map2_lgl(ceo_entities, entities_prev, ~ !any(.x %in% .y)) ~ "external",
      TRUE ~ NA_character_
    )
  ) %>%
  ungroup()


# Tabulate each separately
roles_by_year %>% filter(!is.na(promotion_type_consec)) %>% count(promotion_type_consec)
roles_by_year %>% filter(!is.na(promotion_type)) %>% count(promotion_type)

# Cross-tab to compare
roles_by_year %>%
  filter(!is.na(promotion_type_consec), !is.na(promotion_type)) %>%
  count(promotion_type_consec, promotion_type)

### What are the most common roles to be transitioning from?
# internal promotion
roles_by_year %>%
  filter(gained_ceo & promotion_type == "internal") %>%
  select( roles_prev_year) %>%
  unnest(roles_prev_year) %>%
  count(roles_prev_year, sort = TRUE)

# external
roles_by_year %>%
  filter(gained_ceo & promotion_type == "external") %>%
  select( roles_prev_year) %>%
  unnest(roles_prev_year) %>%
  count(roles_prev_year, sort = TRUE)


###########
# STEP 3: Identify CEO *moves* after initial promotion
# Only for people who were already a CEO (not first time)
# Classify move types:
# - internal: new CEO role at previously worked (non-CEO) hospital
# - lateral: new CEO role at new hospital, but keep existing CEO role
# - external: drop old CEO role(s), become CEO at brand new hospital

# the _consec definition restricts moves defined by cases where we have observations in 2 consecutive years. the other definition does not make this restriction and just compares to the most recent year
###########

roles_by_year <- roles_by_year %>%
  group_by(contact_uniqueid) %>%
  arrange(year, .by_group = TRUE) %>%
  mutate(
    row_in_person = row_number(),
    
    ceo_entities = map(ceo_entities, ~ if (is.null(.x)) character(0) else as.character(.x)),
    entities = map(entities, ~ if (is.null(.x)) character(0) else as.character(.x)),
    ceo_entities_prev = lag(ceo_entities),
    entities_prev = lag(entities),
    year_prev = lag(year),
    
    added_ceo_entities = map2(ceo_entities, ceo_entities_prev, ~ setdiff(.x, .y)),
    
    ceo_last_year = map_lgl(ceo_entities_prev, ~ length(.x) > 0),
    ceo_this_year = map_lgl(ceo_entities, ~ length(.x) > 0)
  )

roles_by_year <- roles_by_year %>%
  mutate(
    move_type = case_when(
      row_in_person > 1 & ceo_this_year & !gained_ceo & ceo_last_year &
        map_lgl(added_ceo_entities, ~ length(.x) > 0) &  # must have added new CEO entities
        map2_lgl(added_ceo_entities, entities_prev, ~ all(!.x %in% .y)) &
        map2_lgl(ceo_entities, ceo_entities_prev, ~ !any(.x %in% .y)) ~ "external",
      
      row_in_person > 1 & ceo_this_year & !gained_ceo & ceo_last_year  &
        map_lgl(added_ceo_entities, ~ length(.x) > 0) &
        map2_lgl(added_ceo_entities, entities_prev, ~ all(.x %in% .y)) ~ "internal",
      
      row_in_person > 1 & ceo_this_year & !gained_ceo & ceo_last_year &
        map_lgl(added_ceo_entities, ~ length(.x) > 0) &
        map2_lgl(added_ceo_entities, entities_prev, ~ any(!.x %in% .y)) &
        map2_lgl(ceo_entities, ceo_entities_prev, ~ any(.x %in% .y)) ~ "lateral",
      
      TRUE ~ NA_character_
    ),
    move_type_consec = case_when(
      row_in_person > 1 & ceo_this_year & !gained_ceo & ceo_last_year & year_prev == year - 1 &
        map_lgl(added_ceo_entities, ~ length(.x) > 0) &  # must have added new CEO entities
        map2_lgl(added_ceo_entities, entities_prev, ~ all(!.x %in% .y)) &
        map2_lgl(ceo_entities, ceo_entities_prev, ~ !any(.x %in% .y)) ~ "external",
      
      row_in_person > 1 & ceo_this_year & !gained_ceo & ceo_last_year & year_prev == year - 1 &
        map_lgl(added_ceo_entities, ~ length(.x) > 0) &
        map2_lgl(added_ceo_entities, entities_prev, ~ all(.x %in% .y)) ~ "internal",
      
      row_in_person > 1 & ceo_this_year & !gained_ceo & ceo_last_year & year_prev == year - 1 &
        map_lgl(added_ceo_entities, ~ length(.x) > 0) &
        map2_lgl(added_ceo_entities, entities_prev, ~ any(!.x %in% .y)) &
        map2_lgl(ceo_entities, ceo_entities_prev, ~ any(.x %in% .y)) ~ "lateral",
      
      TRUE ~ NA_character_
    )
  )

### Tabulate types of moves
roles_by_year %>%
  filter(!is.na(move_type)) %>%
  count(move_type)

roles_by_year %>%
  filter(!is.na(move_type_consec)) %>%
  count(move_type_consec)

###########
# STEP 4: combine moves and promotions together 
###########
roles_by_year <- roles_by_year %>%
  mutate(
    career_transition_type = case_when(
      gained_ceo & promotion_type == "internal" ~ "internal promotion",
      gained_ceo & promotion_type == "external" ~ "external promotion",
      !gained_ceo & move_type == "internal" ~ "internal move",
      !gained_ceo & move_type == "lateral" ~ "lateral move",
      !gained_ceo & move_type == "external" ~ "external move",
      TRUE ~ NA_character_
    )
  )

roles_by_year <- roles_by_year %>%
  mutate(
    career_transition_type_consec = case_when(
      gained_ceo & promotion_type_consec == "internal" ~ "internal promotion",
      gained_ceo & promotion_type_consec == "external" ~ "external promotion",
      !gained_ceo & move_type_consec == "internal" ~ "internal move",
      !gained_ceo & move_type_consec == "lateral" ~ "lateral move",
      !gained_ceo & move_type_consec == "external" ~ "external move",
      TRUE ~ NA_character_
    )
  )

###########
# STEP 5: create summary stats tables 
###########
# summarize total number of moves and promotions (allowing for non-consec years)
summary_tab <- roles_by_year %>%
  filter(!is.na(career_transition_type)) %>%
  group_by(career_transition_type) %>%
  summarize(
    n_transitions = n(),
    n_individuals = n_distinct(contact_uniqueid),
    .groups = "drop"
  ) %>%
  mutate(
    total_ceos = n_distinct(roles_by_year$contact_uniqueid),
    share_of_ceos = n_individuals / total_ceos,
    
    definition = case_when(
      career_transition_type == "internal promotion" ~ "First time becoming CEO at a hospital previously worked at (non-CEO)",
      career_transition_type == "external promotion" ~ "First time becoming CEO at a hospital never worked at before",
      career_transition_type == "internal move" ~ "Already CEO; add CEO role at a hospital previously worked at (non-CEO)",
      career_transition_type == "lateral move" ~ "Already CEO; add CEO role at a hospital never worked at before",
      career_transition_type == "external move" ~ "Drop previous CEO roles; become CEO at a hospital never worked at before"
    ),
    
    example = case_when(
      career_transition_type == "internal promotion" ~ "Year t−1: CFO at Hospital B → Year t: CEO at Hospital B",
      career_transition_type == "external promotion" ~ "Year t−1: No role at Hospital B → Year t: CEO at Hospital B",
      career_transition_type == "internal move" ~ "Year t−1: CEO at Hospital A, CFO at Hospital B → Year t: CEO at A and B",
      career_transition_type == "lateral move" ~ "Year t−1: CEO at Hospital A → Year t: CEO at A and B",
      career_transition_type == "external move" ~ "Year t−1: CEO at Hospital A → Year t: CEO at Hospital B (no longer CEO at A)"
    ),
    
    career_transition_type = factor(
      career_transition_type,
      levels = c("internal promotion", "external promotion",
                 "internal move", "lateral move", "external move")
    )
  ) %>%
  arrange(career_transition_type) %>%
  select(
    career_transition_type,
    definition,
    example,
    n_transitions,
    n_individuals,
    share_of_ceos
  )


kable(summary_tab, format = "markdown")

# summarize total number of moves and promotions (restricting only to moves/promotions observed in consecutive years)
summary_tab_consec <- roles_by_year %>%
  filter(!is.na(career_transition_type_consec)) %>%
  group_by(career_transition_type_consec) %>%
  summarize(
    n_transitions = n(),
    n_individuals = n_distinct(contact_uniqueid),
    .groups = "drop"
  ) %>%
  mutate(
    total_ceos = n_distinct(roles_by_year$contact_uniqueid),
    share_of_ceos = n_individuals / total_ceos,
    
    definition = case_when(
      career_transition_type_consec == "internal promotion" ~ "First time becoming CEO at a hospital previously worked at (non-CEO)",
      career_transition_type_consec == "external promotion" ~ "First time becoming CEO at a hospital never worked at before",
      career_transition_type_consec == "internal move" ~ "Already CEO; add CEO role at a hospital previously worked at (non-CEO)",
      career_transition_type_consec == "lateral move" ~ "Already CEO; add CEO role at a hospital never worked at before",
      career_transition_type_consec == "external move" ~ "Drop previous CEO roles; become CEO at a hospital never worked at before"
    ),
    
    example = case_when(
      career_transition_type_consec == "internal promotion" ~ "Year t−1: CFO at Hospital B → Year t: CEO at Hospital B",
      career_transition_type_consec == "external promotion" ~ "Year t−1: No role at Hospital B → Year t: CEO at Hospital B",
      career_transition_type_consec == "internal move" ~ "Year t−1: CEO at Hospital A, CFO at Hospital B → Year t: CEO at A and B",
      career_transition_type_consec == "lateral move" ~ "Year t−1: CEO at Hospital A → Year t: CEO at A and B",
      career_transition_type_consec == "external move" ~ "Year t−1: CEO at Hospital A → Year t: CEO at Hospital B (no longer CEO at A)"
    ),
    
    career_transition_type_consec = factor(
      career_transition_type_consec,
      levels = c("internal promotion", "external promotion",
                 "internal move", "lateral move", "external move")
    )
  ) %>%
  arrange(career_transition_type_consec) %>%
  select(
    career_transition_type_consec,
    definition,
    example,
    n_transitions,
    n_individuals,
    share_of_ceos
  )

kable(summary_tab_consec, format = "markdown")


