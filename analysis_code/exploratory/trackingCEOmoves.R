###########
# Exploratory analysis: identifying hospital CEO moves
# Author: Maggie Shi
# Last edited: 4/3/2025
###########

library(dplyr)
library(stringr)
library(haven)
library(purrr)

# Load data
df <- read_dta("/Users/mengdishi/Dropbox/hospital_ceos/_data/derived/final_confirmed.dta")  

# Sort 
df <- df %>%
  arrange(contact_uniqueid, year)

# Create variables
df <- df %>%
  mutate(
    CEO = str_detect(title_standardized, "CEO:"),
    hospital = entity_type %in% c("Hospital", "Single Hospital Health System"),
    hospital_CEO = CEO & hospital
  ) %>%
  group_by(contact_uniqueid) %>%
  mutate(
    ever_hospital_CEO = any(hospital_CEO)
  ) %>%
  ungroup()

# Filter to individuals who have ever been a CEO
df <- df %>%
  filter(ever_hospital_CEO)

###########
# Identify across-location moves
# Defined as: 
# 1. being observed in an entity in year t that you have *never* been observed in before
# 2. there is no overlap between the entities in year t and the entities in t -1
###########

# Aggregate locations per contact-year
df_loc <- df %>%
  group_by(contact_uniqueid, year) %>%
  summarize(locations = list(unique(entity_uniqueid)), .groups = "drop") %>%
  arrange(contact_uniqueid, year)

# For each contact ID x year, collect list of all locations in the previous year as well as all prior locations 
df_loc <- df_loc %>%
  mutate(locations = map(locations, as.character)) %>%
  group_by(contact_uniqueid) %>%
  mutate(
    prev_year_locations = lag(locations),
    all_prior_locations = lag(accumulate(locations, union) %>% map(identity))
  ) %>%
  ungroup()

#  Flag year-to-year moves
df_loc <- df_loc %>%
  mutate(
    move_year_to_year = pmap_lgl(
      list(locations, prev_year_locations, all_prior_locations),
      function(curr, prev, hist) {
        if (is.null(prev) || is.null(hist)) {
          return(FALSE)
        }
        all(!curr %in% hist) && length(intersect(curr, prev)) == 0
      }
    )
  )

#  Merge back in 
df_with_move_flag <- df %>%
  left_join(df_loc %>% select(contact_uniqueid, year, move_year_to_year),
            by = c("contact_uniqueid", "year"))


# Identify IDs who ever had a move
movers <- df_with_move_flag %>%
  group_by(contact_uniqueid) %>%
  summarize(ever_moved = any(move_year_to_year, na.rm = TRUE)) %>%
  filter(ever_moved) %>%
  pull(contact_uniqueid)

# Subset original data to just those individuals
df_movers <- df_with_move_flag %>%
  filter(contact_uniqueid %in% movers)

###########
# Among the movers, characterize what roles they gained vs. last relative to the previous year 
###########


  # Step 2: Create a dataset with one row per person-year, listing all titles
  roles_by_year <- df_movers %>%
    group_by(contact_uniqueid, year) %>%
    summarize(roles_this_year = list(unique(title_standardized)), .groups = "drop") %>%
    arrange(contact_uniqueid, year)
  
  # Step 3: For each person, create lagged and cumulative role lists
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

# merge the roles back in
df_movers <- df_movers %>%
  left_join(roles_by_year, by = c("contact_uniqueid", "year"))

# create variable for CEO across-entity move AND the person gained the CEO role
df_movers <- df_movers %>%
  mutate(ceo_promotion_in_move = move_year_to_year & map_lgl(
    roles_gained,
    ~ "CEO:  Chief Executive Officer" %in% .x
  )
)

## Observation:
# sometimes an individual is listed as the CEO in the free-text title ("title") but not in "title_standardized." We will need to fix this



View(df_movers %>%
  select(contact_uniqueid, firstname, lastname, year, entity_uniqueid, entity_name, entity_type, title_standardized, title, move_year_to_year, roles_gained, roles_lost, ceo_promotion_in_move) %>%
  arrange(contact_uniqueid, year))


