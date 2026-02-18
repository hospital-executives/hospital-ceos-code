library(dplyr)
library(haven)
library(rstudioapi)

rm(list = ls())
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("../build_code/config.R")
}

# load upstream data
hospital_xwalk <- read_stata(paste0(derived_data, "/temp/merged_ma_sysid_xwalk.dta"))
hospitals <- read_feather(paste0(derived_data, "/hospitals_final.feather"))
individuals <- read_feather(paste0(derived_data, "/individuals_final.feather"))
type <- read_stata(paste0(derived_data, "/temp/merged_ma_nonharmonized.dta")) %>%
                     distinct(entity_uniqueid, year, type)

# get only hospital ceos 
ceos <- individuals %>% 
  mutate(
    title_lower = tolower(gsub("[[:punct:]]", "", title)),
    char_ceo = str_detect(title_lower, "ceo|chief executive officer| c e o"),
    std_ceo = title_standardized == "CEO:  Chief Executive Officer",
  ) %>%
  filter(char_ceo|std_ceo) %>%
  group_by(entity_uniqueid, year) %>%
  mutate(
    has_std = any(std_ceo),
    has_ceo = any(char_ceo)) %>%
  ungroup() %>%
  mutate(
    is_ceo = case_when(
      has_std & std_ceo ~ TRUE,
      has_std & !std_ceo ~ FALSE,
      TRUE ~ std_ceo
    )
  ) %>%
  filter(is_ceo) %>%
  distinct(entity_uniqueid, year, contact_uniqueid)

# merge data
hosp_sample <- hospital_xwalk %>% left_join(type, by = c("entity_uniqueid", "year")) %>%
  filter(is_hospital == 1) %>%
  mutate(
    partofsample = type %in% c("General Medical","General Medical & Surgical","Critical Access")
  ) %>%
  group_by(entity_uniqueid) %>%
  mutate(ever_partofsample = any(partofsample)) %>%
  ungroup() %>%
  filter(ever_partofsample) %>%
  select(-ever_partofsample)

merged <- hosp_sample %>% left_join(ceos, by = c("entity_uniqueid", "year")) 

#### backfill missing 806 CEOs if possible

#### a) drop cases where we know the position is vacant ####
vacancies <- hospitals %>% distinct(entity_name, entity_city, entity_uniqueid, year, all_ceo, haentitytypeid, sysid, entity_aha)
merged_with_vacancies <- merged %>% left_join(vacancies, by = c("entity_uniqueid", "year"))

mini <- merged_with_vacancies %>% 
  distinct(entity_uniqueid, entity_name, entity_city, year, contact_uniqueid, all_ceo) %>%
  filter(is.na(contact_uniqueid) & all_ceo != "Vacant")

ggplot(mini, aes(x = factor(year), fill = factor(all_ceo))) +
  geom_bar() +
  labs(x = "Year", 
       y = "Number of Observations",
       fill = "CEO Status",
       title = "Observations per Year by CEO Status") +
  theme_minimal()

#### b) backfill cases if there is a gap in CEOs ####

# flag gaps
dropped <- merged %>% 
  distinct(entity_uniqueid, year, contact_uniqueid) %>%
  group_by(entity_uniqueid) %>%
  arrange(entity_uniqueid, year) %>%
  mutate(
    actual_lag_year = lag(year),
    actual_lead_year = lead(year),
    lag_year = year - 1,
    lead_year = year + 1,
    contact_lag = lag(contact_uniqueid),
    contact_lead = lead(contact_uniqueid),
    gap = (actual_lag_year != lag_year & !is.na(actual_lag_year)) | (actual_lead_year != lead_year & !is.na(actual_lead_year))
  ) %>%
  filter(any(gap)) %>%
  ungroup()
gap <- dropped %>% 
  group_by(entity_uniqueid) %>%
  filter(any(contact_lag == contact_lead & gap)) %>%
  ungroup()

# create dataframe with missing years and backfilled contacts
missing_years <- gap %>%
  group_by(entity_uniqueid) %>%
  arrange(entity_uniqueid, year) %>%
  filter(
    actual_lead_year > year + 1 & 
      contact_lead == contact_uniqueid &
      !is.na(contact_uniqueid)
  ) %>%
  mutate(
    missing_years = map2(year + 1, actual_lead_year - 1, seq)
  ) %>%
  unnest(missing_years) %>%
  select(
    entity_uniqueid,
    year = missing_years,
    contact_uniqueid
  ) %>%
  ungroup() %>%
  left_join(hospitals %>% 
            distinct(entity_uniqueid, year, entity_aha, haentitytypeid, sysid,bdtot)) %>%
  mutate(
    all_leader_flag = TRUE
  )

#### Combine dfs ####
original_matches_with_vacancies <- merged_with_vacancies %>%
  distinct(entity_uniqueid, year, aha_id, contact_uniqueid, haentitytypeid, sysid, all_ceo) %>%
  mutate(
    contact_uniqueid = ifelse(is.na(contact_uniqueid) & all_ceo == "Vacant", -100, contact_uniqueid)
  ) %>% filter(!is.na(contact_uniqueid)) %>% select(-all_ceo)

new_matches <- missing_years %>% rename(aha_id = entity_aha) %>% select(-bdtot, -all_leader_flag)

filled_individuals <- rbind(original_matches_with_vacancies, new_matches)

write_feather(filled_individuals, paste0(derived_data, "/temp/updated_individuals.feather"))

# remaining cases
remaining <- hosp_sample %>% anti_join(filled_individuals)

missing_pre_2012 <- remaining %>%
  group_by(entity_uniqueid) %>%
  filter(any(year == 2009) & any(year == 2010) & any(year == 2011)) %>%
  ungroup() %>%
  filter(year %in% c(2009,2010, 2011)) %>%
  left_join(vacancies, by = c("entity_uniqueid", "year"))

missing_2012_ids <- missing_pre_2012 %>% distinct(entity_uniqueid) %>% pull(entity_uniqueid)

# check for tar/control breakdown
event_df <- hospital_xwalk %>% distinct(entity_uniqueid, year, tar) %>%
  group_by(entity_uniqueid) %>%
  arrange(entity_uniqueid, year) %>%
  mutate(tar_year = year[which(tar == 1)[1]]) %>%
  ungroup() %>%
  filter(entity_uniqueid %in% missing_2012_ids) %>%
  mutate(
    bal_2_yr = tar_year >= 2011 & tar_year <= 2015,
    control = is.na(tar_year)
  ) %>%
  filter(bal_2_yr | control) %>%
  distinct(entity_uniqueid, tar, bal_2_yr, control, tar_year)

leftovers <- event_df <- hospital_xwalk %>% distinct(entity_uniqueid, year, tar) %>%
  group_by(entity_uniqueid) %>%
  arrange(entity_uniqueid, year) %>%
  mutate(tar_year = year[which(tar == 1)[1]]) %>%
  ungroup() %>%
  filter(entity_uniqueid %in% missing_2012_ids) %>%
  mutate(
    bal_2_yr = tar_year >= 2011 & tar_year <= 2015,
    control = is.na(tar_year)
  ) %>%
  filter(!bal_2_yr & !control) %>%
  distinct(entity_uniqueid, tar, bal_2_yr, control, tar_year)

## missing from julia
missing_2012_df <- read_stata(paste0(derived_data, "/temp/newfacilities_2012.dta")) 
missing_2012 <- missing_2012_df %>% distinct(entity_uniqueid) %>% pull(entity_uniqueid)

merged_missing <- filled_individuals %>% filter(entity_uniqueid %in% missing_2012) %>%
  filter(year < 2012) %>%
  filter(is.na(contact_uniqueid))

test_corporate <- vacancies %>% filter(entity_uniqueid %in% missing_2012) %>%
  filter(year < 2012 & year > 2008)

aha_clean <- read_csv("../build_code/temp/cleaned_aha_madmin.csv") %>%
  distinct(ahanumber,year, madmin, cleaned_title_aha, full_aha, first_aha, last_aha) %>%
  rename(entity_aha = ahanumber)

merged_df <- test_corporate %>% left_join(aha_clean, by = c('entity_aha', 'year'))

see_matches <- read_feather("../build_code/temp/himss_to_aha_matches.feather") %>%
  rename(entity_aha = ahanumber)
merge_matches <- test_corporate %>% left_join(see_matches, by = c('entity_aha', 'year')) 

check_merge <- merged_df %>% filter(entity_uniqueid == 37476 & year == 2010)
check_indiv <- individuals %>% filter(id == 3985724) %>% distinct(entity_uniqueid, year, full_name, title, title_standardized)

## check missing
check_ceo <- individuals %>%
  filter(entity_uniqueid == 14881) %>%
  distinct(entity_uniqueid, entity_name, entity_city, year, confirmed, contact_uniqueid, firstname, lastname, title, title_standardized) 

#### get number for ambar ####
akm_estimate <- merged %>%
  distinct(aha_id, entity_uniqueid, year, contact_uniqueid) %>%
  group_by(contact_uniqueid, entity_uniqueid) %>%
  filter(n_distinct(year) >= 2) %>%
  ungroup() %>% 
  group_by(contact_uniqueid) %>%
  filter(n_distinct(entity_uniqueid) >= 2) %>%
  ungroup()

valid_contacts <- merged %>%
  mutate(contact_uniqueid = as.character(contact_uniqueid)) %>%
  filter(aha_id != "") %>%
  # Keep only entity-contact pairs with 2+ years
  group_by(contact_uniqueid, entity_uniqueid) %>%
  filter(n_distinct(year) >= 2) %>%
  summarise(years = list(unique(year)), .groups = "drop") %>%
  # Check for non-overlapping years
  group_by(contact_uniqueid) %>%
  filter(n() >= 2) %>%  # At least 2 entities
  summarise(
    n_entities = n(),
    all_year_sets = list(years),
    .groups = "drop"
  ) %>%
  # Check if any year sets overlap
  mutate(
    has_overlap = map_lgl(all_year_sets, function(year_sets) {
      if(length(year_sets) < 2) return(TRUE)
      for(i in 1:(length(year_sets)-1)) {
        for(j in (i+1):length(year_sets)) {
          if(length(intersect(year_sets[[i]], year_sets[[j]])) > 1) {
            return(TRUE)  # More than 1 year overlap - not allowed
          }
        }
      }
      return(FALSE)  # 0 or 1 year overlap - allowed
    })
  ) %>%
  filter(!has_overlap) %>%
  pull(contact_uniqueid)

df_filtered <- merged %>%
  mutate(contact_uniqueid = as.character(contact_uniqueid)) %>%
  filter(contact_uniqueid %in% valid_contacts)

cat(n_distinct(df_filtered$contact_uniqueid))

test_indiv <- individuals %>%
  distinct(haentitytypeid, entity_uniqueid, entity_name, entity_aha, year, title, title_standardized, contact_uniqueid, firstname) %>%
  #filter(entity_aha == 6211350)
  filter(contact_uniqueid == 100820)

test_hosp <- merged %>%
  distinct(entity_uniqueid, aha_id, year, contact_uniqueid) %>%
  filter(contact_uniqueid == 76841)

## check other roles
selected_entities <- vacancies_df %>%
  filter(entity_uniqueid %in% c(37413,37517))