### AHA CEO to HIMSS left join

## libraries
library(rstudioapi)

rm(list = ls())

## set up scripts
if (rstudioapi::isAvailable()) {
  script_directory <- dirname(dirname(rstudioapi::getActiveDocumentContext()$path))
} else {
  script_directory <- params$code_dir
}
config_path <- file.path(script_directory, "config.R")
source(config_path)
rm(script_directory, config_path)

final_path <- paste0(derived_data, "/final_aha.feather")
final <- read_feather(final_path)
hospitals <- read_feather(paste0(derived_data,'/himss_aha_hospitals_final.feather'))

##### SET UP NAME INFRASTRUCTURE ####
names1 <- read.csv(paste0(supplemental_data, "/carltonnorthernnames.csv"), header = FALSE)
female <- read.csv(paste0(supplemental_data, "/female_diminutives.csv"), header = FALSE)
female_lower <- female
female_lower[] <- lapply(female_lower, function(x) {
  if (is.character(x)) tolower(x) else x
})
male <- read.csv(paste0(supplemental_data, "/male_diminutives.csv"), header = FALSE)
male_lower <- male
male_lower[] <- lapply(male_lower, function(x) {
  if (is.character(x)) tolower(x) else x
})

build_cooccurrence_dict <- function(df_all) {
  df_all_lower <- df_all %>% mutate(across(everything(), ~ tolower(as.character(.))))
  
  # Get all rows as sets of names
  rows_as_sets <- apply(df_all_lower, 1, unique)
  
  # Flatten into (name -> set of co-names)
  name_to_neighbors <- list()
  
  for (row in rows_as_sets) {
    for (name in row) {
      others <- setdiff(row, name)
      if (is.null(name_to_neighbors[[name]])) {
        name_to_neighbors[[name]] <- others
      } else {
        name_to_neighbors[[name]] <- union(name_to_neighbors[[name]], others)
      }
    }
  }
  
  name_to_neighbors
}

names_in_same_row_dict <- function(name1, name2, cooccur_dict) {
  name1 <- tolower(name1)
  name2 <- tolower(name2)
  !is.null(cooccur_dict[[name1]]) && name2 %in% cooccur_dict[[name1]]
}

dict1 <- build_cooccurrence_dict(names1)
dict2 <- build_cooccurrence_dict(female_lower)
dict3 <- build_cooccurrence_dict(male_lower)

last_name_overlap <- function(last_aha, last_himss) {
  last_aha <- tolower(last_aha)
  last_himss <- tolower(last_himss)
  
  grepl(last_aha, last_himss, fixed = TRUE) || grepl(last_himss, last_aha, fixed = TRUE)
}

#### load AHA data #####
columns_to_keep <- c( 
  "ID", 
  "YEAR", 
  "MNAME", 
  "HOSPN", 
  "MCRNUM",
  # "CBSANAME", 
  "HRRNAME", 
  "HSANAME", 
  "MADMIN", 
  "SYSID", 
  "SYSNAME", 
  "CNTRL", 
  "SERV", 
  "HOSPBD", 
  "BDTOT", 
  "ADMTOT", 
  "IPDTOT", 
  "MCRDC", 
  "MCRIPD", 
  "MCDDC", 
  "MCDIPD", 
  "BIRTHS", 
  "FTEMD", 
  "FTERN", 
  "FTE",
  "LAT",
  "LONG",
  "MLOCADDR", #street address
  "MLOCCITY", #city
  "MLOCZIP" #zip code
)

# set file path using manual input at beginning of script
file_path <- paste0(supplemental_data,"/AHA_2004_2017.csv")
#import the CSV
aha_data <- read_csv(file_path, col_types = cols(.default = col_character()))

# Use problems() function to identify parsing issues
parsing_issues <- problems(aha_data)

# Check if any parsing issues were detected
if (nrow(parsing_issues) > 0) {
  print(parsing_issues)
} else {
  print("No parsing problems detected.")
}

aha_data <- aha_data %>%
  select(all_of(columns_to_keep)) %>% 
  rename_all(tolower) %>% 
  rename(ahanumber = id,
         latitude_aha = lat,
         longitude_aha = long) %>% 
  mutate(
    # Convert to UTF-8
    mloczip = stri_trans_general(mloczip, "latin-ascii"),
    # cbsaname = stri_trans_general(cbsaname, "latin-ascii"),
    mlocaddr = stri_trans_general(mlocaddr, "latin-ascii") %>% 
      str_replace_all("-", " ") %>%  # Replace hyphens with a space
      str_remove_all("[^[:alnum:],.\\s]") %>%  # Remove other punctuation except commas, periods, and spaces
      str_to_lower() %>%
      str_squish(),  # Remove extra spaces
    mname = stri_trans_general(mname, "latin-ascii") %>% 
      str_replace_all("-", " ") %>%  # Replace hyphens with a space
      str_remove_all("[^[:alnum:],.\\s]") %>%  # Remove other punctuation except commas, periods, and spaces
      str_to_lower() %>%
      str_squish(),  # Remove extra spaces
    # Extract first five digits of the zip code
    mloczip_five = str_extract(mloczip, "^\\d{5}")
  )

aha_data <- aha_data %>%
  mutate(ahanumber = str_remove_all(ahanumber, "[A-Za-z]"),
         ahanumber = as.numeric(ahanumber),
         year = as.numeric(year),
         mcrnum = str_remove_all(mcrnum, "[A-Za-z]"),
         mcrnum = as.numeric(mcrnum))


#### clean aha and filter to only chief executives ####

# clean aha to extract names and titles from madmin
suffixes <- c("i", "ii", "iii", "iv", "v", "jr", "sr", "j.r.", "s.r.", "md", "rn")
cleaned_aha <- aha_data %>%
  mutate(madmin_original = madmin) %>%
  separate(madmin, into = c("aha_name", "title_aha"), 
           sep = ",", extra = "merge", fill = "right") %>%
  rename(madmin = madmin_original) %>%
  mutate(cleaned_title_aha = title_aha %>%
           str_to_lower() %>%
           str_replace_all("[[:punct:]]", "")) %>%
  mutate(full_aha = str_trim(str_remove_all(aha_name, "[[:punct:]]")),
         last_aha = if_else(
           str_to_lower(word(full_aha, -1)) %in% suffixes,
           word(full_aha, -2),
           word(full_aha, -1)),
         first_word = word(full_aha, 1),
         second_word = word(full_aha, 2),
         first_aha = if_else(nchar(first_word) == 1, second_word, first_word),
         first_aha = first_aha %>%
           str_to_lower() %>%
           str_replace_all("[[:punct:]]", ""),
         last_aha = last_aha %>%
           str_to_lower() %>%
           str_replace_all("[[:punct:]]", ""),
         full_aha = full_aha %>% str_to_lower()) %>% 
  select(-c(first_word, second_word)) 

## check titles 
title_check <- cleaned_aha %>% 
  count(cleaned_title_aha) 

temp_titles <- cleaned_aha %>% 
  filter(str_detect(cleaned_title_aha, "act|int\b|interim")) %>% 
  count(cleaned_title_aha) 

na_titles <- cleaned_aha %>% 
  filter(is.na(cleaned_title_aha)) %>%
  select(first_aha, last_aha, cleaned_title_aha,madmin,  mname, ahanumber, year)

# subset AHA to only (aha, year) observations that are in HIMSS
all_aha_ceos<- hospitals %>% distinct(ahanumber,year) %>% left_join(
  cleaned_aha %>% distinct(full_aha, ahanumber, year, cleaned_title_aha)) %>% 
  filter(year > 2008 & !is.na(full_aha))  %>%
  filter(str_detect(cleaned_title_aha, "ceo|chief executive")) %>% 
  distinct(full_aha, ahanumber, year)

aha_madmin_no_ceos <- hospitals %>% distinct(ahanumber,year) %>% left_join(
  cleaned_aha %>% distinct(full_aha, ahanumber, year)) %>% 
  filter(year > 2008 & !is.na(full_aha)) %>% 
  anti_join(all_aha_ceos)

# subset HIMSS 
all_himss_leadership <- final %>% 
  group_by(full_name, entity_aha, year) %>%
  slice(1) %>%
  distinct(full_name, entity_aha, year, title_standardized) %>% 
  rename(ahanumber = entity_aha)

### first pass - match AHA & HIMSS ceos on (aha, year) -----------

# merge on ahanumber and year
aha_year_mini <- aha_madmin_no_ceos %>% left_join(all_himss_leadership)

# get relevant variables from AHA and HIMSS dfs
aha_mini <- cleaned_aha %>% 
  distinct(full_aha, ahanumber, year, first_aha, last_aha)

himss_mini <- final %>% 
 rename(
    first_himss = firstname,
    last_himss = lastname) %>%
  distinct(full_name, first_himss, last_himss, year, entity_aha) %>%
  rename(ahanumber = entity_aha)

aha_year_df <- aha_year_mini %>% left_join(aha_mini) %>%
  left_join(himss_mini) %>% rename(full_himss = full_name)

# exact name match
exact_names <- aha_year_df %>% 
  filter(first_himss == first_aha & last_himss == last_aha)
exact_combos <- exact_names %>% distinct(ahanumber, full_aha, year)

# jw names match
aha_year_df <- aha_year_df %>% anti_join(exact_combos) %>%
  mutate(
  full_jw  = stringdist(full_aha , full_himss , method = "jw", p = 0.1),
  first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1),
  last_jw  = stringdist(last_aha , last_himss , method = "jw", p = 0.1),
  nick_1 = mapply(function(a, b) names_in_same_row_dict(a, b, dict1), first_aha, first_himss),
  nick_2 = mapply(function(a, b) names_in_same_row_dict(a, b, dict2), first_aha, first_himss),
  nick_3 = mapply(function(a, b) names_in_same_row_dict(a, b, dict3), first_aha, first_himss),
  last_substring = mapply(last_name_overlap, last_aha, last_himss),
  first_substring = mapply(last_name_overlap, first_aha, first_himss)
)

jw_matches <- aha_year_df %>% filter(
  (full_jw <= .1 |    
  (
    (last_jw <= .15 | last_substring) & 
    (first_jw <= .15 | first_substring | nick_1 | nick_2 | nick_3)
  )))

jw_combos <- jw_matches %>% distinct(ahanumber, full_aha, year)

name_combos <- rbind(exact_combos, jw_combos) %>%
  mutate(match_type = "jw")

### second pass - match AHA & HIMSS ceos on aha with full name merge --------
remaining_madmin <- aha_madmin_no_ceos %>% anti_join(name_combos) 
remaining_himss <- all_himss_leadership %>% rename(full_himss = full_name) %>%
  anti_join(exact_names %>% distinct(full_himss, ahanumber, year)) %>%
  anti_join(jw_matches %>% distinct(full_himss, ahanumber, year)) %>% 
  rename(full_name = full_himss) 

remaining_csuite <- remaining_himss %>%
  filter(str_detect(title_standardized, "CFO|CEO|COO|CIO|Chief Medical")) %>%
  distinct(full_name, ahanumber, year)
    
joined <- remaining_madmin %>% distinct(ahanumber, year, full_aha) %>%
  rename(full_name = full_aha) %>%
  stringdist_left_join(remaining_himss %>% distinct(full_name, ahanumber, year) ,
                       by = "full_name",
                       method = "jw",
                       max_dist = .2,
                       distance_col = "jw_full") 

cleaned_joined <- joined %>% filter(!is.na(full_name.y)) %>%
  rename(
    aha_aha = ahanumber.x,
    himss_aha = ahanumber.y,
    aha_year = year.x,
    himss_year = year.y,
    full_aha = full_name.x,
    full_himss = full_name.y
  ) %>%
  left_join(aha_mini) %>% 
  left_join(himss_mini %>% rename(full_himss = full_name)) %>%
  filter(!is.na(first_himss))

year_mismatches <- cleaned_joined %>% select(-year, -ahanumber) %>%
  mutate(
    ahanumber = aha_aha,
    full_jw  = stringdist(full_aha , full_himss , method = "jw", p = 0.1),
    first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1),
    last_jw  = stringdist(last_aha , last_himss , method = "jw", p = 0.1),
    nick_1 = mapply(function(a, b) names_in_same_row_dict(a, b, dict1), first_aha, first_himss),
    nick_2 = mapply(function(a, b) names_in_same_row_dict(a, b, dict2), first_aha, first_himss),
    nick_3 = mapply(function(a, b) names_in_same_row_dict(a, b, dict3), first_aha, first_himss),
    last_substring = mapply(last_name_overlap, last_aha, last_himss),
    first_substring = mapply(last_name_overlap, first_aha, first_himss)
  ) %>% 
  filter(abs(himss_year - aha_year) <= 10 & 
           himss_aha == aha_aha & 
           ((
             (last_jw <= .15 |last_substring) & 
               (nick_1 | nick_2 | nick_3|first_substring|first_jw <= .15)
           ) |  (jw_full <= .15))) %>% 
  group_by(full_aha, ahanumber, aha_year) %>%
  mutate(
    has_himss_year_equal = any(himss_year == aha_year, na.rm = TRUE),
    has_himss_after = any(himss_year > aha_year, na.rm = TRUE),
    has_himss_before = any(himss_year < aha_year, na.rm = TRUE),
    has_himss_after_no_match = has_himss_after & !has_himss_year_equal,
    has_himss_before_no_match = has_himss_before & !has_himss_year_equal
  ) %>%
  ungroup() %>%
  mutate(
    year_diff = abs(himss_year - aha_year),
    prefer_after = if_else(himss_year > aha_year, 1, 0)
  ) %>%
  group_by(full_aha, ahanumber, aha_year) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>% 
  rename(himss_after_aha = has_himss_after_no_match, 
         himss_before_aha = has_himss_before_no_match) %>%
  mutate(match_type = paste0("jw_year_", year_diff)) %>%
  rename(year = aha_year) %>%
  distinct(ahanumber, full_aha, year, match_type, himss_after_aha, himss_before_aha) 

jw_and_year_matches <- rbind(
  name_combos %>% mutate(himss_after_aha = NA, himss_before_aha = NA),
  year_mismatches
)


### third pass - find matches using merged HIMSS to AHA data -----------------
aha_df  <- cleaned_aha %>%
  anti_join(jw_and_year_matches %>% select(-match_type)) %>%
  anti_join(all_aha_ceos) %>%
  rename(aha_year  = year)
himss_df <- final %>% filter(!is.na(entity_aha)) %>%
  mutate(ahanumber = entity_aha) %>%
  rename(
    first_himss = firstname,
    last_himss = lastname,
    himss_full = full_name,
    himss_year = year)

within_2yr <- aha_df %>%
  left_join(himss_df, by = "ahanumber") %>%
  filter(abs(himss_year - aha_year) <= 5)

# calculate string similarity/nickname measures
within_2yr <- within_2yr %>% mutate(
  full_jw  = stringdist(full_aha , himss_full , method = "jw", p = 0.1),
  first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1),
  last_jw  = stringdist(last_aha , last_himss , method = "jw", p = 0.1),
  nick_1 = mapply(function(a, b) names_in_same_row_dict(a, b, dict1), first_aha, first_himss),
  nick_2 = mapply(function(a, b) names_in_same_row_dict(a, b, dict2), first_aha, first_himss),
  nick_3 = mapply(function(a, b) names_in_same_row_dict(a, b, dict3), first_aha, first_himss),
  last_substring = mapply(last_name_overlap, last_aha, last_himss),
  first_substring = mapply(last_name_overlap, first_aha, first_himss)
)

# helper that returns NA instead of Inf when everything is missing
min_dist <- function(x) if (all(is.na(x))) NA_real_ else min(x, na.rm = TRUE)

# summarize any match in the two year window
aha_with_min_dists <- within_2yr %>% filter(aha_year > 2008) %>%
  group_by(full_aha, ahanumber, aha_year, himss_year) %>%  
  summarise(
    min_full_jw  = min_dist(full_jw),
    min_first_jw = min_dist(first_jw),
    min_last_jw  = min_dist(last_jw),
    any_nick_1 = any(nick_1),
    any_nick_2 = any(nick_2),
    any_nick_3 = any(nick_3),
    any_last_substring = any(last_substring),
    any_first_substring = any(first_substring),
    .groups = "drop"
  )

# find matches within two years
matches <- aha_with_min_dists %>% 
  mutate(year_diff = abs(himss_year - aha_year),
         prefer_after = if_else(himss_year > aha_year, 1, 0)
  ) %>%
  filter(
    min_full_jw <= .1 |
      (
        (min_last_jw <= .15 | any_last_substring) & 
          (min_first_jw <= .15 | any_first_substring | any_nick_1 | any_nick_2 | any_nick_3)) 
  ) %>% 
  group_by(full_aha, ahanumber, aha_year) %>%
  mutate(
    has_himss_year_equal = any(himss_year == aha_year, na.rm = TRUE),
    has_himss_after = any(himss_year > aha_year, na.rm = TRUE),
    has_himss_before = any(himss_year < aha_year, na.rm = TRUE),
    has_himss_after_no_match = has_himss_after & !has_himss_year_equal,
    has_himss_before_no_match = has_himss_before & !has_himss_year_equal
  ) %>%
  ungroup() %>%
  mutate(
    year_diff = abs(himss_year - aha_year),
    prefer_after = if_else(himss_year > aha_year, 1, 0)
  ) %>%
  group_by(full_aha, ahanumber, aha_year) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>% 
  rename(himss_after_aha = has_himss_after_no_match, 
         himss_before_aha = has_himss_before_no_match) %>%
  mutate(match_type = paste0("jw_year_", year_diff)) %>%
  rename(year = aha_year) %>%
  distinct(ahanumber, full_aha, year, match_type, himss_after_aha, himss_before_aha) 

all_matches <- rbind(jw_and_year_matches, matches) %>%
  mutate(
    match_order = case_when(
      match_type == "jw" ~ 1,
      match_type == "title" ~ 2,
      str_detect(match_type, "^jw_year_\\d+$") ~ 2 + as.numeric(str_extract(match_type, "\\d+")),
      str_detect(match_type, "^jw_year_title_\\d+$") ~ 100 + as.numeric(str_extract(match_type, "\\d+")),
      TRUE ~ Inf
    )
  ) %>%
  group_by(full_aha, ahanumber, year) %>%
  slice_min(order_by = match_order, n = 1, with_ties = FALSE) %>%
  ungroup()

### fourth pass - -----
remaining <- aha_madmin_no_ceos %>% left_join(cleaned_aha) %>%
  distinct(ahanumber, year, mname, full_aha, last_aha, first_aha) %>%
  anti_join(all_matches %>% distinct(full_aha,ahanumber, year)) %>%
  rename(firstname = first_aha,
         lastname = last_aha,
         aha_year = year)  %>%
  filter(!is.na(firstname) & !is.na(lastname)) %>%
  stringdist_join(
    final %>% distinct(id, firstname, lastname, entity_name, campus_aha, 
                       entity_aha, title_standardized, title, year) %>%
      rename(himss_year = year)  %>%
      filter(!is.na(firstname) & !is.na(lastname))) %>%
  filter(!is.na(id))

cleaned_remaining <- remaining %>%
  mutate(first_jw = stringdist(firstname.x, firstname.y, method = "jw"),
         last_jw = stringdist(lastname.x, lastname.y, method = "jw"),
         nick_1 = mapply(function(a, b) names_in_same_row_dict(a, b, dict1), firstname.x, firstname.y),
         nick_2 = mapply(function(a, b) names_in_same_row_dict(a, b, dict2), firstname.x, firstname.y),
         nick_3 = mapply(function(a, b) names_in_same_row_dict(a, b, dict3), firstname.x, firstname.y),
         last_substring = mapply(last_name_overlap, lastname.x, lastname.y),
         first_substring = mapply(last_name_overlap, firstname.x, firstname.y))

aha_sysid <- cleaned_aha %>% distinct(ahanumber, year, sysid)

potential_matches <-  cleaned_remaining %>% rename(year = aha_year) %>%
  left_join(aha_sysid) %>%
  rename(aha_aha = ahanumber,
         aha_sysid = sysid,
         ahanumber = campus_aha,
         aha_year = year,
         year = himss_year) %>%
  left_join(aha_sysid) %>%
  rename(campus_aha = ahanumber,
         himss_sysid = sysid,
         himss_year = year) 

sys_matches <- potential_matches %>% filter(aha_sysid == himss_sysid)
sys_matches <- sys_matches %>%
  group_by(full_aha, aha_aha, aha_year) %>%
  mutate(
    has_himss_year_equal = any(himss_year == aha_year, na.rm = TRUE),
    has_himss_after = any(himss_year > aha_year, na.rm = TRUE),
    has_himss_before = any(himss_year < aha_year, na.rm = TRUE),
    has_himss_after_no_match = has_himss_after & !has_himss_year_equal,
    has_himss_before_no_match = has_himss_before & !has_himss_year_equal
  ) %>% ungroup() %>%
  mutate(year_diff = abs(himss_year - aha_year),
         prefer_after = if_else(himss_year > aha_year, 1, 0)) %>%
  group_by(full_aha, aha_aha, aha_year) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>%
  mutate(
    match_type = case_when(
      himss_year == aha_year ~ "sys",
      himss_year != aha_year ~ paste0("sys_year_", year_diff),
      TRUE ~ "unaccounted"
    )) %>% 
  rename(himss_after_aha = has_himss_after_no_match, 
         himss_before_aha = has_himss_before_no_match)

confirmed_matches <- rbind(all_matches %>% select(full_aha, ahanumber, year), 
                           sys_matches %>% select(full_aha, aha_aha, aha_year) %>%
                             rename(ahanumber = aha_aha, year = aha_year)) %>%
  rename(full_name = full_aha)  %>% distinct(ahanumber, full_name, year)


# get unaccounted for ceos
unmatched <- aha_madmin_no_ceos %>% anti_join(confirmed_matches) %>% 
  left_join(cleaned_aha %>% distinct(full_aha, ahanumber, year,cleaned_title_aha))

# get mapping between ahanumber and systems
all_missing_aha <- unmatched %>% distinct(ahanumber) %>% pull(ahanumber)
corresponding_system_ids <- final %>% filter(campus_aha %in% all_missing_aha) %>%
  distinct(ahanumber, parentid, year, system_id) %>% rename(himss_year = year) 

parent_df <- final %>% filter(parentid %in% corresponding_system_ids$parentid) %>%
  distinct(firstname, lastname, full_name, ahanumber, entity_aha, entity_name, year, title_standardized, title) %>%
  rename(himss_year = year,
         himss_full = full_name,
         first_himss = firstname,
         last_himss = lastname)

merged_df <- unmatched %>% 
  left_join(cleaned_aha %>% distinct(first_aha, last_aha, year, ahanumber)) %>%
  left_join(corresponding_system_ids %>% select(-parentid)) %>%
  left_join(parent_df)

df_with_sim <- merged_df %>% mutate(
  full_jw  = stringdist(full_aha , himss_full , method = "jw", p = 0.1),
  first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1),
  last_jw  = stringdist(last_aha , last_himss , method = "jw", p = 0.1),
  nick_1 = mapply(function(a, b) names_in_same_row_dict(a, b, dict1), first_aha, first_himss),
  nick_2 = mapply(function(a, b) names_in_same_row_dict(a, b, dict2), first_aha, first_himss),
  nick_3 = mapply(function(a, b) names_in_same_row_dict(a, b, dict3), first_aha, first_himss),
  last_substring = mapply(last_name_overlap, last_aha, last_himss),
  first_substring = mapply(last_name_overlap, first_aha, first_himss)
)

check_sim <- df_with_sim %>% 
  filter((full_jw <= .1 |    
            (
              (last_jw <= .15 | last_substring) & 
                (first_jw <= .15 | first_substring | nick_1 | nick_2 | nick_3)
            )))

himss_sys_matches <- check_sim %>%
  rename(aha_year = year) %>%
  group_by(full_aha, ahanumber, aha_year) %>%
  mutate(
    has_himss_year_equal = any(himss_year == aha_year, na.rm = TRUE),
    has_himss_after = any(himss_year > aha_year, na.rm = TRUE),
    has_himss_before = any(himss_year < aha_year, na.rm = TRUE),
    has_himss_after_no_match = has_himss_after & !has_himss_year_equal,
    has_himss_before_no_match = has_himss_before & !has_himss_year_equal
  ) %>% ungroup() %>%
  mutate(year_diff = abs(himss_year - aha_year),
         prefer_after = if_else(himss_year > aha_year, 1, 0)) %>%
  group_by(full_aha, ahanumber, aha_year) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>%
  mutate(
    match_type = case_when(
      str_detect(title_standardized, "CEO") & himss_year == aha_year ~ "sys",
      str_detect(title, "CEO") & himss_year == aha_year ~ "sys",
      str_detect(title_standardized, "CEO") & himss_year != aha_year ~ paste0("sys_year_", year_diff),
      str_detect(title, "CEO") & himss_year != aha_year ~ paste0("sys_year_", year_diff),
      himss_year != aha_year ~ paste0("sys_year_", year_diff),
      himss_year == aha_year ~ "sys_title",
      TRUE ~ "unaccounted"
    )) %>% 
  rename(himss_after_aha = has_himss_after_no_match, 
         himss_before_aha = has_himss_before_no_match)



final_matches <- rbind(all_matches %>% select(-match_order), 
                       sys_matches %>% select(full_aha, aha_aha, aha_year,
                                              match_type, himss_before_aha,
                                              himss_after_aha) %>%
                         rename(ahanumber = aha_aha, year = aha_year), 
                       himss_sys_matches %>% select(full_aha, ahanumber, aha_year,
                                                    match_type, himss_before_aha,
                                                    himss_after_aha) %>%
                         rename(year = aha_year)) %>%
  mutate(
    match_order = case_when(
      match_type == "jw" ~ 1,
      match_type == "title" ~ 2,
      str_detect(match_type, "^jw_year_\\d+$") ~ 2 + as.numeric(str_extract(match_type, "\\d+")),
      str_detect(match_type, "^jw_year_title_\\d+$") ~ 100 + as.numeric(str_extract(match_type, "\\d+")),
      match_type == "sys" ~ 1000,
      str_detect(match_type, "^sys_title$") ~ 1001,
      str_detect(match_type, "^sys_year$") ~ 1002,
      TRUE ~ Inf
    )
  ) %>%
  group_by(full_aha, ahanumber, year) %>%
  slice_min(order_by = match_order, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  mutate(
    match_type = str_replace_all(match_type, "year_|_0", "")
  )

cleaned_matches <- final_matches %>% inner_join(aha_madmin_no_ceos)

###### ggplot code #####
get_count <- cleaned_matches %>% distinct(full_aha,ahanumber, year) 
cat(nrow(get_count)/nrow(aha_madmin_no_ceos))

df_counts <- final_matches %>% 
  count(match_type) %>%
  mutate(percent = n / nrow(aha_madmin_no_ceos) * 100) %>% 
  filter(percent >= 1)

ggplot(df_counts, aes(x = reorder(match_type, -n), y = n)) +
  geom_col(fill = "steelblue") +
  geom_text(aes(label = paste0(round(percent, 1), "%")), vjust = -0.5) +
  labs(title = "Category Counts with % of Total", x = "Category", y = "Count") +
  theme_minimal()

### check cases that didn't merge