
rm(list = ls())
library(rstudioapi)
library(tictoc)
library(xtable)
library(dplyr)
library(purrr)
library(stringdist)

args <- commandArgs(trailingOnly = TRUE)

# load data
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("config.R")
  hospitals <- read_feather(paste0(derived_data, "/hospitals_with_xwalk.feather"))
  final <- read_feather(paste0(derived_data, "/individuals_with_xwalk.feather"))
  supp_path <- supplemental_data
  output_dir <- paste0(data_file_path, "/summary_stats/execs")
} else {
  source("config.R")
hospitals <- read_feather(args[1])
final <- read_feather(args[2])
supp_path <- args[3]
output_dir <- args[4] 
}

summary_file <- paste0(output_dir, "/ceo_summary.tex")
if (file.exists(summary_file)) {
  file.remove(summary_file)
}

names1 <- read.csv(paste0(supp_path, "/carltonnorthernnames.csv"), header = FALSE)
female <- read.csv(paste0(supp_path, "/female_diminutives.csv"), header = FALSE)
male <- read.csv(paste0(supp_path, "/male_diminutives.csv"), header = FALSE)

##### SET UP NAME INFRASTRUCTURE ####
source("helper.R")

female_lower <- female
female_lower[] <- lapply(female_lower, function(x) {
  if (is.character(x)) tolower(x) else x
})
male_lower <- male
male_lower[] <- lapply(male_lower, function(x) {
  if (is.character(x)) tolower(x) else x
})

dict1 <- build_cooccurrence_dict(names1)
dict2 <- build_cooccurrence_dict(female_lower)
dict3 <- build_cooccurrence_dict(male_lower)

#### load cleaned aha data ####
cleaned_aha <- read.csv("temp/cleaned_aha_madmin.csv") %>%
  filter(str_detect(cleaned_title_aha, "ceo|chief executive")) 

# subset AHA to only (aha, year) observations that are in HIMSS
all_aha_ceos <- hospitals %>% distinct(ahanumber,year) %>% left_join(
  cleaned_aha %>% distinct(full_aha, ahanumber, year)) %>% 
  filter(year > 2008 & !is.na(full_aha)) %>%
  rename(full_name = full_aha)

# subset HIMSS to only observations that are CEOs
all_himss_ceos <- final %>% 
  filter(ceo_himss_title_fuzzy) %>% 
  distinct(full_name, entity_aha, year) %>% 
  rename(ahanumber = entity_aha)

#### compare himss vs aha madmin first ####
# process himss madmin column
suffixes <- c("i", "ii", "iii", "iv", "v", "jr", "sr", "j.r.", "s.r.", "md", "rn")

himss_processed <- final %>% filter(!is.na(entity_aha)) %>%
  separate(madmin, into = c("aha_name", "title_aha"), 
           sep = ",", extra = "merge", fill = "right") %>%
  mutate(cleaned_title_aha = title_aha %>%
           str_to_lower() %>%
           str_replace_all("[[:punct:]]", "")) %>%
  filter(str_detect(cleaned_title_aha, "ceo|chief executive")) %>% 
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
  select(-c(first_word, second_word))  %>%
  mutate(ahanumber = entity_aha)

# prep df for rowwise string sim
chunk_size <- 100000L
nick1_vec <- Vectorize(function(a, b) names_in_same_row_dict(a, b, dict1), SIMPLIFY = FALSE)
nick2_vec <- Vectorize(function(a, b) names_in_same_row_dict(a, b, dict2), SIMPLIFY = FALSE)
nick3_vec <- Vectorize(function(a, b) names_in_same_row_dict(a, b, dict3), SIMPLIFY = FALSE)
overlap_vec <- Vectorize(last_name_overlap, SIMPLIFY = FALSE)

# pre-process df
base_df <- himss_processed %>%
  rename(
    first_himss = firstname,
    last_himss  = lastname,
    himss_full  = full_name
  ) %>%
  distinct(
    first_himss, last_himss, himss_full, first_aha, last_aha, full_aha, title,
    title_standardized, cleaned_title_aha, mname, entity_name, year, ahanumber,
    entity_aha, campus_aha, unfiltered_campus_aha, id,
    ceo_himss_title_exact, ceo_himss_title_fuzzy, ceo_himss_title_general
  ) %>%
  arrange(id)

n <- nrow(base_df)
idx <- split(seq_len(n), ceiling(seq_len(n) / chunk_size))

# create df with all rowwise sim
valid_himss <- map_dfr(idx, function(i) {
  out <- process_chunk(base_df[i, , drop = FALSE])
  gc()
  out
})

# get cases where there is a title mismatch (e.g., not a CEO in HIMSS)
title_mismatch <- valid_himss %>% filter(
  full_jw <= .15 |
    (
      (last_jw <= .15 | last_substring) & 
        (first_jw <= .15 | first_substring | nick_1 | nick_2 | nick_3)) 
) %>% 
  mutate(match_type = ifelse(ceo_himss_title_fuzzy, "jw", "title"))

matched_himss <- title_mismatch %>% distinct(ahanumber, year, id, match_type)
aha_matches <- title_mismatch %>% distinct(ahanumber, year, full_aha, match_type)

### second pass - match AHA & HIMSS ceos on full name -------------------------

# get nonmatched ceos
remaining_aha_ceos <- all_aha_ceos %>% 
  anti_join(aha_matches %>% rename(full_name = full_aha))

# join ceos on full name
joined <- remaining_aha_ceos %>%
  stringdist_left_join(all_himss_ceos,
                       by = "full_name",
                       method = "jw",
                       max_dist = .2,
                       distance_col = "jw_full") 

# get relevant variables from AHA and HIMSS dfs
aha_mini <- cleaned_aha %>% 
  distinct(full_aha, ahanumber, year, first_aha, last_aha) %>%
  rename(aha_year = year,
         aha_aha = ahanumber)

himss_mini <- final %>% 
  filter(ceo_himss_title_fuzzy) %>% 
  rename(
    first_himss = firstname,
    last_himss = lastname,
    full_himss = full_name
  ) %>%
  distinct(full_himss, first_himss, last_himss, year, entity_aha, id) %>%
  rename(himss_aha = entity_aha,
         himss_year = year)

cleaned_joined <- joined %>%
  rename(
    aha_aha = ahanumber.x,
    himss_aha = ahanumber.y,
    aha_year = year.x,
    himss_year = year.y,
    full_aha = full_name.x,
    full_himss = full_name.y
  ) %>%
  left_join(aha_mini) %>% left_join(himss_mini)

# verify that number of combos matches aha
total_combos <- cleaned_joined %>% distinct(full_aha, aha_year, aha_aha)
stopifnot(nrow(total_combos) == nrow(remaining_aha_ceos))

# get name similarities
fuzzy_names_ceos <- cleaned_joined %>%
  mutate(last_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1),
         first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1)) %>%
  rowwise() %>%
  mutate(
    nick_1 = names_in_same_row_dict(first_aha, first_himss, dict1),
    nick_2 = names_in_same_row_dict(first_aha, first_himss, dict2),
    nick_3 = names_in_same_row_dict(first_aha, first_himss, dict3),
    last_substring = last_name_overlap(last_aha, last_himss),
    first_substring = last_name_overlap(first_aha, first_himss)
  ) %>%
  ungroup() %>%
  filter(abs(himss_year - aha_year) <= 10 & 
           himss_aha == aha_aha & 
           ((
             (last_jw <= .15 |last_substring) & 
               (nick_1 | nick_2 | nick_3|first_substring|first_jw <= .15)
           ) |  
             (jw_full <= .15))) %>%
  arrange(full_aha, jw_full) %>%
  group_by(full_aha, aha_aha, aha_year) %>%
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
    prefer_after = if_else(himss_year > aha_year, 1, 0),
    match_type = paste0("jw_year_", year_diff)
  ) 

# save (ahanumber, aha_year) : (himss id) matches
matched_himss <- matched_himss %>% 
  rbind(
    fuzzy_names_ceos %>%
    rename(ahanumber = aha_aha, year = aha_year) %>%
    distinct(ahanumber, year, id, match_type)
  ) %>%
  distinct()

year_mismatches <- fuzzy_names_ceos %>%
  group_by(full_aha, aha_aha, aha_year) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>% 
  rename(himss_after_aha = has_himss_after_no_match, 
         himss_before_aha = has_himss_before_no_match) %>%
  select(-year_diff, -prefer_after,-has_himss_after,-has_himss_before) %>%
  rename(ahanumber = aha_aha, year = aha_year) %>%
  distinct(ahanumber, year, full_aha, match_type, himss_before_aha, himss_after_aha)

# add columns to previous matches for merge
aha_matches <- aha_matches %>% mutate(himss_after_aha = NA, himss_before_aha = NA)
cols <- c("full_aha", "ahanumber", "year", "match_type", 
          "himss_before_aha", "himss_after_aha")
accounted_for <- bind_rows(
  aha_matches[cols], year_mismatches[cols]
)

### third pass - find matches where AHA observation occurs in HIMSS, but -----
###              not in the exact year ---------------------------------------

# left join AHA with HIMSS, keeping only observations in HIMSS within 5 years
# of the AHA
aha_df <- cleaned_aha %>%
  anti_join(
    accounted_for %>%
      filter(!str_detect(match_type, "\\d")) %>%
      select(-match_type),
    by = intersect(names(cleaned_aha), names(accounted_for))
  ) %>%
  rename(aha_year = year)

himss_df <- final %>% filter(!is.na(entity_aha)) %>%
  mutate(ahanumber = entity_aha) %>%
  rename(
    first_himss = firstname,
    last_himss = lastname,
    himss_full = full_name,
    himss_year = year)

within_5yr <- aha_df %>%
  left_join(himss_df, by = "ahanumber") %>%
  filter(abs(himss_year - aha_year) <= 5)

# calculate string similarity/nickname measures
within_5yr <- within_5yr %>% mutate(
  full_jw  = stringdist(full_aha , himss_full , method = "jw", p = 0.1),
  first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1),
  last_jw  = stringdist(last_aha , last_himss , method = "jw", p = 0.1),
  nick_1 = mapply(function(a, b) names_in_same_row_dict(a, b, dict1), first_aha, first_himss),
  nick_2 = mapply(function(a, b) names_in_same_row_dict(a, b, dict2), first_aha, first_himss),
  nick_3 = mapply(function(a, b) names_in_same_row_dict(a, b, dict3), first_aha, first_himss),
  last_substring = mapply(last_name_overlap, last_aha, last_himss),
  first_substring = mapply(last_name_overlap, first_aha, first_himss)
)

matches <- within_5yr %>% filter(aha_year > 2008) %>%
  mutate(
    full_condition = (full_jw <= 0.15 & !is.na(full_jw)),
    last_condition = ((last_jw <= 0.15 & !is.na(last_jw)) | last_substring),
    first_condition = (first_jw <= 0.15 & !is.na(first_jw)) | first_substring |
      nick_1 | nick_2 | nick_3,
    similarity_condition = full_condition | (first_condition & last_condition)
  ) %>%
  group_by(full_aha, ahanumber, aha_year, himss_year) %>%  
  filter(any(similarity_condition)) %>%
  ungroup() %>%
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
    prefer_after = if_else(himss_year > aha_year, 1, 0),
    match_type = case_when(
      ceo_himss_title_fuzzy ~ paste0("jw_year_", year_diff),
      TRUE ~ paste0("jw_year_title_", year_diff)
    )
  ) 

matches_aha <- matches %>%
  group_by(full_aha, ahanumber, aha_year) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>% 
  rename(himss_after_aha = has_himss_after_no_match, 
         himss_before_aha = has_himss_before_no_match) %>%
  rename(year = aha_year) %>%
  distinct(ahanumber, full_aha, year, match_type, himss_after_aha, himss_before_aha) 

matched_himss <- matched_himss %>% 
  rbind(
    matches %>%
      rename(year = aha_year) %>%
      distinct(ahanumber, year, id, match_type)
  ) %>%
  distinct()

all_matches <- rbind(accounted_for, matches_aha) %>%
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

#### clear environment ####
keep_names <- c(
  "all_aha_ceos", "cleaned_aha", "all_matches", "matched_himss",
  "final",
  "dict1", "dict2", "dict3",
  "names_in_same_row_dict", "last_name_overlap",
  "auxiliary_data", "output_dir", "summary_file"
)
rm(list = setdiff(ls(envir = .GlobalEnv), keep_names), envir = .GlobalEnv)

### fourth pass - find matches where AHA observation occurs in HIMSS, but ------
###              in a different system, using a (first, last) fuzzy join ------
###              and using system id from AHA 
himss_preprocessed <- final %>% 
  distinct(id, firstname, lastname, entity_name, campus_aha, 
          entity_aha, title_standardized, title, year, ceo_himss_title_fuzzy) %>%
  rename(himss_year = year)  %>%
  filter(!is.na(firstname) & !is.na(lastname))

remaining <- all_aha_ceos %>% left_join(cleaned_aha) %>%
  distinct(ahanumber, year, mname, full_aha, last_aha, first_aha) %>%
  anti_join(all_matches %>% distinct(full_aha,ahanumber, year)) %>%
  rename(firstname = first_aha,
         lastname = last_aha,
         aha_year = year)  %>%
  filter(!is.na(firstname) & !is.na(lastname)) %>%
  stringdist_join(himss_preprocessed, by = c("firstname", "lastname")) %>%
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

aha_sys_matches <- potential_matches %>% filter(aha_sysid == himss_sysid) %>%
  group_by(full_aha, aha_aha, aha_year) %>%
  mutate(
    has_himss_year_equal = any(himss_year == aha_year, na.rm = TRUE),
    has_himss_after = any(himss_year > aha_year, na.rm = TRUE),
    has_himss_before = any(himss_year < aha_year, na.rm = TRUE),
    has_himss_after_no_match = has_himss_after & !has_himss_year_equal,
    has_himss_before_no_match = has_himss_before & !has_himss_year_equal
  ) %>% ungroup() %>%
  mutate(year_diff = abs(himss_year - aha_year),
         prefer_after = if_else(himss_year > aha_year, 1, 0),
         match_type = case_when(
           ceo_himss_title_fuzzy & himss_year == aha_year ~ "sys",
           ceo_himss_title_fuzzy & himss_year != aha_year ~ paste0("sys_year_", year_diff),
           !ceo_himss_title_fuzzy & himss_year != aha_year ~ paste0("sys_title_year_", year_diff),
           !ceo_himss_title_fuzzy & himss_year == aha_year ~ "sys_title",
           TRUE ~ "unaccounted"
         )) 

if (any(aha_sys_matches$match_type == "unaccounted", na.rm = TRUE)) {
  stop("Some matches are unaccounted for")
}

aha_sys_matches_aha <- aha_sys_matches %>%
  group_by(full_aha, aha_aha, aha_year) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>%
  rename(himss_after_aha = has_himss_after_no_match, 
         himss_before_aha = has_himss_before_no_match)

matched_himss <- matched_himss %>% 
  rbind(
    aha_sys_matches %>%
      rename(year = aha_year, ahanumber = aha_aha) %>%
      distinct(ahanumber, year, id, match_type)
  ) %>%
  distinct()

confirmed_matches <- rbind(all_matches %>% select(full_aha, ahanumber, year), 
                           aha_sys_matches_aha %>% select(full_aha, aha_aha, aha_year) %>%
                             rename(ahanumber = aha_aha, year = aha_year)) %>%
  rename(full_name = full_aha)  %>% distinct(ahanumber, full_name, year)

# get unaccounted for ceos
unmatched <- all_aha_ceos %>% anti_join(confirmed_matches) %>% 
  left_join(cleaned_aha %>% distinct(full_aha, ahanumber, year,cleaned_title_aha))

### fifth pass - find matches where AHA observation occurs in HIMSS, but ------
###              in a different system, using a full name fuzzy join ------
###              and using system id from HIMSS
# get mapping between ahanumber and systems

all_missing_aha <- unmatched %>% distinct(ahanumber) %>% pull(ahanumber)
corresponding_system_ids <- final %>% filter(campus_aha %in% all_missing_aha) %>%
  distinct(ahanumber, parentid, year, system_id) %>% rename(himss_year = year) 

parent_df <- final %>% filter(parentid %in% corresponding_system_ids$parentid) %>%
  distinct(firstname, lastname, full_name, ahanumber, entity_aha, entity_name, year, 
           title_standardized, title, ceo_himss_title_fuzzy, id) %>%
  rename(himss_year = year,
         himss_full = full_name,
         first_himss = firstname,
         last_himss = lastname)

himss_sys_matches <- unmatched %>% 
  left_join(cleaned_aha %>% distinct(first_aha, last_aha, year, ahanumber)) %>%
  left_join(corresponding_system_ids %>% select(-parentid)) %>%
  left_join(parent_df)  %>% 
  # add similarity measures
  mutate(
    full_jw  = stringdist(full_aha , himss_full , method = "jw", p = 0.1),
    first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1),
    last_jw  = stringdist(last_aha , last_himss , method = "jw", p = 0.1),
    nick_1 = mapply(function(a, b) names_in_same_row_dict(a, b, dict1), first_aha, first_himss),
    nick_2 = mapply(function(a, b) names_in_same_row_dict(a, b, dict2), first_aha, first_himss),
    nick_3 = mapply(function(a, b) names_in_same_row_dict(a, b, dict3), first_aha, first_himss),
    last_substring = mapply(last_name_overlap, last_aha, last_himss),
    first_substring = mapply(last_name_overlap, first_aha, first_himss)
) %>%
  filter(
    (full_jw <= .1) |    
    (last_jw <= .15 | last_substring) & (first_jw <= .15 | first_substring | nick_1 | nick_2 | nick_3)
  ) %>%
  rename(aha_year = year) %>%
  mutate(
    year_diff = abs(himss_year - aha_year),
    prefer_after = if_else(himss_year > aha_year, 1, 0),
    match_type = case_when(
      ceo_himss_title_fuzzy & himss_year == aha_year ~ "sys",
      ceo_himss_title_fuzzy & himss_year != aha_year ~ paste0("sys_year_", year_diff),
      !ceo_himss_title_fuzzy & himss_year != aha_year ~ paste0("sys_title_year_", year_diff),
      !ceo_himss_title_fuzzy & himss_year == aha_year ~ "sys_title",
      TRUE ~ "unaccounted"
    ))

if (any(himss_sys_matches$match_type == "unaccounted", na.rm = TRUE)) {
  stop("Some matches are unaccounted for")
}

matched_himss <- matched_himss %>% 
  rbind(
    himss_sys_matches %>%
      rename(year = aha_year) %>%
      distinct(ahanumber, year, id, match_type)
  ) %>%
  distinct()

himss_sys_matches_aha <- himss_sys_matches %>%
  group_by(full_aha, ahanumber, aha_year) %>%
  mutate(
    has_himss_year_equal = any(himss_year == aha_year, na.rm = TRUE),
    has_himss_after = any(himss_year > aha_year, na.rm = TRUE),
    has_himss_before = any(himss_year < aha_year, na.rm = TRUE),
    has_himss_after_no_match = has_himss_after & !has_himss_year_equal,
    has_himss_before_no_match = has_himss_before & !has_himss_year_equal
  ) %>% 
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>%
  rename(himss_after_aha = has_himss_after_no_match, 
         himss_before_aha = has_himss_before_no_match)

##### clean and export aha matches #####
final_matches <- rbind(all_matches %>% select(-match_order), 
                       aha_sys_matches_aha %>% select(full_aha, aha_aha, aha_year,
                                              match_type, himss_before_aha,
                                              himss_after_aha) %>%
                         rename(ahanumber = aha_aha, year = aha_year), 
                       himss_sys_matches_aha %>% select(full_aha, ahanumber, aha_year,
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

cleaned_matches <- final_matches %>% inner_join(all_aha_ceos)
write_feather(cleaned_matches, paste0(auxiliary_data, "/matched_aha_ceos.feather"))

# save himss matches 
write_feather(matched_himss, paste0(auxiliary_data, "/matched_aha_ceos_himss.feather"))

###### ggplot code #####
get_count <- cleaned_matches %>% distinct(full_aha,ahanumber, year) 
cat(nrow(get_count), file = summary_file, append = TRUE)
cat(nrow(get_count)/nrow(all_aha_ceos), file = summary_file, append = TRUE)

df_counts <- final_matches %>% 
  count(match_type) %>%
  mutate(percent = n / nrow(all_aha_ceos) * 100) 

print(
  xtable(df_counts, caption = "AHA CEO to HIMSS Matches", label = "tab:example"),
  file = paste0(output_dir, "/aha_ceos.tex"),
  include.rownames = FALSE
)
write.csv(df_counts, "temp/aha_ceos.csv", row.names = FALSE)

p <- ggplot(df_counts %>% filter(percent >= 1),
            aes(x = reorder(match_type, -n), y = n)) +
  geom_col(fill = "steelblue") +
  geom_text(aes(label = paste0(round(percent, 1), "%")), vjust = -0.5) +
  labs(title = "Category Counts with % of Total", x = "Category", y = "Count") +
  scale_y_continuous(expand = expansion(mult = c(0.02, 0.15))) +  # add top space
  coord_cartesian(clip = "off") +                                  # don't clip text
  theme_minimal() +
  theme(plot.margin = margin(t = 12, r = 10, b = 10, l = 10))

ggsave(paste0(output_dir, "/aha_ceos_to_himss.png"), plot = p, width = 6, height = 4, dpi = 300)

#### identify remaining cases
real_missing <- all_aha_ceos %>% anti_join(
  cleaned_matches %>% distinct(ahanumber, full_aha, year)
) %>% left_join(cleaned_aha %>% distinct(full_aha, ahanumber, year,cleaned_title_aha))

stopifnot(nrow(real_missing) + nrow(cleaned_matches) == nrow(all_aha_ceos))

interim_cases <- real_missing %>% 
  filter(str_detect(cleaned_title_aha, "interim|acting")) %>%
  distinct(full_aha, ahanumber, year)


######### TBD USEFUL ########
#check_matches <- matches %>%
  #filter(aha_sysid != himss_sysid) %>% filter(
    #(last_jw <= .1 | last_substring) & 
     # (first_jw <= .1 | first_substring | nick_1 | nick_2 | nick_3)) 
# these may be cases where we are potentially failing to track people
# through HIMSS

#view_remaining <- real_missing %>% anti_join(interim_cases)