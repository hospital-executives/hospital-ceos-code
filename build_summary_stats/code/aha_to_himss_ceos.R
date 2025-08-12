
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# load dictionaries
config_path <- file.path("../input/config_path.R")
source(config_path)

final <- read_feather("../input/individuals_final.feather")
hospitals <- read_feather("../input/hospitals_final.feather")

##### SET UP NAME INFRASTRUCTURE ####
names1 <- read.csv(paste0("../input/supplemental_path", "/carltonnorthernnames.csv"), header = FALSE)
female <- read.csv(paste0("../input/supplemental_path", "/female_diminutives.csv"), header = FALSE)
female_lower <- female
female_lower[] <- lapply(female_lower, function(x) {
  if (is.character(x)) tolower(x) else x
})
male <- read.csv(paste0("../input/supplemental_path", "/male_diminutives.csv"), header = FALSE)
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

cleaned_aha <- read_csv("../temp/cleaned_aha_madmin.csv") %>%
  filter(str_detect(cleaned_title_aha, "ceo|chief executive")) 

# subset AHA to only (aha, year) observations that are in HIMSS
all_aha_ceos <- hospitals %>% distinct(ahanumber,year) %>% left_join(
  cleaned_aha %>% distinct(full_aha, ahanumber, year)) %>% 
  filter(year > 2008 & !is.na(full_aha)) %>%
  rename(full_name = full_aha)

# subset HIMSS to only observations that are CEOs
all_himss_ceos <- final %>% 
  filter(title_standardized == "CEO:  Chief Executive Officer" |
           str_detect(title, "CEO") | str_detect(title, "C.E.O") | 
           str_detect(title, "Chief Executive")) %>% 
  distinct(full_name, entity_aha, year) %>% rename(ahanumber = entity_aha)

### first pass - match AHA & HIMSS ceos on full name -------------------------

# join ceos on full name
joined <- all_aha_ceos %>% 
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
  filter(title_standardized == "CEO:  Chief Executive Officer" |
           str_detect(title, "CEO") | str_detect(title, "C.E.O") | 
           str_detect(title, "Chief Executive")) %>% 
  rename(
    first_himss = firstname,
    last_himss = lastname,
    full_himss = full_name
  ) %>%
  distinct(full_himss, first_himss, last_himss, year, entity_aha) %>%
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
stopifnot(nrow(total_combos) == nrow(all_aha_ceos))

# split into matches and not-yet-matches
confirmed_same <- cleaned_joined %>% 
  filter(himss_year == aha_year & aha_aha == himss_aha & jw_full <=.15) %>%
  mutate(match_type = "jw")
remaining1 <- cleaned_joined %>% 
  anti_join(confirmed_same, by = c("full_aha", "aha_aha", "aha_year")) 

### second pass - match AHA & HIMSS ceos on first/last string sim and nicknames ----
fuzzy_names_ceos <- remaining1 %>%
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
  ungroup()

# split into new matches and not-yet-matches
confirmed_same_2 <- fuzzy_names_ceos %>% filter(
  himss_year == aha_year & aha_aha == himss_aha & 
    (
      (last_jw <= .15 | last_substring) & 
        (first_jw <= .15 | first_substring | nick_1 | nick_2 | nick_3)) 
)  %>%
  mutate(match_type = "jw")

remaining2 <- fuzzy_names_ceos %>% 
  anti_join(confirmed_same_2, by = c("full_aha", "aha_aha", "aha_year")) 

### third pass - match AHA & HIMSS ceos on first/last string sim and nicknames ----
###              but with mismatch on year -----------------------------------

confirmed_same_3 <- remaining2 %>%
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
    prefer_after = if_else(himss_year > aha_year, 1, 0)
  ) %>%
  group_by(full_aha, aha_aha, aha_year) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>% 
  rename(himss_after_aha = has_himss_after_no_match, 
         himss_before_aha = has_himss_before_no_match) %>%
  mutate(match_type = paste0("jw_year_", year_diff)) %>%
  select(-year_diff, -prefer_after,-has_himss_after,-has_himss_before)

remaining3 <- remaining2 %>% 
  anti_join(confirmed_same_3, by = c("full_aha", "aha_aha", "aha_year")) 

# add columns to previous matches for merge
confirmed_same <- confirmed_same %>% mutate(himss_after_aha = NA,
                                            himss_before_aha = NA)
confirmed_same_2 <- confirmed_same_2 %>% mutate(himss_after_aha = NA,
                                                himss_before_aha = NA)
accounted_for <- bind_rows(
  confirmed_same[, c("full_aha", "aha_aha", "aha_year", "match_type", "himss_before_aha", "himss_after_aha")],
  confirmed_same_2[, c("full_aha", "aha_aha", "aha_year", "match_type", "himss_before_aha", "himss_after_aha")],
  confirmed_same_3[, c("full_aha", "aha_aha", "aha_year", "match_type", "himss_before_aha", "himss_after_aha")]
)

### fourth pass - find matches using merged HIMSS to AHA data -----------------

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
  mutate(ahanumber = entity_aha) %>%
  anti_join(accounted_for %>% 
              filter(!str_detect(match_type, "jw_year_")) %>%
              rename(ahanumber = aha_aha, year = aha_year), 
            by = c("full_aha", "ahanumber", "year"))

# add string distance and nickname measures
valid_himss <- himss_processed %>%
  rename(
    first_himss = firstname,
    last_himss = lastname,
    himss_full = full_name
  ) %>% select(
    first_himss,last_himss, himss_full, first_aha, last_aha, full_aha, title,
    title_standardized, cleaned_title_aha, mname, entity_name, year, ahanumber
  ) %>%
  mutate(last_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1),
         first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1),
         full_jw = stringdist(full_aha, himss_full, method = "jw", p = 0.1),
         nick_1 = mapply(function(a, b) names_in_same_row_dict(a, b, dict1), first_aha, first_himss),
         nick_2 = mapply(function(a, b) names_in_same_row_dict(a, b, dict2), first_aha, first_himss),
         nick_3 = mapply(function(a, b) names_in_same_row_dict(a, b, dict3), first_aha, first_himss),
         last_substring = mapply(last_name_overlap, last_aha, last_himss),
         first_substring = mapply(last_name_overlap, first_aha, first_himss))

# get cases where there is a title mismatch (e.g., not a CEO in HIMSS)
title_mismatch <- valid_himss %>% filter(
  full_jw <= .15 |
    (
      (last_jw <= .15 | last_substring) & 
        (first_jw <= .15 | first_substring | nick_1 | nick_2 | nick_3)) 
) %>% 
  mutate(match_type = "title")

# update confirmed with title mismatch
confirmed_with_title <- rbind(accounted_for %>% rename(ahanumber = aha_aha,
                                                       year = aha_year), 
                              title_mismatch %>% mutate(himss_after_aha = NA,
                                                        himss_before_aha = NA) %>%
                                distinct(ahanumber, full_aha, year, match_type,
                                         himss_after_aha,himss_before_aha))


### fifth pass - find matches where AHA observation occurs in HIMSS, but -----
###              not in the exact year ---------------------------------------

# left join AHA with HIMSS, keeping only observations in HIMSS within 2 years
# of the AHA
aha_df  <- cleaned_aha %>%
  anti_join(confirmed_with_title %>% select(-match_type)) %>%
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
  filter(abs(himss_year - aha_year) <= 2)

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
    min_full_jw <= .15 |
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
  mutate(match_type = paste0("jw_year_title_", year_diff)) %>%
  rename(year = aha_year) %>%
  distinct(ahanumber, full_aha, year, match_type, himss_after_aha, himss_before_aha) 

all_matches <- rbind(confirmed_with_title, matches) %>%
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

### sixth pass - find matches where AHA observation occurs in HIMSS, but ------
###              in a different system, using a (first, last) fuzzy join ------
###              and using system id from AHA 
remaining <- all_aha_ceos %>% left_join(cleaned_aha) %>%
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

if (any(sys_matches$match_type == "unaccounted", na.rm = TRUE)) {
  stop("Some matches are unaccounted for")
}

### seventh pass - find matches where AHA observation occurs in HIMSS, but ------
###              in a different system, using a full name fuzzy join ------
###              and using system id from HIMSS

# combine current matches
confirmed_matches <- rbind(all_matches %>% select(full_aha, ahanumber, year), 
                           sys_matches %>% select(full_aha, aha_aha, aha_year) %>%
                             rename(ahanumber = aha_aha, year = aha_year)) %>%
  rename(full_name = full_aha)  %>% distinct(ahanumber, full_name, year)

# get unaccounted for ceos
unmatched <- all_aha_ceos %>% anti_join(confirmed_matches) %>% 
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



#### clean output

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

cleaned_matches <- final_matches %>% inner_join(all_aha_ceos)

###### ggplot code #####
get_count <- cleaned_matches %>% distinct(full_aha,ahanumber, year) 
cat(nrow(get_count)/nrow(all_aha_ceos))

df_counts <- final_matches %>% 
  count(match_type) %>%
  mutate(percent = n / nrow(all_aha_ceos) * 100) %>% 
  filter(percent >= 1)

ggplot(df_counts, aes(x = reorder(match_type, -n), y = n)) +
  geom_col(fill = "steelblue") +
  geom_text(aes(label = paste0(round(percent, 1), "%")), vjust = -0.5) +
  labs(title = "Category Counts with % of Total", x = "Category", y = "Count") +
  theme_minimal()

#### identify remaining cases
real_missing <- all_aha_ceos %>% anti_join(
  cleaned_matches %>% distinct(ahanumber, full_aha, year)
) %>% left_join(cleaned_aha %>% distinct(full_aha, ahanumber, year,cleaned_title_aha))

stopifnot(nrow(real_missing) + nrow(cleaned_matches) == nrow(all_aha_ceos))

interim_cases <- real_missing %>% 
  filter(str_detect(cleaned_title_aha, "interim|acting")) %>%
  distinct(full_aha, ahanumber, year)
cat("Of the remaining", nrow(real_missing), "cases,",
    nrow(interim_cases), "can be explained by the madmin CEO being interim.")

######### TBD USEFUL ########
check_matches <- matches %>%
  filter(aha_sysid != himss_sysid) %>% filter(
    (last_jw <= .1 | last_substring) & 
      (first_jw <= .1 | first_substring | nick_1 | nick_2 | nick_3)) 
# these may be cases where we are potentially failing to track people
# through HIMSS

view_remaining <- real_missing %>% anti_join(interim_cases)