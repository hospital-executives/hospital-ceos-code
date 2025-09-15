
## libraries
library(rstudioapi)
#setwd(dirname(getActiveDocumentContext()$path))
rm(list = ls())

## set up scripts
config_path <- file.path("../input/config_path.R")
source(config_path)

final <- read_feather("../input/individuals_final.feather")
hospitals <- read_feather("../input/hospitals_final.feather")

##### SET UP NAME INFRASTRUCTURE ####
source("helper.R")
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

dict1 <- build_cooccurrence_dict(names1)
dict2 <- build_cooccurrence_dict(female_lower)
dict3 <- build_cooccurrence_dict(male_lower)

#### load AHA data #####
cleaned_aha <- read_csv("../temp/cleaned_aha_madmin.csv") 
aha_data <-  read_csv("../temp/cleaned_aha_madmin.csv")

all_himss_ceos <- aha_data %>% distinct(ahanumber,year) %>% 
  left_join(
    final %>% filter(haentitytypeid == 1) %>%
      distinct(contact_uniqueid ,full_name, firstname, lastname,
               ahanumber, entity_aha, campus_aha, entity_uniqueid, entity_name, year, 
               title_standardized, title, madmin, haentitytypeid, id, parentid, confirmed) %>%
      mutate(ahanumber = entity_aha)) %>% 
  filter(year > 2008 & !is.na(full_name) & !is.na(ahanumber))  %>%
  filter(str_detect(title, "ceo|chief executive") |
           str_detect(title_standardized , "CEO"))

all_aha_madmin <- hospitals %>% distinct(ahanumber,year) %>% left_join(
  cleaned_aha %>% distinct(full_aha, ahanumber, year)) %>% 
  filter(year > 2008 & !is.na(full_aha)) %>%
  rename(full_name = full_aha)

num_total_combos = nrow(all_himss_ceos %>% 
                         distinct(entity_uniqueid, year, contact_uniqueid, id))

cat("There are ", num_total_combos,
    "(hospital, year, CEO) observations in HIMSS for which",
    "the ahanumber and year also appear in the AHA data.")

## first step - get HIMSS ceos that we already matched by ------------------
## name similarity in the aha merge
aha_combos <- read_feather("../temp/matched_aha_ceos.feather")

already_matched <- all_himss_ceos %>% left_join(
  aha_combos %>% filter(match_type == "jw") %>% select(ahanumber, year, full_aha)
) %>% filter(!is.na(full_aha))

matched_pairs <- already_matched %>% distinct(entity_aha, contact_uniqueid, year, id)

cat("Of these,", 
    nrow(matched_pairs)/num_total_combos,
    "are already matched by jw similarity and year in the AHA code.")

id_to_contact_xwalk <- final %>%
  distinct(id, contact_uniqueid) %>% 
  left_join(aha_combos %>% distinct(id, match_type)) %>%
  filter(!is.na(match_type))

## extract non-exact matches
not_yet_matched <- all_himss_ceos %>% anti_join(
  aha_combos %>% filter(match_type == "jw") %>% select(ahanumber, year, full_aha)
) 

match_on_id <- not_yet_matched %>% left_join(id_to_contact_xwalk) %>%
  filter(!is.na(match_type))

unmatched <- not_yet_matched %>% anti_join(match_on_id %>% distinct(id))

### second pass - match AHA & HIMSS ceos on year differences -------------------------

# join ceos on full name
joined <- unmatched %>% 
  stringdist_left_join(all_aha_madmin,
                       by = "full_name",
                       method = "jw",
                       max_dist = .2,
                       distance_col = "jw_full") 

# get relevant variables from AHA and HIMSS dfs
aha_mini <- cleaned_aha %>% 
  distinct(full_aha, ahanumber, year, first_aha, last_aha,cleaned_title_aha) %>%
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
  left_join(aha_mini) %>% left_join(himss_mini) %>%
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

confirmed_same <- cleaned_joined %>%
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
  mutate(is_ceo = str_detect(
    cleaned_title_aha,
    regex("\\bchief\\s+executive(\\s+officer)?\\b|(?<!\\bvice[-\\s])\\bpresident\\b|\\bceo\\b", 
          ignore_case = TRUE)
          ),
    match_type = case_when(
      is_ceo ~ paste0("jw_year_", year_diff),
      TRUE ~ paste0("jw_year_title_", year_diff)
    )) %>%
  select(-year_diff, -prefer_after,-has_himss_after,-has_himss_before)

post_year_matches <- bind_rows(
  matched_pairs %>% mutate(himss_after_aha = NA,himss_before_aha = NA, match_type = "jw"),
  confirmed_same %>% 
    select(entity_aha, contact_uniqueid, himss_year, himss_after_aha, himss_before_aha, match_type, id) %>%
    rename(year = himss_year)
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
  mutate(ahanumber = entity_aha) 

# add string distance and nickname measures
valid_himss <- himss_processed %>%
  anti_join(post_year_matches %>% distinct(id)) %>%
  rename(
    first_himss = firstname,
    last_himss = lastname,
    himss_full = full_name
  ) %>% select(
    first_himss,last_himss, himss_full, first_aha, last_aha, full_aha, title,
    title_standardized, cleaned_title_aha, mname, entity_name, year, ahanumber,
    entity_aha, campus_aha, unfiltered_campus_aha, contact_uniqueid, id
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
  mutate(is_ceo = str_detect(
    cleaned_title_aha,
    regex("\\bchief\\s+executive(\\s+officer)?\\b|(?<!\\bvice[-\\s])\\bpresident\\b|\\bceo\\b", 
          ignore_case = TRUE)
        ),
        match_type = ifelse(is_ceo, "jw", "title"))

confirmed_with_title <- rbind(post_year_matches, 
                              title_mismatch %>% 
                                mutate(himss_after_aha = NA, himss_before_aha = NA) %>%
                                select(colnames(post_year_matches)))


### fifth pass - find matches where AHA observation occurs in HIMSS, but -----
###              not in the exact year ---------------------------------------

# left join HIMSS with AHA, keeping only observations in AHA within 5 years
# of HIMSS
aha_df  <- cleaned_aha %>%
  anti_join(confirmed_with_title %>% select(-match_type) %>% rename(ahanumber = entity_aha)) %>%
  rename(aha_year  = year)

himss_df <- final %>% filter(haentitytypeid == 1) %>%
  mutate(ahanumber = entity_aha) %>% 
  filter(year > 2008 & !is.na(full_name) & !is.na(ahanumber))  %>%
  filter(str_detect(title, "ceo|chief executive") |
           str_detect(title_standardized , "CEO")) %>%
  distinct(firstname, lastname, full_name, year, ahanumber, entity_aha, contact_uniqueid, id) %>%
  rename(
    first_himss = firstname,
    last_himss = lastname,
    himss_full = full_name,
    himss_year = year)

himss_left_join_aha <- himss_df %>%
  left_join(aha_df, by = "ahanumber", relationship = "many-to-many") %>%
  filter(abs(himss_year - aha_year) <= 5)

# calculate string similarity/nickname measures
himss_left_join_aha <- himss_left_join_aha %>% mutate(
  full_jw  = stringdist(full_aha , himss_full , method = "jw", p = 0.1),
  first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1),
  last_jw  = stringdist(last_aha , last_himss , method = "jw", p = 0.1),
  nick_1 = mapply(function(a, b) names_in_same_row_dict(a, b, dict1), first_aha, first_himss),
  nick_2 = mapply(function(a, b) names_in_same_row_dict(a, b, dict2), first_aha, first_himss),
  nick_3 = mapply(function(a, b) names_in_same_row_dict(a, b, dict3), first_aha, first_himss),
  last_substring = mapply(last_name_overlap, last_aha, last_himss),
  first_substring = mapply(last_name_overlap, first_aha, first_himss)
)

matches <- himss_left_join_aha %>% filter(aha_year > 2008) %>%
  mutate(
    full_condition = (full_jw <= 0.15 & !is.na(full_jw)),
    last_condition = ((last_jw <= 0.15 & !is.na(last_jw)) | last_substring),
    first_condition = (first_jw <= 0.15 & !is.na(first_jw)) | first_substring |
      nick_1 | nick_2 | nick_3,
    similarity_condition = full_condition | (first_condition & last_condition),
    is_ceo = str_detect(
      cleaned_title_aha,
      regex("\\bchief\\s+executive(\\s+officer)?\\b|(?<!\\bvice[-\\s])\\bpresident\\b|\\bceo\\b", 
            ignore_case = TRUE)
    )
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
    prefer_after = if_else(himss_year > aha_year, 1, 0)
  ) %>%
  group_by(full_aha, ahanumber, aha_year) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>% 
  rename(himss_after_aha = has_himss_after_no_match, 
         himss_before_aha = has_himss_before_no_match) %>%
  mutate(match_type = case_when(
    is_ceo ~ paste0("jw_year_", year_diff),
    TRUE ~ paste0("jw_year_title_", year_diff)
  )) %>%
  rename(year = aha_year) %>%
  distinct(entity_aha, contact_uniqueid, year, match_type, 
           himss_after_aha, himss_before_aha, id) 

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
  group_by(id) %>%
  slice_min(order_by = match_order, n = 1, with_ties = FALSE) %>%
  ungroup()

### sixth pass - find matches where AHA observation occurs in HIMSS, but ------
###              in a different system, using a (first, last) fuzzy join ------
###              and using system id from AHA 

filtered_aha <- final %>% distinct(entity_aha, year) %>% rename(ahanumber = entity_aha) %>%
  left_join(cleaned_aha) %>% distinct(cleaned_title_aha, full_aha, first_aha, last_aha,
                                     ahanumber, year) %>%
  filter(!is.na(full_aha)) %>%
  rename(aha_year = year, firstname = first_aha, lastname = last_aha) %>%
  filter(!is.na(firstname) & !is.na(lastname))

remaining <- all_himss_ceos %>% 
  distinct(entity_aha, year, contact_uniqueid, firstname, lastname, full_name, id) %>%
  anti_join(all_matches %>% distinct(contact_uniqueid,entity_aha, year)) %>%
  rename(himss_year = year)  %>%
  filter(!is.na(firstname) & !is.na(lastname)) %>%
  stringdist_join(filtered_aha)  

cleaned_remaining <- remaining %>%
  mutate(first_jw = stringdist(firstname.x, firstname.y, method = "jw"),
         last_jw = stringdist(lastname.x, lastname.y, method = "jw"),
         nick_1 = mapply(function(a, b) names_in_same_row_dict(a, b, dict1), firstname.x, firstname.y),
         nick_2 = mapply(function(a, b) names_in_same_row_dict(a, b, dict2), firstname.x, firstname.y),
         nick_3 = mapply(function(a, b) names_in_same_row_dict(a, b, dict3), firstname.x, firstname.y),
         last_substring = mapply(last_name_overlap, lastname.x, lastname.y),
         first_substring = mapply(last_name_overlap, firstname.x, firstname.y))

aha_sysid <- cleaned_aha %>% distinct(ahanumber, year, sysid)

potential_matches <-  cleaned_remaining %>% 
  rename(year = aha_year) %>%
  left_join(aha_sysid) %>%
  rename(aha_aha = ahanumber,
         aha_sysid = sysid,
         aha_year = year,
         ahanumber = entity_aha,
         year = himss_year) %>%
  left_join(aha_sysid) %>%
  rename(entity_aha = ahanumber,
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
    is_ceo = str_detect(
      cleaned_title_aha,
      regex("\\bchief\\s+executive(\\s+officer)?\\b|(?<!\\bvice[-\\s])\\bpresident\\b|\\bceo\\b", 
            ignore_case = TRUE)
    ),
    match_type = case_when(
      is_ceo & himss_year == aha_year ~ "sys",
      is_ceo & himss_year == aha_year ~ "sys",
      is_ceo & himss_year != aha_year ~ paste0("sys_year_", year_diff),
      is_ceo & himss_year != aha_year ~ paste0("sys_year_", year_diff),
      himss_year != aha_year ~ paste0("sys_year_", year_diff),
      himss_year == aha_year ~ "sys_title",
      TRUE ~ "unaccounted"
    )) %>% 
  rename(himss_after_aha = has_himss_after_no_match, 
         himss_before_aha = has_himss_before_no_match)

if (any(sys_matches$match_type == "unaccounted", na.rm = TRUE)) {
  stop("Some matches are unaccounted for")
}

cols_keep <- setdiff(names(all_matches), "match_order")

confirmed_matches <- bind_rows(
  all_matches %>% select(all_of(cols_keep)),
  sys_matches  %>% rename(year = himss_year) %>% select(any_of(cols_keep))  # won't error if a column is missing
) %>%
  select(all_of(cols_keep))

### seventh pass - find matches where AHA observation occurs in HIMSS, but ------
###              in a different system, using a full name fuzzy join ------
###              and using system id from HIMSS

# get unaccounted for ceos
unmatched <- all_himss_ceos %>% anti_join(confirmed_matches)

## test contact_uniqueid xwalk
contact_xwalk <- final %>%
  filter(confirmed) %>%
  distinct(id, contact_uniqueid) %>% 
  left_join(aha_combos %>% distinct(id, match_type)) %>%
  filter(!is.na(match_type)) %>% distinct(contact_uniqueid) %>% 
  pull(contact_uniqueid)

any_contact_match <- unmatched %>% filter(contact_uniqueid %in% contact_xwalk)


## match across contact_uniqueid
test <- all_himss_ceos %>%
  group_by(contact_uniqueid) %>%
  filter(all(confirmed) & 
         any(id %in% confirmed_matches$id) & 
         any(!id %in% confirmed_matches$id)) %>%
  ungroup()




final_matches <- confirmed_matches %>%
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
  group_by(id) %>%
  mutate(mult_flag = n() > 1) %>%
  slice_min(order_by = match_order, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  mutate(
    match_type = str_replace_all(match_type, "year_|_0", "")
  ) %>% filter(id %in% unique(all_himss_ceos$id))

check_matches <- confirmed_matches %>% filter(id == 2535765)
