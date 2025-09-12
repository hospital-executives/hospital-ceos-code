
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# load dictionaries
library(xtable)
config_path <- file.path("../input/config_path.R")
source(config_path)

final <- read_feather("../input/individuals_final.feather")
hospitals <- read_feather("../input/hospitals_final.feather")

### AHA CEO to HIMSS left join

## set up scripts
config_path <- file.path("../input/config_path.R")
source(config_path)

final_path <- paste0(derived_data, "/final_aha.feather")
final <- read_feather(final_path)
hospitals <- read_feather(paste0(derived_data,'/himss_aha_hospitals_final.feather'))

##### SET UP NAME INFRASTRUCTURE ####
source("helper.R")
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

dict1 <- build_cooccurrence_dict(names1)
dict2 <- build_cooccurrence_dict(female_lower)
dict3 <- build_cooccurrence_dict(male_lower)

### load aha
cleaned_aha <- read_csv("../temp/cleaned_aha_madmin.csv") 

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
all_aha_ceos <- hospitals %>% distinct(ahanumber,year) %>% left_join(
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
  ungroup() %>%
  distinct(full_name, entity_aha, year, title_standardized, id) %>% 
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
  distinct(full_name, first_himss, last_himss, year, entity_aha, id) %>%
  rename(ahanumber = entity_aha)

aha_year_df <- aha_year_mini %>% left_join(aha_mini) %>%
  left_join(himss_mini) %>% rename(full_himss = full_name)

# exact name match
exact_names <- aha_year_df %>% 
  filter(first_himss == first_aha & last_himss == last_aha)
exact_combos <- exact_names %>% distinct(ahanumber, full_aha, year, id)

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

jw_combos <- jw_matches %>% group_by(ahanumber, full_aha, year) %>% slice(1) %>%
  ungroup() %>% distinct(ahanumber, full_aha, year, id)

name_combos <- rbind(exact_combos, jw_combos) %>%
  mutate(match_type = "jw")

### second pass - match AHA & HIMSS ceos on aha with full name merge --------
remaining_madmin <- aha_madmin_no_ceos %>% 
  anti_join(name_combos %>% 
              select(-id, -match_type) %>%
              distinct())
remaining_himss <- all_himss_leadership %>% rename(full_himss = full_name) %>%
  anti_join(exact_names %>% distinct(full_himss, ahanumber, year)) %>%
  anti_join(jw_matches %>% distinct(full_himss, ahanumber, year)) %>% 
  rename(full_name = full_himss) 

remaining_csuite <- remaining_himss %>%
  filter(str_detect(title_standardized, "CFO|CEO|COO|CIO|Chief Medical")) %>%
  group_by(full_name, ahanumber, year) %>%
  slice(1) %>%
  ungroup() %>% distinct(ahanumber, full_name, year, id)
    
joined <- remaining_madmin %>% distinct(ahanumber, year, full_aha) %>%
  rename(full_name = full_aha) %>%
  stringdist_left_join(remaining_himss %>% distinct(full_name, ahanumber, year, id) ,
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
  distinct(ahanumber, full_aha, year, match_type, himss_after_aha, himss_before_aha, id) 

jw_and_year_matches <- rbind(
  name_combos %>% mutate(himss_after_aha = NA, himss_before_aha = NA),
  year_mismatches
)


### third pass - find matches using merged HIMSS to AHA data -----------------
aha_df  <- cleaned_aha %>%
  anti_join(
    jw_and_year_matches %>%
      filter(!str_detect(match_type, "\\d")) %>%  # keep only rows w/o numbers
      select(-match_type),
    by = intersect(names(cleaned_aha), names(jw_and_year_matches))
  ) %>%
  anti_join(all_aha_ceos) %>%
  rename(aha_year  = year)
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
  distinct(ahanumber, full_aha, year, match_type, himss_after_aha, himss_before_aha, id) 

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

confirmed_matches <- rbind(all_matches %>% select(full_aha, ahanumber, year, id), 
                           sys_matches %>% select(full_aha, aha_aha, aha_year, id) %>%
                             rename(ahanumber = aha_aha, year = aha_year)) %>%
  rename(full_name = full_aha)  %>% distinct(ahanumber, full_name, year, id)


# get unaccounted for ceos
unmatched <- aha_madmin_no_ceos %>% anti_join(confirmed_matches) %>% 
  left_join(cleaned_aha %>% distinct(full_aha, ahanumber, year,cleaned_title_aha))

# get mapping between ahanumber and systems
all_missing_aha <- unmatched %>% distinct(ahanumber) %>% pull(ahanumber)
corresponding_system_ids <- final %>% filter(campus_aha %in% all_missing_aha) %>%
  distinct(ahanumber, parentid, year, system_id) %>% rename(himss_year = year) 

parent_df <- final %>% filter(parentid %in% corresponding_system_ids$parentid) %>%
  distinct(firstname, lastname, full_name, ahanumber, entity_aha, entity_name, year, title_standardized, title, id) %>%
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
                                              himss_after_aha, id) %>%
                         rename(ahanumber = aha_aha, year = aha_year), 
                       himss_sys_matches %>% select(full_aha, ahanumber, aha_year,
                                                    match_type, himss_before_aha,
                                                    himss_after_aha, id) %>%
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
write_feather(cleaned_matches, "../temp/matched_aha_leadership.feather")

###### ggplot code #####
get_count <- cleaned_matches %>% distinct(full_aha,ahanumber, year) 
cat(nrow(get_count))
cat(nrow(get_count)/nrow(aha_madmin_no_ceos))

df_counts <- final_matches %>% 
  count(match_type) %>%
  mutate(percent = n / nrow(aha_madmin_no_ceos) * 100) 
  
print(
  xtable(df_counts, caption = "AHA CEO to HIMSS Matches", label = "tab:example"),
  file = "../output/execs/aha_leadership_only.tex",
  include.rownames = FALSE
)
write.csv(df_counts, "../temp/aha_leadership_only.csv", row.names = FALSE)

p <- ggplot(df_counts %>% filter(percent >= 1), aes(x = reorder(match_type, -n), y = n)) +
  geom_col(fill = "steelblue") +
  geom_text(aes(label = paste0(round(percent, 1), "%")), vjust = -0.5) +
  labs(title = "Category Counts with % of Total", x = "Category", y = "Count") +
  scale_y_continuous(expand = expansion(mult = c(0.02, 0.15))) +  # add top space
  coord_cartesian(clip = "off") +                                  # don't clip text
  theme_minimal() +
  theme(plot.margin = margin(t = 12, r = 10, b = 10, l = 10))

ggsave("../output/execs/aha_leadership_to_himss.png", plot = p, width = 6, height = 4, dpi = 300)

### check titles
title_df <- final %>% distinct(id, title, title_standardized)
matches_with_titles <- final_matches %>% left_join(title_df)

matches_with_titles %>% count(title, sort = TRUE)
matches_with_titles %>% count(title_standardized, sort = TRUE)