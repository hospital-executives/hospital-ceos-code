rm(list = ls())
library(rstudioapi)
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
  args <- commandArgs(trailingOnly = TRUE)
  hospital_path <- args[1]
  final_path <- args[2]
  hospitals <- read_feather(hospital_path)
  final <- read_feather(final_path)
  supp_path <- args[3]
  output_dir <- args[4] 
}

##### SET UP NAME INFRASTRUCTURE ####
source("helper.R")

names1 <- read.csv(paste0(supp_path, "/carltonnorthernnames.csv"), header = FALSE)
female <- read.csv(paste0(supp_path, "/female_diminutives.csv"), header = FALSE)
male <- read.csv(paste0(supp_path, "/male_diminutives.csv"), header = FALSE)

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

set.seed(123)

## load + combine matches from the aha
himss_ceo_matches <- read_feather(paste0(auxiliary_data, "/matched_aha_ceos_himss.feather"))
himss_non_ceo_matches <- read_feather(paste0(auxiliary_data, "/matched_aha_no_ceos_himss.feather"))
himss_matches <- rbind(himss_ceo_matches,himss_non_ceo_matches) %>%
  mutate(match_type = ifelse(match_type == "jw_title_0", "jw_title", match_type))

## get cleaned HIMSS ceos and observations that need to be matched to aha still
aha_data <-  read_csv("temp/cleaned_aha_madmin.csv")

all_himss_obs <- aha_data %>% distinct(ahanumber,year) %>% 
  left_join(
    final %>%
      distinct(contact_uniqueid ,full_name, firstname, lastname,
               ahanumber, entity_aha, campus_aha, entity_uniqueid, entity_name, year, 
               title_standardized, title, madmin, haentitytypeid, id, parentid, 
               confirmed, ceo_himss_title_fuzzy) %>%
      mutate(ahanumber = entity_aha)) %>% 
  filter(year > 2008 & !is.na(full_name) & !is.na(ahanumber)) 

all_himss_ceos <- all_himss_obs %>% filter(ceo_himss_title_fuzzy)
unmatched_himss <- all_himss_obs %>% 
  group_by(entity_uniqueid, year) %>%
  mutate(any_ceo = any(ceo_himss_title_fuzzy)) %>%
  ungroup() %>%
  filter(!(any_ceo & !ceo_himss_title_fuzzy)) %>%
  anti_join(himss_matches %>% distinct(id), by = "id")

##### use contact_uniqueid to map himss to aha ####

# get contact xwalks by confirmed cases
contact_xwalk <- himss_matches %>% left_join(all_himss_obs) %>%
  group_by(contact_uniqueid) %>%
  filter(any(confirmed)) %>%
  ungroup()

mismatched <- unmatched_himss %>% 
  filter(confirmed) %>%
  left_join(
  contact_xwalk %>% distinct(entity_aha, year, contact_uniqueid, 
                             firstname, lastname, match_type),
  by = "contact_uniqueid"
  ) %>% 
  rename_with(~ str_replace(.x, "\\.x$", "_unmatched")) %>%
  rename_with(~ str_replace(.x, "\\.y$", "_matched"))

aha_match <- mismatched %>% 
  filter(entity_aha_unmatched == entity_aha_matched) %>%
  mutate(year_diff = abs(year_unmatched - year_matched)) %>%
  group_by(id) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_sample(n = 1) %>%  # randomly pick one from ties
  ungroup() %>%
  mutate(match_type = case_when(
         match_type == "jw_title_0" ~ "jw_title",
         match_type == "jw_0" ~ "jw",
         TRUE ~ match_type), 
         new_match_type = paste0(match_type, "_", year_diff)) %>%
  select(-match_type) %>%
  rename(year = year_unmatched, match_type = new_match_type)

## get contact xwalks for unconfirmed cases
## - just need to make sure there is a valid match in AHA with name similarity measures

contact_xwalk_unconfirmed <- himss_matches %>% left_join(all_himss_obs) %>%
  filter(!confirmed) 

himss_unconfirmed_contact_uniqueid <- unmatched_himss %>% 
  filter(!confirmed) %>%
  left_join(
    contact_xwalk_unconfirmed %>% distinct(entity_aha, year, contact_uniqueid, 
                               firstname, lastname, match_type),
    by = "contact_uniqueid"
  ) %>% 
  rename_with(~ str_replace(.x, "\\.x$", "_unmatched")) %>%
  rename_with(~ str_replace(.x, "\\.y$", "_matched")) %>%
  filter(!is.na(year_matched)) %>%
  mutate(last_jw = stringdist(lastname_matched, lastname_unmatched, method = "jw", p = 0.1),
         first_jw = stringdist(firstname_matched, firstname_unmatched, method = "jw", p = 0.1)
        ) %>%
  rowwise() %>%
  mutate(
    nick_1 = names_in_same_row_dict(firstname_matched, firstname_unmatched, dict1),
    nick_2 = names_in_same_row_dict(firstname_matched, firstname_unmatched, dict2),
    nick_3 = names_in_same_row_dict(firstname_matched, firstname_unmatched, dict3),
    last_substring = last_name_overlap(lastname_matched, lastname_unmatched),
    first_substring = last_name_overlap(firstname_matched, firstname_unmatched)
  ) %>%
  ungroup() %>%
  mutate(
    last_condition = ((last_jw <= 0.15 & !is.na(last_jw)) | last_substring),
    first_condition = (first_jw <= 0.15 & !is.na(first_jw)) | first_substring |
           nick_1 | nick_2 | nick_3,
    similarity_condition = (first_condition & last_condition)
  ) %>%
  filter(similarity_condition)
  
aha_match_among_unconfirmed <- himss_unconfirmed_contact_uniqueid %>% 
  filter(entity_aha_unmatched == entity_aha_matched) %>%
  mutate(year_diff = abs(year_unmatched - year_matched)) %>%
  group_by(id) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_sample(n = 1) %>%  # randomly pick one from ties
  ungroup() %>%
  mutate(match_type = case_when(
    match_type == "jw_title_0" ~ "jw_title",
    match_type == "jw_0" ~ "jw",
    TRUE ~ match_type), 
    new_match_type = paste0(match_type, "_", year_diff)) %>%
  select(-match_type) %>%
  rename(year = year_unmatched, match_type = new_match_type)
  
## combined matches on contact_uniqueid where ahanumber matches
cleaned_matches <- rbind(
  aha_match %>% distinct(ahanumber, year, id, match_type),
  aha_match_among_unconfirmed %>% distinct(ahanumber, year, id, match_type)
)

all_matches <- rbind(himss_matches, cleaned_matches) %>% distinct(ahanumber, year, id)
matched_ids <- all_matches %>% distinct(id) %>% filter(id %in% all_himss_ceos$id)
cat( nrow(matched_ids) / n_distinct(all_himss_ceos$id) )

## get matches within system across ahas 
aha_sys_xwalk <- hospitals %>% filter(!is.na(sysid) & !is.na(ahanumber)) %>%
  distinct(ahanumber, sysid, year) %>%
  rename(entity_aha = ahanumber)

aha_mismatch <- mismatched %>% 
  filter(entity_aha_unmatched != entity_aha_matched) %>%
  left_join(aha_sys_xwalk, 
            by = c("entity_aha_unmatched" = "entity_aha", "year_unmatched" = "year"),
            suffix = c("", "_unmatched")) %>%
  rename(sysid_unmatched = sysid) %>%
  left_join(aha_sys_xwalk, 
            by = c("entity_aha_matched" = "entity_aha", "year_matched" = "year"),
            suffix = c("", "_matched")) %>%
  rename(sysid_matched = sysid) %>%
  filter(sysid_unmatched == sysid_matched & 
        !is.na(sysid_unmatched) & 
        !is.na(sysid_matched)) %>%
  mutate(year_diff = abs(year_unmatched - year_matched)) %>%
  group_by(ahanumber, year_unmatched, contact_uniqueid) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_sample(n = 1) %>%  # randomly pick one from ties
  ungroup() %>%
  mutate(
    match_type = ifelse(match_type == "jw_title_0", "jw_title", match_type), 
    match_type = case_when(
      year_diff == 0 ~ paste0(match_type, "_sys"),
      TRUE ~paste0(match_type, "_sys_", year_diff)
    )
  )

aha_mismatch_unconfirmed_contact <- himss_unconfirmed_contact_uniqueid %>% 
  filter(entity_aha_unmatched != entity_aha_matched) %>%
  left_join(aha_sys_xwalk, 
            by = c("entity_aha_unmatched" = "entity_aha", "year_unmatched" = "year"),
            suffix = c("", "_unmatched")) %>%
  rename(sysid_unmatched = sysid) %>%
  left_join(aha_sys_xwalk, 
            by = c("entity_aha_matched" = "entity_aha", "year_matched" = "year"),
            suffix = c("", "_matched")) %>%
  rename(sysid_matched = sysid) %>%
  filter(sysid_unmatched == sysid_matched & 
           !is.na(sysid_unmatched) & 
           !is.na(sysid_matched)) %>%
  mutate(year_diff = abs(year_unmatched - year_matched)) %>%
  group_by(ahanumber, year_unmatched, contact_uniqueid) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_sample(n = 1) %>%  # randomly pick one from ties
  ungroup() %>%
  mutate(
    match_type = ifelse(match_type == "jw_title_0", "jw_title", match_type), 
    match_type = case_when(
      year_diff == 0 ~ paste0(match_type, "_sys"),
      TRUE ~paste0(match_type, "_sys_", year_diff)
    )
  )

## combined matches 
derived_matches <- rbind(
  himss_matches,
  cleaned_matches,
  aha_mismatch %>% rename(year = year_unmatched) %>% distinct(ahanumber, year, id, match_type),
  aha_mismatch_unconfirmed_contact %>% rename(year = year_unmatched) %>% distinct(ahanumber, year, id, match_type)
) %>%
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
  slice_min(order_by = match_order, n = 1, with_ties = FALSE) %>%
  ungroup()

##### match pre-2009 aha observations to HIMSS #####

## clean up matches
still_unmatched <- unmatched_himss %>% anti_join(derived_matches %>% distinct(id), by = "id")

matched_ahanumbers <- hospitals %>% distinct(entity_aha) %>% rename(ahanumber = entity_aha) %>% pull(ahanumber)
pre_2009_aha <- aha_data %>% filter(year < 2009 & ahanumber %in% matched_ahanumbers) %>%
  rename(full_name = full_aha) %>%
  filter(!is.na(full_name)) %>%
  distinct(full_name, first_aha, last_aha, ahanumber, year, mname, cleaned_title_aha)

# part a:  match dfs on fuzzy full name merge
full_name_match <- still_unmatched %>% filter(!is.na(full_name)) %>%
  stringdist_left_join(pre_2009_aha, by = "full_name", method = "jw", max_dist = 0.1) %>%
  rename(
    aha_aha = ahanumber.y,
    himss_aha = entity_aha,
    aha_year = year.y,
    himss_year = year.x,
    full_aha = full_name.y,
    full_himss = full_name.x,
    last_himss = lastname,
    first_himss = firstname 
  ) %>%
  mutate(last_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1),
         first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1),
         full_jw = stringdist(full_aha, full_himss, method = "jw", p = 0.1)) %>%
  rowwise() %>%
  mutate(
    nick_1 = names_in_same_row_dict(first_aha, first_himss, dict1),
    nick_2 = names_in_same_row_dict(first_aha, first_himss, dict2),
    nick_3 = names_in_same_row_dict(first_aha, first_himss, dict3),
    last_substring = last_name_overlap(last_aha, last_himss),
    first_substring = last_name_overlap(first_aha, first_himss)
  ) %>%
  ungroup()  %>%
  mutate(
    full_condition = (full_jw <= 0.1 & !is.na(full_jw)),
    last_condition = ((last_jw <= 0.15 & !is.na(last_jw)) | last_substring),
    first_condition = (first_jw <= 0.15 & !is.na(first_jw)) | first_substring |
      nick_1 | nick_2 | nick_3,
    similarity_condition = full_condition | (first_condition & last_condition),
    year_diff = abs(himss_year - aha_year),
    prefer_after = if_else(himss_year > aha_year, 1, 0),
    aha_ceo_flag = str_detect(cleaned_title_aha, "ceo|chief executive officer|^president")
  )

aha_full_matches <- full_name_match %>% 
  filter(aha_aha == himss_aha & similarity_condition) %>%
  group_by(id) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>%
  mutate(match_type = case_when(
    aha_ceo_flag ~ paste0("year_", year_diff),
    TRUE ~ paste0("year_title_", year_diff)
  ))
  
aha_sysid <- aha_data %>% distinct(ahanumber, year, sysid)

full_sys_matches <-  full_name_match %>% 
  anti_join(aha_full_matches %>% select(id)) %>%
  rename(year = aha_year, ahanumber = aha_aha) %>%
  left_join(aha_sysid) %>%
  rename(aha_aha = ahanumber,
         aha_sysid = sysid,
         ahanumber = campus_aha,
         aha_year = year,
         year = himss_year) %>%
  left_join(aha_sysid) %>%
  rename(campus_aha = ahanumber,
         himss_sysid = sysid,
         himss_year = year) %>%
  filter(himss_sysid == aha_sysid & !is.na(himss_sysid) & !is.na(aha_sysid) & 
           similarity_condition) %>%
  group_by(id) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>%
  mutate(match_type = case_when(
    aha_ceo_flag ~ paste0("sys_year_", year_diff),
    TRUE ~ paste0("sys_year_title_", year_diff)
  ))

# part b:  match dfs on fuzzy last name merge
matched_ids <- union(unique(full_sys_matches$id), unique(aha_full_matches$id))

last_match <- still_unmatched %>% 
  filter(!(id %in% matched_ids)) %>%
  filter(!is.na(firstname) & !is.na(lastname)) %>%
  stringdist_left_join(
    pre_2009_aha %>% rename(firstname = first_aha, lastname = last_aha), 
    by = c("lastname"), 
    method = "jw",
    max_dist = 0.15
  ) %>%
  rename(
    aha_aha = ahanumber.y,
    himss_aha = entity_aha,
    aha_year = year.y,
    himss_year = year.x,
    full_aha = full_name.y,
    full_himss = full_name.x,
    last_himss = lastname.x,
    first_himss = firstname.x, 
    last_aha = lastname.y,
    first_aha = firstname.y, 
  ) %>%
  mutate(last_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1),
         first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1),
         full_jw = stringdist(full_aha, first_himss, method = "jw", p = 0.1)) %>%
  rowwise() %>%
  mutate(
    nick_1 = names_in_same_row_dict(first_aha, first_himss, dict1),
    nick_2 = names_in_same_row_dict(first_aha, first_himss, dict2),
    nick_3 = names_in_same_row_dict(first_aha, first_himss, dict3),
    last_substring = last_name_overlap(last_aha, last_himss),
    first_substring = last_name_overlap(first_aha, first_himss)
  ) %>%
  ungroup() %>%
  mutate(
    full_condition = (full_jw <= 0.1 & !is.na(full_jw)),
    last_condition = ((last_jw <= 0.15 & !is.na(last_jw)) | last_substring),
    first_condition = (first_jw <= 0.15 & !is.na(first_jw)) | first_substring |
      nick_1 | nick_2 | nick_3,
    similarity_condition = full_condition | (first_condition & last_condition),
    year_diff = abs(himss_year - aha_year),
    prefer_after = if_else(himss_year > aha_year, 1, 0),
    aha_ceo_flag = str_detect(cleaned_title_aha, "ceo|chief executive officer|^president")
  )

aha_last_matches <- last_match %>% 
  filter(aha_aha == himss_aha & similarity_condition) %>%
  group_by(id) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>%
  mutate(match_type = case_when(
    aha_ceo_flag ~ paste0("year_", year_diff),
    TRUE ~ paste0("year_title_", year_diff)
  ))

last_sys_matches <-  last_match %>% 
  anti_join(aha_full_matches %>% select(id)) %>%
  rename(year = aha_year, ahanumber = aha_aha) %>%
  left_join(aha_sysid) %>%
  rename(aha_aha = ahanumber,
         aha_sysid = sysid,
         ahanumber = campus_aha,
         aha_year = year,
         year = himss_year) %>%
  left_join(aha_sysid) %>%
  rename(campus_aha = ahanumber,
         himss_sysid = sysid,
         himss_year = year) %>%
  filter(himss_sysid == aha_sysid & !is.na(himss_sysid) & !is.na(aha_sysid) & 
           similarity_condition) %>%
  group_by(id) %>%
  slice_min(order_by = year_diff, with_ties = TRUE) %>%  # keep ties
  slice_max(order_by = prefer_after, n = 1, with_ties = FALSE) %>%  # break ties by favoring after
  ungroup() %>%
  mutate(match_type = case_when(
    aha_ceo_flag ~ paste0("sys_year_", year_diff),
    TRUE ~ paste0("sys_year_title_", year_diff)
  ))

## combine new matches
selected_cols <- c("aha_aha", "aha_year", "id", "match_type")
new_matches <- rbind(aha_full_matches[selected_cols],
                     full_sys_matches[selected_cols],
                     aha_last_matches[selected_cols],
                     last_sys_matches[selected_cols]) %>%
  rename(ahanumber = aha_aha, year = aha_year)

final_matches <- rbind(
  derived_matches %>% select(-match_order),
  new_matches
) %>%
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
  slice_min(order_by = match_order, n = 1, with_ties = FALSE) %>%
  ungroup()

write_feather(final_matches, "temp/himss_to_aha_matches.feather")
matched_ids <- final_matches %>% distinct(id) %>% filter(id %in% all_himss_ceos$id)
cat( nrow(matched_ids) / n_distinct(all_himss_ceos$id) )

df_counts <- final_matches %>% 
  filter(id %in% all_himss_ceos$id) %>%
  count(match_type) %>%
  mutate(percent = n / n_distinct(all_himss_ceos$id) * 100) 

p <- ggplot(df_counts %>% filter(percent >= 1 & !is.na(match_type)), aes(x = reorder(match_type, -n), y = n)) +
  geom_col(fill = "steelblue") +
  geom_text(aes(label = paste0(round(percent, 1), "%")), vjust = -0.5) +
  labs(title = "Category Counts with % of Total", x = "Category", y = "Count") +
  scale_y_continuous(expand = expansion(mult = c(0.02, 0.15))) +  # add top space
  coord_cartesian(clip = "off") +                                  # don't clip text
  theme_minimal() +
  theme(plot.margin = margin(t = 12, r = 10, b = 10, l = 10))

ggsave(paste0(output_dir, "/himss_ceos_to_aha.png"), plot = p, width = 6, height = 4, dpi = 300)

# check remaining

remaining <- all_himss_ceos %>% anti_join(final_matches %>% distinct(id), by = "id")
