
### load packages
library(dplyr)
library(stringr)
library(arrow)
library(tidyr)
library(stringdist)
library(purrr)

## load data 
rm(list = ls())
final_path <- "/Users/katherinepapen/Library/CloudStorage/Dropbox/hospital_ceos/_data/derived/final_confirmed_aha_final.feather"
final <- read_feather(final_path)
true_aha <- read_feather("/Users/katherinepapen/Desktop/clean_aha.feather")
data_path <- "/Users/katherinepapen/Library/CloudStorage/Dropbox/hospital_ceos/_data/"

##### SET UP NICKNAME INFRASTRUCTURE ####
names1 <- read.csv(paste0(data_path, "supplemental/carltonnorthernnames.csv"), header = FALSE)

female <- read.csv(paste0(data_path, "supplemental/female_diminutives.csv"), header = FALSE)
female_lower <- female
female_lower[] <- lapply(female_lower, function(x) {
  if (is.character(x)) tolower(x) else x
})

male <- read.csv(paste0(data_path, "supplemental/male_diminutives.csv"), header = FALSE)
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


##### DETERMINE AHA:HIMSS MERGE FOR TRUE_AHA
aha_check <- true_aha %>% left_join(final) 

# determine how many (name, entity, year) obs for which there is no match
missing_in_himss <- aha_check %>% filter(is.na(firstname))
total_missing <- missing_in_himss %>% 
  separate(madmin, into = c("full_aha", "title_aha"), 
           sep = ",", extra = "merge", fill = "right") %>%
  mutate(cleaned_title_aha = title_aha %>%
           str_to_lower() %>%
           str_replace_all("[[:punct:]]", "")) %>%
  filter(str_detect(cleaned_title_aha, "ceo|chief executive")) %>%
  filter(!(as.numeric(latitude_aha) <= 20 & as.numeric(longitude_aha) >= -70)) %>%
  filter(as.numeric(longitude_aha) >= -140) %>%
  distinct(full_aha, mname, year) 

# determine if full names that are "missing" aren't really missing
nonmissing_in_himss <- aha_check %>% filter(!is.na(firstname))
all_non_missing <- nonmissing_in_himss %>% 
  separate(madmin, into = c("full_aha", "title_aha"), 
           sep = ",", extra = "merge", fill = "right") %>%
  mutate(cleaned_title_aha = title_aha %>%
           str_to_lower() %>%
           str_replace_all("[[:punct:]]", "")) %>%
  filter(str_detect(cleaned_title_aha, "ceo|chief executive")) %>%
  filter(!(as.numeric(latitude_aha) <= 20 & as.numeric(longitude_aha) >= -70)) %>%
  filter(as.numeric(longitude_aha) >= -140) 

non_missing_combos <- all_non_missing %>% distinct(full_aha, mname, year, firstname, lastname, full_name) 

any_hosp_match <- total_missing %>% left_join(non_missing_combos, 
                                              by = c("full_aha", "mname")) %>%
  filter(!is.na(firstname)) %>%
  rename(
    first_himss = firstname,
    last_himss = lastname,
    full_himss = full_name
  )  %>%
  mutate(last_aha = word(full_aha, -1),
         first_word = word(full_aha, 1),
         second_word = word(full_aha, 2),
         first_aha = if_else(nchar(first_word) == 1, second_word, first_word)
  ) %>% select(-c(first_word, second_word)) %>% ## split into cases where
  mutate(
    jw_similarity = 1 - stringdist(full_aha, full_himss, method = "jw", p = 0.1),
    first_jw = 1 - stringdist(first_aha, first_himss, method = "jw", p = 0.1),
    last_jw = 1 - stringdist(last_aha, last_himss, method = "jw", p = 0.1)
  ) %>%
  group_by(full_aha) %>%
  mutate(
    high_sim =  any(jw_similarity > 0.9),
    max_sim = max(jw_similarity)
  ) %>%
  ungroup() 

name_match <- any_hosp_match %>%
  filter(jw_similarity == max_sim)


##### CLEAN DATA #####
name_df <- final %>%
  select(firstname, lastname, full_name, madmin, title_standardized, title, 
    entity_name, mname, clean_aha, campus_aha, ccn_himss, ccn_aha, year) %>%
  filter(!is.na(madmin)) %>%
  rename(
    first_himss = firstname,
    last_himss = lastname,
    full_himss = full_name
  ) %>% 
  separate(madmin, into = c("full_aha", "title_aha"), 
           sep = ",", extra = "merge", fill = "right") %>% 
  mutate(full_aha = full_aha %>%
           str_to_lower() %>%
           str_replace_all("[[:punct:]]", ""))

full_df <- name_df %>%
  mutate(last_aha = word(full_aha, -1),
    first_word = word(full_aha, 1),
    second_word = word(full_aha, 2),
    first_aha = if_else(nchar(first_word) == 1, second_word, first_word)
  ) %>% select(-c(first_word, second_word))

name_sim <- full_df %>% ## split into cases where
  mutate(
    jw_similarity = 1 - stringdist(full_aha, full_himss, method = "jw", p = 0.1),
    first_jw = 1 - stringdist(first_aha, first_himss, method = "jw", p = 0.1),
    last_jw = 1 - stringdist(last_aha, last_himss, method = "jw", p = 0.1)
  ) %>%
  group_by(clean_aha) %>%
  mutate(
    high_sim =  any(jw_similarity > 0.9),
    max_sim = max(jw_similarity)
    ) %>%
  ungroup()

view <- name_sim %>%
  arrange(max_sim, full_aha, last_himss)  

titles <- full_df %>% distinct(title_aha)

## units are in (madmin, mname, year)
units <- full_df %>% distinct(full_aha,mname, year) #46135 units

#### CEO ANALYSIS #####
aha_ceos <-  name_sim %>% mutate(cleaned_title_aha = title_aha %>%
                      str_to_lower() %>%
                      str_replace_all("[[:punct:]]", "")) %>%
  filter(str_detect(cleaned_title_aha, "ceo|chief executive")) 

total <- aha_ceos %>% distinct(full_aha, mname, year) 
# there are 33795 full_aha, mname, year combinations

confirmed_ceos_in_same_year <- aha_ceos %>%
  filter(jw_similarity >= .85 & 
           (title_standardized == "CEO:  Chief Executive Officer" |
              str_detect(title, "CEO")) | str_detect(title, "C.E.O") | 
           str_detect(title, "Chief Executive"))

accounted_for_1 <- confirmed_ceos_in_same_year %>% distinct(full_aha, mname, year)
# of these, 25094 have an exact-ish match with a CEO in HIMSS

# want to drop all (aha_name, year) combinations that are already accounted for
remaining1 <- aha_ceos %>%
  anti_join(confirmed_ceos_in_same_year, by = c("full_aha", "mname", "year"))

unaccounted_for <- remaining1 %>% distinct(full_aha, mname, year) #13296 units

## implement name similarity measures
# Step 1: Prepare the matrix once

fuzzy_names_ceos <- remaining1 %>%
  rowwise() %>%
  mutate(
    nick_1 = names_in_same_row_dict(first_aha, first_himss, dict1),
    nick_2 = names_in_same_row_dict(first_aha, first_himss, dict2),
    nick_3 = names_in_same_row_dict(first_aha, first_himss, dict3)
  ) %>%
  ungroup()

nickname_and_ceo <- fuzzy_names_ceos %>%
  filter(last_jw >=  .85 & (nick_1 | nick_2 | nick_3) & 
           (title_standardized == "CEO:  Chief Executive Officer" |
              str_detect(title, "CEO")) | str_detect(title, "C.E.O") | 
           str_detect(title, "Chief Executive"))
accounted_for_2 <- nickname_and_ceo %>% distinct(full_aha, mname, year) 
# another 1242 can be accounted for using nicknames
# 26336 / 33795 CEOs accounted for at this checkpoint

remaining2 <- fuzzy_names_ceos %>%
  anti_join(nickname_and_ceo, by = c("full_aha", "mname", "year"))

last_name_overlap <- function(last_aha, last_himss) {
  last_aha <- tolower(last_aha)
  last_himss <- tolower(last_himss)
  
  grepl(last_aha, last_himss, fixed = TRUE) || grepl(last_himss, last_aha, fixed = TRUE)
}

last_flag <- remaining2 %>%
  rowwise() %>%
  mutate(last_match = last_name_overlap(last_aha, last_himss),
         first_match = last_name_overlap(first_aha, first_himss)) %>%
  ungroup() 

last_subsets <- last_flag %>% 
  filter(((last_match & (nick_1 | nick_2 | nick_3|first_jw > .8)) |
            (first_match & last_jw > .8)) & 
           (title_standardized == "CEO:  Chief Executive Officer" |
              str_detect(title, "CEO")) | str_detect(title, "C.E.O") | 
           str_detect(title, "Chief Executive"))

accounted_for_3 <- last_subsets %>% distinct(full_aha, mname, year) 
# another 130 can be accounted for using nicknames
# 26466 / 33795 CEOs accounted for at this checkpoint

all_combos <- rbind(accounted_for_1,accounted_for_2,accounted_for_3) %>% distinct(full_aha, mname)

remaining3 <- last_flag %>%
  anti_join(last_subsets, by = c("full_aha", "mname", "year"))


view <- remaining3 %>% arrange(full_aha, last_himss)


##### TEST - this means ~200 basically have the same name but aren't
#### CEOs in HIMSS
test <- remaining1 %>% filter(jw_similarity == max_sim & max_sim >= 0.9)
remaining2 <- remaining1 %>% 
  filter(!(jw_similarity == max_sim & max_sim >= 0.9))

unaccounted_for <- remaining2 %>% distinct(full_aha, mname, year) #13030 units

###### look at remaining CEOS in AHA #####
unaccounted_ceo_df <- remaining1 %>%
  mutate(cleaned_title_aha = title_aha %>%
           str_to_lower() %>%
           str_replace_all("[[:punct:]]", "")) %>%
  filter(str_detect(cleaned_title_aha, "ceo|chief executive")) 

unaccounted_ceo_count <- unaccounted_ceo_df %>% distinct(full_aha, mname, year) #8701 units

# check if there are people in himss who aren't listed as ceos in himss
non_himss_ceos <- unaccounted_ceo_df %>% filter(jw_similarity == max_sim & 
                                                  max_sim >= 0.9)
remaining_ceos <- unaccounted_ceo_df %>% 
  filter(!(jw_similarity == max_sim & max_sim >= 0.9))

unaccounted_for <- remaining_ceos %>% distinct(full_aha, mname, year) #8620 units

# run jw
high_separate <- 




##### POST CEO EXPLORATION ####
check_interim <- remaining1 %>% filter(jw_similarity == max_sim) %>%
  filter(str_detect(title_aha, "nterim"))


## non ceo side quest
confirmed_ppl_in_same_year <- remaining1 %>%
  filter(jw_similarity >= .85)

num <- confirmed_ppl_in_same_year %>% distinct(full_aha, mname, year)

title_counts <- confirmed_ppl_in_same_year %>%
  count(title_standardized) %>%
  mutate(percentage = n / sum(n))

ggplot(title_counts, aes(x = "", y = percentage, fill = title_standardized)) +
  geom_col(width = 1, color = "white") +
  coord_polar(theta = "y") +
  labs(title = "Distribution of Titles", fill = "Title") +
  theme_void()

freeform_title_counts <- confirmed_ppl_in_same_year %>%
  count(title) %>%
  mutate(percentage = n / sum(n))

ggplot(freeform_title_counts, aes(x = "", y = percentage, fill = title)) +
  geom_col(width = 1, color = "white") +
  coord_polar(theta = "y") +
  labs(title = "Distribution of Titles", fill = "Title") +
  theme_void()


confirmed_ceos_in_same_year <- remaining1 %>%
  filter(jw_similarity > .8 & 
           (title_standardized == "CEO:  Chief Executive Officer" |
              str_detect(title, "CEO")))


view_himss <- view %>% filter(mname == "penn state milton s. hershey medical center") %>%
  filter(year >= 2013 & title_standardized == "CEO:  Chief Executive Officer")

verify_aha <- true_aha %>% filter(mname == "penn state milton s. hershey medical center")