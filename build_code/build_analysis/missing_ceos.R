## this file determines how many CEOs in AHA appear in HIMSS
library(dplyr)
library(fuzzyjoin)
library(arrow)
library(tidyr)
library(stringdist)

##### Part 1: Clean AHA Data #####
true_aha <- read_feather("/Users/katherinepapen/Desktop/clean_aha.feather")

suffixes <- c("i", "ii", "iii", "iv", "v", "jr", "sr", "j.r.", "s.r.")

cleaned_aha <- true_aha %>%
  separate(madmin, into = c("full_aha", "title_aha"), 
           sep = ",", extra = "merge", fill = "right") %>%
  mutate(cleaned_title_aha = title_aha %>%
           str_to_lower() %>%
           str_replace_all("[[:punct:]]", "")) %>%
  filter(str_detect(cleaned_title_aha, "ceo|chief executive")) %>% 
  mutate(last_aha = if_else(
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

##### Part 2: How many CEOs are in AHA that I am counting as missing in HIMSS? #####
all_aha_ceos <- cleaned_aha %>% distinct(full_aha, mname, year) %>%
  rename(full_name = full_aha) #49,548 obs

all_himss_ceos <- final %>% 
  filter(title_standardized == "CEO:  Chief Executive Officer" |
           str_detect(title, "CEO") | str_detect(title, "C.E.O") | 
  str_detect(title, "Chief Executive")) %>% distinct(full_name, mname, year)
# 77,024 obs

joined <- all_aha_ceos %>% #first pass 23,291 matched on year + mname
  stringdist_left_join(all_himss_ceos,
                        by = "full_name",
                        method = "jw",   # Jaro-Winkler
                        max_dist = .2, # adjust threshold (1 = no match, 0 = exact)
                        distance_col = "name_dist") 

aha_mini <- cleaned_aha %>% 
  distinct(full_aha, mname, year, first_aha, last_aha) %>%
  rename(aha_year = year,
         aha_mname = mname)

himss_mini <- final %>% 
  filter(title_standardized == "CEO:  Chief Executive Officer" |
           str_detect(title, "CEO") | str_detect(title, "C.E.O") | 
           str_detect(title, "Chief Executive")) %>% 
  rename(
    first_himss = firstname,
    last_himss = lastname,
    full_himss = full_name
  ) %>%
  distinct(full_himss, first_himss, last_himss, year, mname) %>%
  rename(himss_mname = mname,
         himss_year = year)


cleaned_joined <- joined %>%
  rename(
    aha_mname = mname.x,
    himss_mname = mname.y,
    aha_year = year.x,
    himss_year = year.y,
    full_aha = full_name.x,
    full_himss = full_name.y
  ) %>% #filter(!is.na(full_himss)) %>% # i am suspect of this
  left_join(aha_mini) %>% left_join(himss_mini)

total_combos <- cleaned_joined %>% distinct(full_aha, aha_year, aha_mname) #

ever_match_df <- cleaned_joined %>%
  mutate(last_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1),
         first_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1)) %>%
  rowwise() %>%
  mutate(
    nick_1 = names_in_same_row_dict(first_aha, first_himss, dict1),
    nick_2 = names_in_same_row_dict(first_aha, first_himss, dict2),
    nick_3 = names_in_same_row_dict(first_aha, first_himss, dict3)
  )

ever_match <- ever_match_df %>%  
  filter(((last_jw <= .15 & (nick_1 | nick_2 | nick_3)) |
           (last_jw <= .15 & first_jw <= .15) | 
           name_dist <= .15) & himss_mname == aha_mname) %>% distinct(full_aha)


## implement jw match
confirmed_same <- cleaned_joined %>% filter(himss_year == aha_year & 
                                              aha_mname == himss_mname & 
                                              name_dist <=.15) #23291


remaining1 <- cleaned_joined %>% 
  anti_join(confirmed_same, by = c("full_aha", "aha_mname", "aha_year")) 

fuzzy_names_ceos <- remaining1 %>%
  mutate(last_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1),
         first_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1)) %>%
  rowwise() %>%
  mutate(
    nick_1 = names_in_same_row_dict(first_aha, first_himss, dict1),
    nick_2 = names_in_same_row_dict(first_aha, first_himss, dict2),
    nick_3 = names_in_same_row_dict(first_aha, first_himss, dict3)
  ) %>%
  ungroup()

nickname_and_ceo <- fuzzy_names_ceos %>%
  mutate(separates_flag = (last_jw <= .15 & first_jw <= .15)) %>%
  filter(((last_jw <= .15 & (nick_1 | nick_2 | nick_3)) |
           (last_jw <= .15 & first_jw <= .15)) & 
           himss_year == aha_year & 
           aha_mname == himss_mname) #1530


remaining2 <- fuzzy_names_ceos %>% 
  anti_join(nickname_and_ceo, by = c("full_aha", "aha_mname", "aha_year")) 

remaining <- remaining2 %>% distinct(full_aha, aha_year, aha_mname)

## attributable to year mismatch
fuzzy_year <- remaining2 %>%
  filter(abs(himss_year - aha_year) <= 2 & himss_mname == aha_mname & 
           ((last_jw <= .15 & (nick_1 | nick_2 | nick_3)) |
              (last_jw <= .15 & first_jw <= .15) | name_dist <= .15)) %>%
  arrange(full_aha, name_dist)

year_mismatch <- fuzzy_year %>% distinct(full_aha, aha_year, aha_mname)

remaining3 <- remaining2 %>% 
  anti_join(fuzzy_year, by = c("full_aha", "aha_mname", "aha_year")) %>% 
  select(contains("aha")) %>% distinct() %>% rename(mname = aha_mname)

## attributable to title mismatch
non_ceos <- final %>% 
  filter(!(title_standardized == "CEO:  Chief Executive Officer" |
           str_detect(title, "CEO") | str_detect(title, "C.E.O") | 
           str_detect(title, "Chief Executive"))) %>% 
  distinct(full_name, mname, year)

himss_mini_no_ceo <- final %>% 
  filter(!(title_standardized == "CEO:  Chief Executive Officer" |
           str_detect(title, "CEO") | str_detect(title, "C.E.O") | 
           str_detect(title, "Chief Executive"))) %>% 
  rename(
    first_himss = firstname,
    last_himss = lastname,
    full_himss = full_name
  ) %>%
  distinct(full_himss, first_himss, last_himss, year, mname, 
           title_standardized, title) %>%
  rename(himss_mname = mname,
         himss_year = year)

filtered_remaining3 <- semi_join(remaining3, non_ceos, by = "mname")

title_join <- filtered_remaining3 %>%
  rename(full_name = full_aha) %>%
  stringdist_left_join(non_ceos,
                       by = "full_name",
                       method = "jw",
                       max_dist = 0.2,
                       distance_col = "full_jw") 

cleaned_title <- title_join %>% rename(
  aha_mname = mname.x,
  himss_mname = mname.y,
  himss_year = year,
  full_aha = full_name.x,
  full_himss = full_name.y
  ) %>% #filter(!is.na(full_himss)) %>% # i am suspect of this
    left_join(aha_mini) %>% left_join(himss_mini_no_ceo) 

matched_full <- cleaned_title %>%
  filter(full_jw <= .15 & 
           himss_year == aha_year & 
           aha_mname == himss_mname)

get_count <- matched_full %>% distinct(full_aha, aha_year, aha_mname) #457

remaining4 <- cleaned_title %>% 
  anti_join(matched_full, by = c("full_aha", "aha_mname", "aha_year")) %>% 
  mutate(last_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1),
         first_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1)) %>%
  rowwise() %>%
  mutate(
    nick_1 = names_in_same_row_dict(first_aha, first_himss, dict1),
    nick_2 = names_in_same_row_dict(first_aha, first_himss, dict2),
    nick_3 = names_in_same_row_dict(first_aha, first_himss, dict3)
  ) %>%
  ungroup()

matched_fuzzy <- remaining4 %>%
  filter(((last_jw <= .15 & (nick_1 | nick_2 | nick_3)) |
                    (last_jw <= .15 & first_jw <= .15)) & 
  himss_year == aha_year & 
  aha_mname == himss_mname)

get_count <- matched_fuzzy %>% distinct(full_aha, aha_year, aha_mname) #30

## attributable to both
fuzzy_year_title <- remaining4 %>%
  filter(abs(himss_year - aha_year) <= 2 & himss_mname == aha_mname & 
           ((last_jw <= .15 & (nick_1 | nick_2 | nick_3)) |
              (last_jw <= .15 & first_jw <= .15) | full_jw <= .15)) %>%
  arrange(full_aha, full_jw)

get_count <- fuzzy_year_title %>% distinct(full_aha, aha_year, aha_mname) #167


######
case_study <- final %>% 
  filter(title_standardized == "CEO:  Chief Executive Officer" |
           str_detect(title, "CEO") | str_detect(title, "C.E.O") | 
           str_detect(title, "Chief Executive")) %>%
  filter(lastname == "mortoza") %>% 
  select(firstname, lastname, entity_name, madmin, title,title_standardized, year)

liz <- final %>% filter(lastname == "aderholdt") %>%
  select(mname, firstname, lastname, year, madmin)

  filter(mname.x == mname.y, year.x == year.y)

