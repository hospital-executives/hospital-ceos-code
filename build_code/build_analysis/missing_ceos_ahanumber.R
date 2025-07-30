## this file determines how many CEOs in AHA appear in HIMSS
library(dplyr)
library(fuzzyjoin)
library(arrow)
library(tidyr)
library(stringdist)
library(tiyverse)

##### Part 1: Clean AHA Data #####
rm(list = ls())
final_path <- "/Users/katherinepapen/Library/CloudStorage/Dropbox/hospital_ceos/_data/derived/final_aha.feather"
final <- read_feather(final_path)
true_aha <- read_feather("/Users/katherinepapen/Desktop/clean_aha.feather")
data_path <- "/Users/katherinepapen/Library/CloudStorage/Dropbox/hospital_ceos/_data/"

suffixes <- c("i", "ii", "iii", "iv", "v", "jr", "sr", "j.r.", "s.r.")

cleaned_aha <- true_aha %>%
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
  select(-c(first_word, second_word)) 

##### Part 2: How many CEOs are in AHA that I am counting as missing in HIMSS? #####
all_aha_ceos <- cleaned_aha %>% distinct(full_aha, ahanumber, year) %>% 
  filter(year > 2008 & 
           (ahanumber %in% unique(final$campus_aha) |
              ahanumber %in% unique(final$entity_aha))) %>%
  rename(full_name = full_aha) #36610

all_himss_ceos <- final %>% 
  filter(title_standardized == "CEO:  Chief Executive Officer" |
           str_detect(title, "CEO") | str_detect(title, "C.E.O") | 
           str_detect(title, "Chief Executive")) %>% 
  distinct(full_name, entity_aha, year) %>% rename(ahanumber = entity_aha)
# 77,024 obs

joined <- all_aha_ceos %>% #first pass 23,291 matched on year + mname
  stringdist_left_join(all_himss_ceos,
                       by = "full_name",
                       method = "jw",   # Jaro-Winkler
                       max_dist = .2, # adjust threshold (1 = no match, 0 = exact)
                       distance_col = "name_dist") 

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
  ) %>% #filter(!is.na(full_himss)) %>% # i am suspect of this
  left_join(aha_mini) %>% left_join(himss_mini)

total_combos <- cleaned_joined %>% distinct(full_aha, aha_year, aha_aha) #

## implement jw match
confirmed_same <- cleaned_joined %>% filter(himss_year == aha_year & 
                                              aha_aha == himss_aha & 
                                              name_dist <=.15) #23291
remaining1 <- cleaned_joined %>% 
  anti_join(confirmed_same, by = c("full_aha", "aha_aha", "aha_year")) 

get_count <- remaining1 %>% distinct(full_aha, aha_year, aha_aha) #26257

fuzzy_names_ceos <- remaining1 %>%
  mutate(last_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1),
         first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1)) %>%
  rowwise() %>%
  mutate(
    nick_1 = names_in_same_row_dict(first_aha, first_himss, dict1),
    nick_2 = names_in_same_row_dict(first_aha, first_himss, dict2),
    nick_3 = names_in_same_row_dict(first_aha, first_himss, dict3)
  ) %>%
  ungroup()

nickname_and_ceo <- fuzzy_names_ceos %>%
  mutate(separates_flag = (last_jw <= .15 & first_jw <= .15)) %>%
  filter(((last_jw <= .2 & (nick_1 | nick_2 | nick_3)) |
            (last_jw <= .15 & first_jw <= .15)) & 
           himss_year == aha_year & 
           aha_aha == himss_aha) #1481

accounted_for <- rbind(
  confirmed_same %>% distinct(full_aha, aha_year, aha_aha),
  nickname_and_ceo %>% distinct(full_aha, aha_year, aha_aha)) %>% distinct() %>%
  mutate(match_type = "exact")

remaining2 <- fuzzy_names_ceos %>% 
  anti_join(nickname_and_ceo, by = c("full_aha", "aha_aha", "aha_year")) 
get_count <- remaining2 %>% distinct(full_aha, aha_year, aha_aha) #26257

cat("CHECKPOINT:", nrow(accounted_for) + nrow(get_count))
cat("\nTotal Determined: ", nrow(accounted_for))
cat("\n% Determined: ", nrow(accounted_for)/nrow(all_aha_ceos))

## attributable to year mismatch
fuzzy_year <- remaining2 %>%
  filter(abs(himss_year - aha_year) <= 2 & himss_aha == aha_aha & 
           ((last_jw <= .15 & (nick_1 | nick_2 | nick_3)) |
              (last_jw <= .15 & first_jw <= .15) | name_dist <= .15)) %>%
  arrange(full_aha, name_dist)

year_mismatch <- fuzzy_year %>% distinct(full_aha, aha_year, aha_aha) %>%
  mutate(match_type = "fuzzy_year")
accounted_for <- rbind(accounted_for, year_mismatch) %>% distinct() 

remaining3 <- remaining2 %>% 
  anti_join(fuzzy_year, by = c("full_aha", "aha_aha", "aha_year"))

get_count <- remaining3 %>% distinct(full_aha, aha_year, aha_aha)
cat("CHECKPOINT:", nrow(accounted_for) + nrow(get_count))
cat("\nTotal Determined: ", nrow(accounted_for))
cat("\n% Determined: ", nrow(accounted_for)/nrow(all_aha_ceos))

check_fuzzy <- fuzzy_year %>%
  mutate(himss_leq = himss_year < aha_year,
         aha_leq = aha_year < himss_year,
         equal = aha_year == himss_year) %>% 
  distinct(full_aha, aha_year, aha_aha, himss_leq, aha_leq, equal) %>%
  summarize(
    himss_leq_count = sum(himss_leq, na.rm = TRUE),
    aha_leq_count   = sum(aha_leq, na.rm = TRUE),
    equal_count     = sum(equal, na.rm = TRUE)
  )



last_name_overlap <- function(last_aha, last_himss) {
  last_aha <- tolower(last_aha)
  last_himss <- tolower(last_himss)
  
  grepl(last_aha, last_himss, fixed = TRUE) || grepl(last_himss, last_aha, fixed = TRUE)
}

substrings <- remaining3 %>%
  rowwise() %>%
  mutate(last_substring = last_name_overlap(last_aha, last_himss),
         first_substring = last_name_overlap(first_aha, first_himss)) %>%
  ungroup() 

substring_hit <- substrings %>% filter(!is.na(full_himss)) %>%
  filter(aha_aha == himss_aha & himss_year == aha_year & 
           ((last_substring & (first_jw <= .15 | nick_1 | nick_2 | 
                                 nick_3 | first_substring)) | 
              (first_substring & (last_jw <= .15)))) %>% mutate(match_type = "exact")

substring_hit_fuzzy_year <- substrings %>% filter(!is.na(full_himss)) %>%
  filter(aha_aha == himss_aha & abs(himss_year - aha_year) <= 2 & 
           ((last_substring & (first_jw <= .15 | nick_1 | nick_2 | 
                                 nick_3 | first_substring)) | 
              (first_substring & (last_jw <= .15)))) %>% mutate(match_type = "fuzzy_year")

accounted_for <- rbind(
  confirmed_same %>% distinct(full_aha, aha_year, aha_aha),
  nickname_and_ceo %>% distinct(full_aha, aha_year, aha_aha)) %>% distinct() %>%
  mutate(match_type = "exact")
accounted_for <- rbind(accounted_for, year_mismatch) %>% distinct() 
accounted_for <- bind_rows(
  accounted_for,
  substring_hit %>% distinct(full_aha, aha_aha, aha_year, match_type) #,
  #substring_hit_fuzzy_year %>% distinct(full_aha, aha_mname, aha_year, match_type)
) %>%
  distinct()

remaining4a <- substrings %>% 
  anti_join(substring_hit, by = c("full_aha", "aha_aha", "aha_year"))

get_count <- remaining4a %>% distinct(full_aha, aha_year, aha_aha)
cat("CHECKPOINT:", nrow(accounted_for) + nrow(get_count))
cat("\nTotal Determined: ", nrow(accounted_for))
cat("\n% Determined: ", nrow(accounted_for)/39138)


## check himss
check_himss <- final %>% 
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
  anti_join(accounted_for %>% rename(entity_aha = aha_aha, year = aha_year), 
            by = c("full_aha", "entity_aha", "year"))

check <- check_himss %>% 
  rename(last_himss = lastname, first_himss = firstname, full_himss = full_name) %>%
  mutate(
    full_jw = stringdist(full_aha, full_himss, method = "jw", p = 0.1),
    last_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1),
    first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1)) %>%
  rowwise() %>%
  mutate(
    nick_1 = names_in_same_row_dict(first_aha, first_himss, dict1),
    nick_2 = names_in_same_row_dict(first_aha, first_himss, dict2),
    nick_3 = names_in_same_row_dict(first_aha, first_himss, dict3)
  )

view <- check %>% filter(full_jw <= .1 | 
                           (last_jw <= .1 & (nick_1 | nick_2 | nick_3 | first_jw <= .1))) %>% 
  select(full_jw, full_himss, full_aha, first_himss, first_aha, last_himss,
         last_aha, year, title_standardized, title, entity_aha)

cleaned <- view %>%
  mutate(
    title_clean = str_to_lower(str_replace_all(title, "[[:punct:]]", " ")),
    match_type = case_when(
      title_standardized == "CEO:  Chief Executive Officer" ~ "exact",
      str_detect(title_clean, "chief executive") ~ "exact",
      str_detect(title_clean, "\\bceo\\b") ~ "exact",
      TRUE ~ "TBD"
    )
  ) %>% distinct(full_aha, year, entity_aha, match_type)

##### code to check distribution of non ceo jobs ####
non_ceos <- view %>%
  mutate(
    title_clean = str_to_lower(str_replace_all(title, "[[:punct:]]", " ")),
    match_type = case_when(
      title_standardized == "CEO:  Chief Executive Officer" ~ "exact",
      str_detect(title_clean, "chief executive") ~ "exact",
      str_detect(title_clean, "\\bceo\\b") ~ "exact",
      TRUE ~ "TBD"
    )
  ) %>% filter(match_type == "TBD")

pie_data <- non_ceos %>%
  count(title_standardized) %>%
  mutate(perc = n / 572 * 100) ,
label = paste0(title_standardized))

# Plot pie chart
ggplot(pie_data, aes(x = "", y = n, fill = title_standardized)) +
  geom_col(width = 1) +
  coord_polar(theta = "y") +
  labs(title = "Distribution of Title (Non-CEOs)",
       fill = "Title") +
  theme_void() +
  geom_text(aes(label = label), position = position_stack(vjust = 0.5), size = 3)

#### re merge ####
check_cleaned <- inner_join(cleaned %>% mutate(ahanumber = entity_aha),
                            all_aha_ceos %>% rename(full_aha = full_name))

combined <- rbind(accounted_for, 
                  check_cleaned %>% rename(aha_aha = ahanumber, aha_year = year) %>%
                    select(-entity_aha)) %>% 
  distinct(full_aha, aha_aha, aha_year, match_type)

missing <- all_aha_ceos %>% anti_join(combined %>% 
                                        rename(full_name = full_aha, 
                                               ahanumber = aha_aha,
                                               year = aha_year)) %>%
  mutate(full_aha = str_trim(str_remove_all(full_name, "[[:punct:]]")),
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

still_missing <- missing %>% distinct(full_aha, ahanumber, year)

cat("% accounted for: ", 1 - nrow(still_missing)/nrow(all_aha_ceos))

## check merge on enti

## check
merged_haentity <- read_feather(paste0(derived_data,'/himss_aha_hospitals_final.feather'))

check_final <- merged_haentity %>%
  group_by(ahanumber,year) %>% 
  filter(sum(is_hospital, na.rm = TRUE) > 1) %>%
  ungroup() %>% select(ahanumber, entity_aha, year, is_hospital, entity_name, haentitytypeid)

still_incorrect <- unique(check_final$entity_aha)

check_final_mini <- step3 %>% 
  filter(sys_aha==6230054) %>%
  select(entity_name, entity_address, clean_aha, sys_aha, year, haentitytypeid)

merged_haentity <- read_feather(paste0(derived_data,'/himss_aha_hospitals_final.feather'))

check_build <- merged_haentity %>% filter(campus_aha == 6140002) %>%
  select(ahanumber, campus_aha, entity_aha, entity_uniqueid)

