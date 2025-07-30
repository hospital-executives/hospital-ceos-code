## this file determines how many CEOs in AHA appear in HIMSS
library(dplyr)
library(fuzzyjoin)
library(arrow)
library(tidyr)
library(stringdist)

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
  filter(year > 2008) %>%
  rename(full_name = full_aha) #39,138 obs

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
    aha_aha = mname.x,
    himss_aha = mname.y,
    aha_year = year.x,
    himss_year = year.y,
    full_aha = full_name.x,
    full_himss = full_name.y
  ) %>% #filter(!is.na(full_himss)) %>% # i am suspect of this
  left_join(aha_mini) %>% left_join(himss_mini)

total_combos <- cleaned_joined %>% distinct(full_aha, aha_year, aha_mname) #

## implement jw match
confirmed_same <- cleaned_joined %>% filter(himss_year == aha_year & 
                                              aha_mname == himss_mname & 
                                              name_dist <=.15) #23291
remaining1 <- cleaned_joined %>% 
  anti_join(confirmed_same, by = c("full_aha", "aha_mname", "aha_year")) 

get_count <- remaining1 %>% distinct(full_aha, aha_year, aha_mname) #26257

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
           aha_mname == himss_mname) #1481

accounted_for <- rbind(
  confirmed_same %>% distinct(full_aha, aha_year, aha_mname),
  nickname_and_ceo %>% distinct(full_aha, aha_year, aha_mname)) %>% distinct() %>%
  mutate(match_type = "exact")

remaining2 <- fuzzy_names_ceos %>% 
  anti_join(nickname_and_ceo, by = c("full_aha", "aha_mname", "aha_year")) 
get_count <- remaining2 %>% distinct(full_aha, aha_year, aha_mname) #26257

cat("CHECKPOINT:", nrow(accounted_for) + nrow(get_count))
cat("\nTotal Determined: ", nrow(accounted_for))
cat("\n% Determined: ", nrow(accounted_for)/49548)

## attributable to year mismatch
fuzzy_year <- remaining2 %>%
  filter(abs(himss_year - aha_year) <= 2 & himss_mname == aha_mname & 
           ((last_jw <= .15 & (nick_1 | nick_2 | nick_3)) |
              (last_jw <= .15 & first_jw <= .15) | name_dist <= .15)) %>%
  arrange(full_aha, name_dist)

year_mismatch <- fuzzy_year %>% distinct(full_aha, aha_year, aha_mname) %>%
  mutate(match_type = "fuzzy_year")
accounted_for <- rbind(accounted_for, year_mismatch) %>% distinct() 

remaining3 <- remaining2 %>% 
  anti_join(fuzzy_year, by = c("full_aha", "aha_mname", "aha_year"))

get_count <- remaining3 %>% distinct(full_aha, aha_year, aha_mname)
cat("CHECKPOINT:", nrow(accounted_for) + nrow(get_count))
cat("\nTotal Determined: ", nrow(accounted_for))
cat("\n% Determined: ", nrow(accounted_for)/49548)

check_fuzzy <- fuzzy_year %>%
  mutate(himss_leq = himss_year < aha_year,
         aha_leq = aha_year < himss_year,
         equal = aha_year == himss_year) %>% 
  distinct(full_aha, aha_year, aha_mname, himss_leq, aha_leq, equal) %>%
  summarize(
    himss_leq_count = sum(himss_leq, na.rm = TRUE),
    aha_leq_count   = sum(aha_leq, na.rm = TRUE),
    equal_count     = sum(equal, na.rm = TRUE)
  )

check_final <- final %>% filter(full_name == "anneplatt") %>%
  distinct(year, entity_name, mname)


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
  filter(aha_mname == himss_mname & himss_year == aha_year & 
        ((last_substring & (first_jw <= .15 | nick_1 | nick_2 | 
                             nick_3 | first_substring)) | 
          (first_substring & (last_jw <= .15)))) %>% mutate(match_type = "exact")

substring_hit_fuzzy_year <- substrings %>% filter(!is.na(full_himss)) %>%
  filter(aha_mname == himss_mname & abs(himss_year - aha_year) <= 2 & 
           ((last_substring & (first_jw <= .15 | nick_1 | nick_2 | 
                                 nick_3 | first_substring)) | 
              (first_substring & (last_jw <= .15)))) %>% mutate(match_type = "fuzzy_year")

accounted_for <- rbind(
  confirmed_same %>% distinct(full_aha, aha_year, aha_mname),
  nickname_and_ceo %>% distinct(full_aha, aha_year, aha_mname)) %>% distinct() %>%
  mutate(match_type = "exact")
accounted_for <- rbind(accounted_for, year_mismatch) %>% distinct() 
accounted_for <- bind_rows(
  accounted_for,
  substring_hit %>% distinct(full_aha, aha_mname, aha_year, match_type) #,
  #substring_hit_fuzzy_year %>% distinct(full_aha, aha_mname, aha_year, match_type)
) %>%
  distinct()

remaining4a <- substrings %>% 
  anti_join(substring_hit, by = c("full_aha", "aha_mname", "aha_year"))

get_count <- remaining4a %>% distinct(full_aha, aha_year, aha_mname)
cat("CHECKPOINT:", nrow(accounted_for) + nrow(get_count))
cat("\nTotal Determined: ", nrow(accounted_for))
cat("\n% Determined: ", nrow(accounted_for)/39138)


## this just tells you that there's a potential system hit
any_match <- remaining4a %>% filter(!is.na(full_himss)) %>%
  mutate(mname_jw = stringdist(himss_mname, aha_mname, method = "jw", p = 0.1)) %>%
  filter((first_jw < .2 | last_jw < .2 | name_dist < .2 | nick_1 | nick_2 | 
           nick_3 | first_substring | last_substring) & himss_year == aha_year & mname_jw <=.15) 

  
hmmm <- remaining4a %>%   mutate(mname_jw = stringdist(himss_mname, aha_mname, method = "jw", p = 0.1)) %>%
filter(aha_year == himss_year & mname_jw <=.2) %>% arrange(last_aha, first_aha)

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
  anti_join(accounted_for %>% rename(mname = aha_mname, year = aha_year), 
            by = c("full_aha", "mname", "year"))

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
         last_aha, year, title_standardized, title, mname)
  
cleaned <- view %>%
  mutate(
    title_clean = str_to_lower(str_replace_all(title, "[[:punct:]]", " ")),
    match_type = case_when(
      title_standardized == "CEO:  Chief Executive Officer" ~ "exact",
      str_detect(title_clean, "chief executive") ~ "exact",
      str_detect(title_clean, "\\bceo\\b") ~ "exact",
      TRUE ~ "TBD"
    )
  ) %>% distinct(full_aha, year, mname, match_type) 

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

check_cleaned <- inner_join(cleaned, all_aha_ceos %>% rename(full_aha = full_name))

combined <- rbind(accounted_for, 
                  check_cleaned %>% rename(aha_mname = mname, aha_year = year)) %>% 
  distinct(full_aha, aha_mname, aha_year, match_type)

missing <- all_aha_ceos %>% anti_join(combined %>% 
                                        rename(full_name = full_aha, 
                                               mname = aha_mname,
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

## get year mismatch
year_mismatch_df <- combined %>% filter(match_type == "fuzzy_year") %>% 
  mutate(himss_leq = himss_year)



# drop pre 2009
drop_pre_2009 <- missing %>% filter(year > 2008)

# there are 16740 combinations that are still missing
# however, only 7606 have an mname that ever appears in himss
in_himss_mnames <- (intersect(final$mname, missing$mname))
in_himss <- drop_pre_2009 %>% filter(mname %in% in_himss_mnames)

# verify that cases not in himss should not be in df
not_in_himss <- drop_pre_2009 %>% filter(!mname %in% in_himss_mnames)

mname_hrr_xwalk <- true_aha %>% 
  separate(madmin, into = c("aha_name", "title_aha"), 
           sep = ",", extra = "merge", fill = "right") %>%
  mutate(cleaned_title_aha = title_aha %>%
           str_to_lower() %>%
           str_replace_all("[[:punct:]]", "")) %>%
  filter(str_detect(cleaned_title_aha, "ceo|chief executive")) %>% 
  mutate(full_aha = str_to_lower(str_trim(str_remove_all(aha_name, "[[:punct:]]")))) %>%
  distinct(full_aha, mname, hrrname, year)

drop_non_us <- not_in_himss %>% left_join(mname_hrr_xwalk) %>% 
  filter(!is.na(hrrname))
# this gets rid of 335 rows where the hospital is not in the U.S.
# look at nasir - different people
# and mcgeachey and clare haar

check_aha <- drop_non_us %>% filter(last_aha == "mcgeachey")
check_final <- final %>% filter(lastname == "mcgeachey") %>%
  distinct(firstname, lastname, entity_name, mname, year, entity_uniqueid, system_id, haentitytypeid)

himss_mini <- final %>% 
  distinct(full_name, firstname, lastname, year, mname, title_standardized, title) 

test_merge <- missing %>%
  rename(firstname = first_aha, lastname = last_aha) %>%
  left_join(himss_mini, by = c("firstname", "lastname")) %>%
  rename(aha_full = full_name.x,
         aha_mname = mname.x,
         aha_year = year.x,
         himss_full = full_name.y,
         himss_year = year.y,
         himss_mname = mname.y
         ) %>% filter(!is.na(himss_year))

exact <- test_merge %>% filter(aha_mname == himss_mname & 
                                 abs(aha_year - himss_year) <=2)

check_final <- final %>% filter(campus_aha == "6040002") %>% #filter(str_detect(entity_address, "avenida de diego")) %>%
  distinct(year, entity_name, firstname, title_standardized, title, madmin, mname)
  
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

filtered_remaining4 <-  semi_join(
  remaining4a %>% select(contains("aha")) %>% distinct() %>% rename(mname = aha_mname),
  non_ceos, by = "mname")

title_join <- filtered_remaining4 %>%
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

get_count <- filtered_remaining4 %>% distinct(full_aha, aha_year, mname)
cat("CHECKPOINT:", nrow(accounted_for) + nrow(get_count))
cat("\nTotal Determined: ", nrow(accounted_for))
cat("\n% Determined: ", nrow(accounted_for)/49548)

matched_full <- cleaned_title %>%
  filter(full_jw <= .15 & 
           himss_year == aha_year & 
           aha_mname == himss_mname)

get_count <- matched_full %>% distinct(full_aha, aha_year, aha_mname) #457

updated_combos <- rbind(combined, 
                        get_count %>% mutate(match_type = "TBD")) %>% 
  distinct(full_aha, aha_year, aha_mname)


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

remaining5 <- remaining4 %>% 
  anti_join(matched_fuzzy, by = c("full_aha", "aha_mname", "aha_year")) 

## attributable to both
fuzzy_year_title <- remaining4 %>%
  filter(abs(himss_year - aha_year) <= 2 & himss_mname == aha_mname & 
           ((last_jw <= .15 & (nick_1 | nick_2 | nick_3)) |
              (last_jw <= .15 & first_jw <= .15) | full_jw <= .15)) %>%
  arrange(full_aha, full_jw)

get_count <- fuzzy_year_title %>% distinct(full_aha, aha_year, aha_mname) #167

remaining6 <- remaining5 %>% 
  anti_join(fuzzy_year_title, by = c("full_aha", "aha_mname", "aha_year")) 

get_count <- remaining5 %>% distinct(full_aha, aha_year, aha_mname) #8539

remaining_ceos <- remaining3 %>% rename(aha_mname = mname) %>%
  anti_join(matched_full, by = c("full_aha", "aha_mname", "aha_year")) %>% 
  anti_join(matched_fuzzy, by = c("full_aha", "aha_mname", "aha_year")) %>% 
  anti_join(fuzzy_year_title, by = c("full_aha", "aha_mname", "aha_year")) 

test <- remaining_ceos %>% rename(year = aha_year, mname = aha_mname) %>%
  left_join(himss_mini %>% rename(mname = himss_mname, year = himss_year)) %>% 
  mutate(last_jw = stringdist(last_aha, last_himss, method = "jw", p = 0.1),
         first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1),
         full_jw = stringdist(full_aha, full_himss, method = "jw", p = 0.1)) %>%
  rowwise() %>%
  mutate(
    nick_1 = names_in_same_row_dict(first_aha, first_himss, dict1),
    nick_2 = names_in_same_row_dict(first_aha, first_himss, dict2),
    nick_3 = names_in_same_row_dict(first_aha, first_himss, dict3)
  )

any_match <- test %>% filter(!is.na(full_himss)) %>%
  filter(first_jw < .2 | last_jw < .2 | full_jw < .2 | nick_1 | nick_2 | nick_3)
get_count <- any_match %>% distinct(full_aha, year, mname) #1864


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

