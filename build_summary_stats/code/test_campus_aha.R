

#(list = ls())
setwd(dirname(getActiveDocumentContext()$path))
config_path <- file.path("../input/config_path.R")
source(config_path)
source("helper.R")
hospitals_final <- read_feather(paste0(derived_data,'/himss_aha_hospitals_final.feather'))

aha_to_entity_uniqueid <- hospitals_final %>%
  group_by(campus_aha, year) %>%
  summarise(
    target_entity = first(
      entity_uniqueid[entity_aha == campus_aha], na_rm = TRUE
    ),
    .groups = "drop"
  )

view_key <- aha_to_entity_uniqueid %>% filter(campus_aha == 636060)

## attempt 1 - match on system ids 
hospitals_final <- hospitals_final %>% 
  rename(himss_sysid = system_id, 
         aha_sysid = sysid) %>%
  group_by(entity_aha, year) %>%
  mutate(entity_himss_sys =  ifelse(is.na(entity_aha), NA, himss_sysid)) %>%
  ungroup() %>%
  group_by(campus_aha, year) %>%
  mutate(
    campus_himss_sys = dplyr::first(
      entity_himss_sys[dplyr::near(entity_aha, campus_aha) & !is.na(entity_himss_sys)],
      default = NA_character_
    )
  ) %>% ungroup() %>%
  mutate(real_campus_aha = 
           ifelse(
             himss_sysid == campus_himss_sys, campus_aha, NA))

sys_matches <- hospitals_final %>% filter(himss_sysid == campus_himss_sys)

### how many do we need to match?
non_na_campus_aha_obs <- nrow(hospitals_final %>% filter(!is.na(campus_aha)))
non_na_campus_aha_entities <- nrow(hospitals_final %>% 
                                     filter(!is.na(campus_aha)) %>% 
                                     distinct(entity_uniqueid))
cat(non_na_campus_aha_obs)
cat(non_na_campus_aha_entities)

assigned_obs <- hospitals_final %>% filter(!is.na(campus_aha)) %>%
   filter((!is.na(entity_aha)))

cat(nrow(assigned_obs))

## matching on names
add_match <-  hospitals_final %>% 
  select(campus_aha, entity_aha, entity_name, mname, entity_address, mlocaddr,
         year, entity_uniqueid, haentitytypeid) %>%
  mutate(
    add_sim = stringsim(entity_address, mlocaddr, method = "jw", p = 0.1), 
    name_sim = stringsim(entity_name, mname, method = "jw", p = 0.1),
    real_campus_aha = ifelse(name_sim >= .85, campus_aha, NA)) %>% 
  filter(is.na(entity_aha) & real_campus_aha == campus_aha & !is.na(campus_aha))

curr_matches <- rbind(assigned_obs %>% 
                        distinct(entity_uniqueid, year, campus_aha),
                      add_match %>% 
                        distinct(entity_uniqueid, year,campus_aha),
                      sys_matches %>% 
                        distinct(entity_uniqueid, year,campus_aha)) %>%
  distinct()

cat(nrow(curr_matches)/non_na_campus_aha_obs)

## match on address + name
add_name_matches <- hospitals_final %>% filter(!is.na(campus_aha)) %>% 
  anti_join(curr_matches)  %>% 
  select(campus_aha, entity_aha, entity_name, mname, entity_address, mlocaddr,
         year, entity_uniqueid, haentitytypeid) %>%  mutate(
    add_sim = stringsim(entity_address, mlocaddr, method = "jw", p = 0.1), 
    name_sim = stringsim(entity_name, mname, method = "jw", p = 0.1),
    real_campus_aha = ifelse(add_sim >= .95 & name_sim >= .75, campus_aha, NA)) %>% 
  filter(is.na(entity_aha) & real_campus_aha == campus_aha & !is.na(campus_aha))

post_add_matches <- rbind(curr_matches, 
                          add_name_matches %>% distinct(entity_uniqueid, year, campus_aha)) %>%
  distinct()
cat(nrow(post_add_matches)/non_na_campus_aha_obs)

## add cases where not geocoded
not_geocoded <- hospitals_final %>% anti_join(post_add_matches) %>%
  filter(geocoded == "False") %>% 
  select(mname, entity_name, mlocaddr, entity_address, ahanumber, entity_aha, year, entity_uniqueid, campus_aha) %>%
  mutate(mname = as.character(mname), entity_name = as.character(entity_name),
         jw_sim = ifelse(
           !is.na(mname),
           1 - stringdist(entity_name, mname, method = "jw", p = 0.1),
           NA_real_
         )
  ) %>% filter(jw_sim >= .75)

post_geo_matches <- rbind(post_add_matches, 
                          not_geocoded %>% distinct(entity_uniqueid, year, campus_aha)) %>%
  distinct()
cat(nrow(post_geo_matches)/non_na_campus_aha_obs)

## check tokens



library(dplyr)
library(purrr)
library(stringr)
library(tidyr)
library(tidytext)

# helper: tokenize a character vector into a list-column of tokens per row
tokenize_col <- function(x) {
  x %>%
    coalesce("") %>%                          # replace NA with empty string
    str_to_lower() %>%
    # keep letters/digits; split on non-alphanum
    str_split("[^[:alnum:]]+") %>%
    map(~ .x[.x != ""] %>% unique())          # drop empties; dedupe within row
}

# helper: rare tokens by quantile from a single text column of a data frame
rare_tokens_q <- function(data, col, q = 0.1) {
  token_counts <- data %>%
    select({{col}}) %>%
    unnest_tokens(token, {{col}}) %>%         # one token per row across the column
    count(token, sort = TRUE)
  
  if (nrow(token_counts) == 0) return(character(0))
  
  cutoff <- quantile(token_counts$n, probs = q, na.rm = TRUE)
  token_counts %>%
    filter(n <= cutoff) %>%
    pull(token) %>%
    unique()
}

# ---- main pipeline ----
q_cut <- 0.75  # bottom 10% as "rare"; adjust to taste

hospitals_final <- hospitals_final %>%
  mutate(
    entity_tokens = tokenize_col(entity_name),
    mname_tokens  = tokenize_col(mname)
  )

# rare sets (computed on the original text columns)
rare_entity <- rare_tokens_q(hospitals_final, entity_name, q = q_cut)
rare_mname  <- rare_tokens_q(hospitals_final, mname,       q = q_cut)

# tokens that are rare in BOTH columns
rare_both <- union(rare_entity, rare_mname)

# row-wise flag: overlap contains a token that is rare in BOTH columns
remaining <- hospitals_final %>% filter(!is.na(campus_aha)) %>%
  anti_join(post_geo_matches) 

remaining <- remaining %>%
  mutate(
    token_flag = map2_lgl(entity_tokens, mname_tokens, ~ {
      if (length(.x) == 0L || length(.y) == 0L) return(FALSE)
      any(intersect(.x, .y) %in% rare_both)
    })
  )

check_overlap <- remaining %>% filter(token_flag) %>%
  distinct(entity_name, mname, mlocaddr, entity_address)

name_sim_2 <-  hospitals_final %>% filter(!is.na(campus_aha)) %>%
  anti_join(post_geo_matches) %>%
  distinct(entity_name, mname, campus_aha, entity_aha, mlocaddr, entity_address, mloccity, entity_city,
           himss_sysid, campus_himss_sys) %>%
  mutate(
    jw_flag = stringsim(
      word(!!sym("mname"), 1),   # first word from col1
      word(!!sym("entity_name"), 1),   # first word from col2
      method = "jw"
    ) > 0.975
  ) %>% filter(jw_flag)

## test post geo matches 
check <- post_geo_matches %>% left_join(hospitals_final) %>%
  group_by(ahanumber, year) %>%
  filter(n() > 1) %>%
  ungroup() %>%
  distinct(entity_name, mname, mlocaddr, entity_address, entity_aha, campus_aha, year) %>%
  arrange(campus_aha, year)


remaining <- hospitals_final %>% filter(!is.na(campus_aha)) %>%
  anti_join(post_geo_matches) %>%
  distinct(entity_name, mname, campus_aha, entity_aha, mlocaddr, entity_address, mloccity, entity_city,
           himss_sysid, campus_himss_sys) %>%
  mutate(add_sim = stringsim(entity_address, mlocaddr, method = "jw", p = 0.1),
         name_sim = stringsim(entity_name, mname, method = "jw", p = 0.1))


## check matches
merged <- aha_clean %>% 
  anti_join(post_add_matches %>% 
              rename(ahanumber = campus_aha)) %>%
  left_join(hospitals_final %>% 
              select(year, entity_uniqueid, entity_name, entity_aha, entity_address, campus_aha)) 

view_merged <- merged %>%
  distinct(entity_name, mname, campus_aha, entity_aha, mlocaddr, entity_address)

va_check <- step10 %>%
  filter(sysid == 9295 & !is.na(clean_aha) & clean_aha != 0 & 
           !str_detect(entity_name, "va|veteran")) %>%
  select(entity_name, mname, entity_uniqueid, ahanumber, clean_aha)


## match on system 




what <-  hospitals_final %>% 
  select(campus_aha, entity_aha, entity_name, mname, entity_address, system_id, 
         sysid, entity_parentid, surveyid, year, entity_uniqueid, 
         haentitytypeid) %>% #filter(entity_uniqueid == 11039)
  filter(campus_aha == 6720134 & year == 2005)
  #filter(entity_parentid == 250356 | entity_uniqueid == 250356)

aha_clean <- read.csv("../temp/cleaned_aha.csv")
aha_address <- aha_clean %>% filter(ahanumber == 6720134 & year == 2005) %>%
  distinct(mlocaddr) %>% pull(mlocaddr)
cat(aha_address)

should_match <- cleaned_campus_aha %>% 
  group_by(campus_aha, year) %>%
  mutate(still_unmatched = all(is.na(entity_aha) | entity_aha != campus_aha)) %>%
  ungroup() %>% filter(still_unmatched & !is.na(mname)) %>%
  mutate(jw_dist = stringdist(entity_name, mname, method = "jw", p = 0.1)) %>%
  filter(
    jw_dist <= .2
  )


test_mismatches <- cleaned_campus_aha %>% filter(entity_aha != campus_aha)


  #rowwise() %>%
  #mutate(
    aha_sysid_list = list(
      unique(hospitals_final$aha_sysid[hospitals_final$entity_uniqueid %in% target_entity])
    )
  ) %>%
  ungroup()
