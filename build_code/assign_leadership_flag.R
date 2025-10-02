
# load data
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("config.R")
  hospitals <- read_feather(paste0(derived_data, "/hospitals_with_xwalk.feather"))
  individuals <- read_feather(paste0(derived_data, "/individuals_with_xwalk.feather"))
  supp_path <- supplemental_data
  output_dir <- paste0(data_file_path, "/summary_stats/execs")
} else {
  args <- commandArgs(trailingOnly = TRUE)
  source("config.R")
  hospitals <- read_feather(args[1])
  individuals <- read_feather(args[2])
  supp_path <- args[3]
  output_dir <- args[4] 
}

himss_xwalk <- read_feather("temp/himss_to_aha_matches.feather")

jw_matches <- himss_xwalk %>% filter(match_type == "jw") %>% distinct(match_type, id) %>% 
  mutate(aha_leader_flag = TRUE)

jw_year_matches <- himss_xwalk %>% filter(str_detect(match_type, "^jw_[0-9]+$")) %>% distinct(match_type, id) %>% 
  mutate(aha_leader_flag = TRUE)

mini_individuals <- individuals %>% 
  distinct(entity_aha, entity_uniqueid, year, id, firstname, lastname, full_name, title, title_standardized,
           ceo_himss_title_exact, ceo_himss_title_fuzzy, ceo_himss_title_general) %>%
  left_join(himss_xwalk %>% rename(entity_aha = ahanumber) %>% distinct(entity_aha, id, match_type), by = c("id", "entity_aha")) %>%
  mutate(aha_leader_flag = ifelse(is.na(match_type), FALSE, TRUE)) %>%
  group_by(entity_aha, year) %>%
  mutate(himss_has_ceo = any(ceo_himss_title_fuzzy)) %>%
  ungroup()

cat(sum(mini_individuals$aha_leader_flag, na.rm = TRUE))
cat(individuals %>% filter(ceo_himss_title_fuzzy & !is.na(entity_aha)) %>% distinct(id) %>% nrow())

# first assign if title standardized == ceo
pt1  <- mini_individuals %>% 
  filter(aha_leader_flag|ceo_himss_title_fuzzy) %>%
  mutate(std_ceo = title_standardized == "CEO:  Chief Executive Officer") %>%
  group_by(entity_uniqueid, year) %>%
  mutate(multiple_ceo = n_distinct(full_name[aha_leader_flag|ceo_himss_title_fuzzy]) > 1,
         any_std_ceo = any(title_standardized == "CEO:  Chief Executive Officer")) %>% 
  ungroup() %>%
  mutate(
    aha_leader_flag = case_when(
    multiple_ceo & any_std_ceo & std_ceo ~ TRUE,
    multiple_ceo & any_std_ceo & !std_ceo ~ FALSE,
    TRUE ~ aha_leader_flag
    ),
    assigned = ifelse(multiple_ceo & any_std_ceo, TRUE, FALSE)) %>% 
  group_by(entity_uniqueid, year) %>%
  mutate(multiple_ceo = n_distinct(full_name[aha_leader_flag|ceo_himss_title_fuzzy]) > 1,
         any_fuzzy_ceo = any(ceo_himss_title_fuzzy),
         entity_assigned_ceo = any(assigned)) %>% 
  ungroup() %>%
  mutate(
    aha_leader_flag = case_when(
      !entity_assigned_ceo & multiple_ceo & any_fuzzy_ceo & ceo_himss_title_fuzzy ~ TRUE,
      !entity_assigned_ceo & multiple_ceo & any_fuzzy_ceo & !ceo_himss_title_fuzzy ~ FALSE,
      TRUE ~ aha_leader_flag
    ),
    assigned = ifelse( multiple_ceo & any_std_ceo, TRUE, FALSE)) 

pt2 <- pt1 %>%
  group_by(entity_uniqueid, year) %>%
  mutate(multiple_ceos = n_distinct(full_name[aha_leader_flag]) > 1,
         num_coos = n_distinct(full_name[title_standardized == "COO:  Chief Operating Officer"]),
         num_cfos = n_distinct(full_name[title_standardized == "CFO:  Chief Financial Officer"]),
         num_titles = n_distinct(
           full_name[!is.na(title) & title %in% c("President", "CEO", "Administrator")],
           na.rm = TRUE
         )
         ) %>%
  ungroup() %>%
  mutate(
    aha_leader_flag = case_when(
    multiple_ceos & num_coos == 1 & title_standardized == "COO:  Chief Operating Officer" ~ TRUE,
    multiple_ceos & num_coos == 1 & title_standardized != "COO:  Chief Operating Officer" ~ FALSE,
    multiple_ceos & num_cfos == 1 & title_standardized == "CFO:  Chief Financial Officer" ~ TRUE,
    multiple_ceos & num_cfos == 1 & title_standardized != "CFO:  Chief Financial Officer" ~ FALSE,
    multiple_ceos & num_titles == 1 & title %in% c("President", "CEO", "Administrator") ~ TRUE,
    multiple_ceos & num_titles == 1 & !(title %in% c("President", "CEO", "Administrator")) ~ FALSE,
    TRUE ~ aha_leader_flag
  ))

drop_multiples <- pt2 %>% 
  group_by(entity_uniqueid, year) %>%
  mutate(multiple_ceos = n_distinct(full_name[aha_leader_flag]) > 1) %>%
  ungroup() %>%
  mutate(
    aha_leader_flag = ifelse(multiple_ceos, FALSE, aha_leader_flag)
  )

test_df <- rbind(drop_multiples[colnames(mini_individuals)], 
                 mini_individuals %>% filter(!(aha_leader_flag|ceo_himss_title_fuzzy)))

keys <- test_df %>%
  mutate(
    aha_flag = coalesce(aha_leader_flag, FALSE),
    ceo_flag = coalesce(ceo_himss_title_fuzzy, FALSE)
  ) %>%
  filter(!is.na(entity_aha), aha_flag | ceo_flag, !is.na(full_name)) %>%
  group_by(entity_uniqueid, year) %>%
  summarise(
    aha_names = list(sort(unique(full_name[aha_flag]))),
    ceo_names = list(sort(unique(full_name[ceo_flag]))),
    # Count exclusive flags
    aha_only = sum(aha_flag & !ceo_flag),
    ceo_only = sum(ceo_flag & !aha_flag),
    .groups = "drop"
  ) %>%
  # Keep only cases with exactly one person in each exclusive category
  filter(aha_only == 1 & ceo_only == 1)

# Pull all original rows for the disagreeing groups
test_disagreements <- test_df %>% 
  filter(!is.na(entity_aha) & (aha_leader_flag|ceo_himss_title_fuzzy)) %>%
  semi_join(keys, by = c("entity_uniqueid", "year")) %>%
  arrange(entity_uniqueid, year)


# Pull all original rows for the disagreeing groups
test_disagreements <- test_merge %>% 
  filter(!is.na(entity_aha)) %>%
  semi_join(keys, by = c("entity_uniqueid", "year")) %>%
  arrange(entity_uniqueid, year)


## there are 2 potential sources of disagreement that we need to check: 
# the first is that if someone is actually cfo in period t,
# but is ceo in t+k that they arent marked as ceo in period t
# make sure 6540753 is cleaned

check_mismatch <- test_merge %>% filter(!ceo_himss_title_fuzzy & !ceo_himss_title_general & aha_leader_flag)

# the second is what to do when himss and aha_leader_flag mark different people as ceo in the same year
# i think it's fine if we treat himss as the ground truth




## need to check non-ceo cases to make sure they are accurate
## particularly, want to make sure that if someone is actually cfo in period t,
## but is ceo in t+k that they arent marked as ceo in period t
cleaned_aha <- read.csv("temp/cleaned_aha_madmin.csv") 

title_count <- test_merge %>%
  filter(aha_leader_flag & !ceo_himss_title_fuzzy) %>%
  count( title_standardized, sort = TRUE)

## check rare titles
check_rare <- test_merge %>% filter(title_standardized == "PACS Administrator" & aha_leader_flag)
check_himss <- individuals %>% filter(full_name == "chasitytrivette") %>%
  distinct(title, title_standardized, year, entity_name, entity_aha)
any_ceo <- individuals %>% filter(entity_aha == 6520023 & year == 2014)%>%
  distinct(title, title_standardized, full_name, year, entity_name, entity_aha)
check_aha <- cleaned_aha %>% filter(ahanumber == 6520023)

# ids - 4738906, 4738923

check_xwalk <- himss_xwalk %>% filter(ahanumber == 6410027 & year == 2010)

check_mini <- matched_himss %>% filter(ahanumber == 6230041 & year == 2009)