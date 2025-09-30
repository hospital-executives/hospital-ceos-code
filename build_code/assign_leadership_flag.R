
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
           ceo_himss_title_fuzzy, ceo_himss_title_general) %>%
  left_join(himss_xwalk %>% rename(entity_aha = ahanumber) %>% distinct(entity_aha, id, match_type), by = c("id", "entity_aha")) %>%
  mutate(aha_leader_flag = ifelse(is.na(match_type), FALSE, TRUE))

cat(sum(mini_individuals$aha_leader_flag, na.rm = TRUE))
cat(individuals %>% filter(ceo_himss_title_fuzzy & !is.na(entity_aha)) %>% distinct(id) %>% nrow())

test_merge <- mini_individuals %>% 
  filter(aha_leader_flag) %>%
  mutate(std_ceo = title_standardized == "CEO:  Chief Executive Officer") %>%
  group_by(entity_uniqueid, year) %>%
  mutate(multiple_ceo = sum(aha_leader_flag) > 1,
         any_std_ceo = any(std_ceo)) %>% 
  ungroup() %>%
  # first assign if title standardized == ceo
  mutate(
    aha_leader_flag = case_when(
  multiple_ceo & any_std_ceo & std_ceo ~ TRUE,
  multiple_ceo & any_std_ceo & !std_ceo ~ FALSE,
  TRUE ~ aha_leader_flag
    )) %>%
  group_by(entity_uniqueid, year) %>%
  mutate(multiple_ceo = sum(aha_leader_flag) > 1,
         any_ceo = any(ceo_himss_title_fuzzy)) %>%
  ungroup() %>%
  # then assign if freeform title has ceo
  mutate(
    aha_leader_flag = case_when(
      multiple_ceo & any_ceo & ceo_himss_title_fuzzy ~ TRUE,
      multiple_ceo & any_ceo & !ceo_himss_title_fuzzy ~ FALSE,
      TRUE ~ aha_leader_flag
    )) %>%
  group_by(entity_uniqueid, year) %>%
  mutate(multiple_ceo = sum(aha_leader_flag) > 1,
         num_names = n_distinct(full_name[aha_leader_flag])) %>%
  ungroup() %>%
  mutate(
    aha_leader_flag = case_when(
      multiple_ceo & num_names == 1 ~ TRUE,
      TRUE ~ aha_leader_flag
    )) %>%
  group_by(entity_uniqueid, year) %>%
  # don't flag cases where there is only one person who is assigned aha_leader
  mutate(
    multiple_ceo = n_distinct(full_name[aha_leader_flag]) > 1,
    one_coo =  n_distinct(full_name[aha_leader_flag & 
                                      title_standardized == "COO:  Chief Operating Officer"] == 1),
    one_cfo =  n_distinct(full_name[aha_leader_flag & 
                                      title_standardized == "CFO:  Chief Financial Officer"] == 1),
    one_admin =  n_distinct(full_name[aha_leader_flag & 
                                      str_detect(title, "Administrator")] == 1)
    ) %>% 
    ungroup() %>%
    mutate(
      aha_leader_flag = case_when(
        multiple_ceo & one_coo == 1 ~ TRUE,
        multiple_ceo & one_coo == 0 & one_cfo == 1 ~ TRUE,
        multiple_ceo & one_coo == 0 & one_cfo == 0 & one_admin == 1~ TRUE,
        TRUE ~ aha_leader_flag
      )) %>%
    group_by(entity_uniqueid, year) %>%
    mutate(
      single_second = (one_coo == 1) | (one_coo == 0 & one_cfo == 1) |
        (one_coo == 0 & one_cfo == 0 & one_admin == 1),
      multiple_ceo = n_distinct(full_name[aha_leader_flag]) > 1 & !single_second
    ) %>%
  ungroup() %>%
  arrange(entity_uniqueid, year)

## need to check non-ceo cases to make sure they are accurate

# ids - 4738906, 4738923

check_xwalk <- himss_xwalk %>% filter(ahanumber == 6410027 & year == 2010)

check_mini <- matched_himss %>% filter(ahanumber == 6230041 & year == 2009)