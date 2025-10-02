
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
           ceo_himss_title_exact, ceo_himss_title_fuzzy, ceo_himss_title_general, contact_uniqueid, confirmed) %>%
  left_join(himss_xwalk %>% rename(entity_aha = ahanumber) %>% distinct(entity_aha, id, match_type), by = c("id", "entity_aha")) %>%
  mutate(aha_leader_flag = ifelse(is.na(match_type), FALSE, TRUE)) %>%
  group_by(entity_aha, year) %>%
  mutate(himss_has_ceo = any(ceo_himss_title_fuzzy)) %>%
  ungroup()

# cat(sum(mini_individuals$aha_leader_flag, na.rm = TRUE))
# cat(individuals %>% filter(ceo_himss_title_fuzzy & !is.na(entity_aha)) %>% distinct(id) %>% nrow())

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

# 1: assign aha_leader_flag = false in the rare cases where there are multiple aha_leader_flag per (entity, year)
# 2: assign aha_leader_flag = false in the rare cases where match_type is not on title/jw or off by one year (<1%)
drop_multiples <- pt2 %>% 
  group_by(entity_uniqueid, year) %>%
  mutate(multiple_ceos = n_distinct(full_name[aha_leader_flag]) > 1) %>%
  ungroup() %>%
  mutate(
    aha_leader_flag = ifelse(multiple_ceos, FALSE, aha_leader_flag),
    aha_clean_match = (match_type %in% c("title", "jw", "title_1","jw_1")),
    aha_leader_flag = ifelse(aha_clean_match, aha_leader_flag, FALSE)
  )

# combine cases with matches to AHA and those without and create umbrella flag
combined_df <- rbind(
  drop_multiples[colnames(mini_individuals)], 
  mini_individuals %>% filter(!(aha_leader_flag|ceo_himss_title_fuzzy))) %>%
  mutate(
    std_ceo = title_standardized == "CEO:  Chief Executive Officer", 
    all_leader_flag = aha_leader_flag | std_ceo | ceo_himss_title_fuzzy
  ) 

# make sure umbrella flag is only assigned once per entity_uniqueid, year
final_flag_df <- combined_df %>% 
  group_by(entity_uniqueid, year) %>%
  mutate(multiple_ceos = n_distinct(full_name[all_leader_flag]) > 1,
         distinct_std_ceos = n_distinct(full_name[std_ceo])) %>%
  ungroup() %>%
  mutate(
    all_leader_flag = case_when(
      distinct_std_ceos == 1 & std_ceo ~ TRUE,
      distinct_std_ceos == 1 & !std_ceo ~ FALSE,
      TRUE ~ FALSE
    )
  ) %>%
  distinct(id, aha_leader_flag, all_leader_flag)

# combine with import df for clean export
export_df <- individuals %>% left_join(final_flag_df) %>%
  mutate(all_leader_flag = ifelse(is.na(all_leader_flag), FALSE, all_leader_flag),
         aha_leader_flag = ifelse(is.na(aha_leader_flag), FALSE, aha_leader_flag))

write_feather(export_df, paste0(derived_data, "/individuals_final.feather"))