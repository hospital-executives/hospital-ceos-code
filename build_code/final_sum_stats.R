## create final ceo level summary statistics
rm(list = ls())
library(rstudioapi)
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("config.R")
  hospitals <- read_feather(paste0(derived_data, "/hospitals_final.feather"))
  cleaned_individuals <- read_feather(paste0(derived_data, "/individuals_final.feather"))
  supp_path <- supplemental_data
  output_dir <- paste0(data_file_path, "_outputs/execs")
} else {
  args <- commandArgs(trailingOnly = TRUE)
  source("config.R")
  hospitals <- read_feather(args[1])
  individuals <- read_feather(args[2])
  supp_path <- args[3]
  output_dir <- args[4] 
}

summary_file <- paste0(output_dir, "/final_summary.tex")
if (file.exists(summary_file)) {
  file.remove(summary_file)
}

#### load data ####
# load aha data
cleaned_aha <- read_csv("temp/cleaned_aha_madmin.csv") 

# load aha matches 
ceo_matches <- read_feather(paste0(auxiliary_data, "/matched_aha_ceos.feather"))
madmin_matches <- read_feather(paste0(auxiliary_data, "/matched_aha_no_ceos.feather"))
merged_madmin <- cleaned_aha %>% 
  mutate(is_ceo = str_detect(cleaned_title_aha, "ceo|chief executive")) %>%
  left_join(ceo_matches, by = c("full_aha", "year", "ahanumber")) %>%
  left_join(madmin_matches, by = c("full_aha", "year", "ahanumber")) %>%
  mutate(
    match_type = case_when(
      !is.na(match_type.x) & is.na(match_type.y) ~ match_type.x,
      !is.na(match_type.y) & is.na(match_type.x) ~ match_type.y,
      is.na(match_type.y) & is.na(match_type.x) ~ NA,
      TRUE ~ "misc"
    )
  )

# load aha no ceos
all_aha_ceos <- hospitals %>% distinct(entity_aha,year) %>% 
  rename(ahanumber = entity_aha) %>%
  left_join(
    cleaned_aha %>% distinct(full_aha, ahanumber, year, cleaned_title_aha)
  ) %>% 
  filter(year > 2008 & !is.na(full_aha) & str_detect(cleaned_title_aha, "ceo|chief executive officer"))

all_aha_no_ceos <- hospitals %>% distinct(entity_aha,year) %>% 
  rename(ahanumber = entity_aha) %>%
  left_join(
  cleaned_aha %>% distinct(full_aha, ahanumber, year)) %>% 
  filter(year > 2008 & !is.na(full_aha)) %>%
  anti_join(all_aha_ceos, by = c("ahanumber","year","full_aha"))

# load himss matches
himss_matches <- read_feather("temp/himss_to_aha_matches.feather")

# load himss master
himss_master <- read_feather(paste0(derived_data, "/himss_title_master.feather"))

#### get only entity, years for which aha and himss match ####
aha_data <- read.csv("temp/cleaned_aha.csv") %>% distinct(ahanumber, year)
himss_aha <- hospitals %>% 
  filter(year > 2008) %>%
  distinct(entity_aha, year, entity_uniqueid) %>% 
  rename(ahanumber = entity_aha) %>%
  inner_join(aha_data %>% distinct(ahanumber, year), by = c("ahanumber", "year"))

#### hospital-year level summary statistics ####
num_obs <- himss_aha %>% distinct(ahanumber, year) %>% nrow()

## part i
# aha missing ceo
cat("Hospital-year level statistics:\n", file = summary_file, append = TRUE)
cat("There are ", num_obs, 
    "hospital-year observations shared across HIMSS and the AHA.\n", 
    file = summary_file, append = TRUE)
cat("I.\n", file = summary_file, append = TRUE)
cat("AHA reports madmin that are not CEOs in", nrow(all_aha_no_ceos), 
    "hospital-years.\n", file = summary_file, append = TRUE)

# himss missing ceo
active_in_himss <- aha_data %>% 
  left_join(hospitals %>% 
              distinct(entity_aha, year, entity_uniqueid) %>% 
              rename(ahanumber = entity_aha), by = c("year", "ahanumber")) %>%
  left_join(himss_master, by = c("entity_uniqueid", "year")) %>%
  filter(all_ceo == "Active" | flagged_leader_in_aha)

missing <- himss_aha %>% 
  anti_join(active_in_himss %>% distinct(entity_uniqueid, year) , by = c("entity_uniqueid", "year"))

cat("HIMSS reports madmin that are not CEOs in ", nrow(missing), 
    "hospital-years.\n", file = summary_file, append = TRUE)

# number with the same person as CEO
exact_matches <- himss_aha %>% left_join(merged_madmin, by = c("ahanumber", "year")) %>% 
  filter(match_type == "jw")
cat("II.\n", file = summary_file, append = TRUE)
cat("AHA and HIMSS overlap exactly on year, entity, and being a CEO in ", nrow(exact_matches), 
    "cases (", round(nrow(exact_matches) / num_obs, 2), ").\n",
    file = summary_file, append = TRUE)

matches_3_years <- himss_aha %>% left_join(merged_madmin, by = c("ahanumber", "year")) %>% 
  filter(match_type %in% c("jw", "jw_0", "jw_1", "jw_2", "jw_3"))
cat("AHA and HIMSS exactly on entity and being a CEO and are within 3 years of each other in ", nrow(matches_3_years), 
    "cases (", round(nrow(matches_3_years) / num_obs, 2), ").\n",
    file = summary_file, append = TRUE)

matches_title <- himss_aha %>% left_join(merged_madmin, by = c("ahanumber", "year")) %>% 
  filter(match_type %in% c("title", "jw_title", "jw_title_1", "jw_title_3"))
cat("AHA and HIMSS exactly on entity and have a title mismatch (up to 3 years a part) in an additional", nrow(matches_title), 
    "cases (", round(nrow(matches_title) / num_obs, 2), ").\n",
    file = summary_file, append = TRUE)

ever_matched_num <- nrow(himss_aha %>% left_join(merged_madmin, by = c("ahanumber", "year")) %>% filter(!is.na(match_type)))
cat("In all, we are able to match", ever_matched_num,
    "cases in the AHA to HIMSS in the same hospital and/or system (", round(ever_matched_num / num_obs, 2), ").\n",
    file = summary_file, append = TRUE)

# number with the same person | both are non na
non_na_obs <- active_in_himss %>% distinct(ahanumber, year, entity_uniqueid) %>%
  inner_join(all_aha_ceos %>% distinct(ahanumber, year), by = c("ahanumber", "year")) %>% 
  distinct(ahanumber, entity_uniqueid, year)

non_na_matches <- non_na_obs %>%
  left_join(exact_matches) %>% filter(!is.na(match_type))

cat("III.\n", file = summary_file, append = TRUE)
cat(
  "AHA and HIMSS overlap on year, title, and entity in ",
  nrow(non_na_matches), " cases out of ", nrow(non_na_obs),
  " where CEO is non-missing in both datasets (",
  round(nrow(non_na_matches) / nrow(non_na_obs), 2),
  ").\n",
  file = summary_file, append = TRUE
)

non_na_matches_years <- non_na_obs %>%
  left_join(matches_3_years) %>% filter(!is.na(match_type))
non_na_matches_titles <- non_na_obs %>%
  left_join(matches_title) %>% filter(!is.na(match_type))
non_na_matches_all <- non_na_obs %>%
  left_join(merged_madmin) %>% filter(!is.na(match_type))

cat(
  "AHA and HIMSS overlap on entity and title, and are within 3 years of each other, in ",
  nrow(non_na_matches_years), " cases out of ", nrow(non_na_obs),
  " where CEO is non-missing in both datasets (",
  round(nrow(non_na_matches_years) / nrow(non_na_obs), 2),
  ").\n",
  file = summary_file, append = TRUE
)

cat(
  "AHA and HIMSS overlap on entity but not title (or necessarily year) in ",
  nrow(non_na_matches_titles), " cases out of ", nrow(non_na_obs),
  " where CEO is non-missing in both datasets (",
  round(nrow(non_na_matches_titles) / nrow(non_na_obs), 2),
  ").\n",
  file = summary_file, append = TRUE
)

cat(
  "AHA and HIMSS ever overlap in ",
  nrow(non_na_matches_all), " cases out of ", nrow(non_na_obs),
  " where CEO is non-missing in both datasets (",
  round(nrow(non_na_matches_all) / nrow(non_na_obs), 2),
  ").\n",
  file = summary_file, append = TRUE
)

# hospital year overlaps
year_mismatches <- merged_madmin %>% filter(grepl("^jw(_\\d+)?$", match_type))
cat("IV.\n", file = summary_file, append = TRUE)
cat("AHA and HIMSS overlap on entity, but not necessarily year, in ", nrow(year_mismatches), 
    "cases (",round(nrow(year_mismatches) / num_obs, 2), ").\n",
    file = summary_file, append = TRUE
)

#### hospital-level summary statistics #### 
cat("\nHospital-level statistics:\n", file = summary_file, append = TRUE)

aha_nums <- himss_aha %>% distinct(ahanumber) %>% pull(ahanumber)
cat("There are", length(aha_nums), "hospitals.\n", file = summary_file, append = TRUE)

# ever a himss ceo that matches with aha ceo
ever_match_ceo <- ceo_matches %>% filter(ahanumber %in% aha_nums) 
ever_match_madmin <- merged_madmin %>% filter(ahanumber %in% aha_nums) %>% 
  filter(!is.na(match_type))
cat("I.\n", file = summary_file, append = TRUE)
cat(n_distinct(ever_match_ceo$ahanumber), "hospitals ever have a match between their CEO and HIMSS.\n", file = summary_file, append = TRUE)
cat(n_distinct(ever_match_madmin$ahanumber), "hospitals ever have a match between their madmin and HIMSS.\n", file = summary_file, append = TRUE)

# ever a himss ceo that doesnt show up in AHA
id_entity_xwalk <- cleaned_individuals %>% distinct(id, entity_uniqueid)
matches_with_entity <- himss_matches %>% left_join(id_entity_xwalk, by = "id") %>% select(-ahanumber)

never_matched_himss <- himss_aha %>%
  left_join(matches_with_entity, by = c("entity_uniqueid", "year")) %>%
  group_by(ahanumber) %>%
  filter(any(is.na(match_type))) %>%
  ungroup() %>% distinct(ahanumber) %>% pull(ahanumber)
cat("II.\n", file = summary_file, append = TRUE)
cat("There are ", length(never_matched_himss), 
    "hospitals for which there is ever a HIMSS CEO that doesn't show up in the AHA.\n", 
    file = summary_file, append = TRUE)

# ever an AHA ceo that doesnt show up in HIMSS
never_matched_aha <- aha_data %>% inner_join(himss_aha, by = c("ahanumber", "year")) %>%
  left_join(merged_madmin %>% distinct(ahanumber, year, match_type, madmin, full_name),
            by = c("ahanumber", "year")) %>%
  group_by(ahanumber) %>%
  filter(any(is.na(match_type))) %>%
  ungroup() %>% distinct(ahanumber) %>% pull(ahanumber)

cat("III.\n", file = summary_file, append = TRUE)
cat("There are ", length(never_matched_aha), 
    "hospitals for which there is ever an AHA CEO that doesn't show up in HIMSS.\n", 
    file = summary_file, append = TRUE)

# ever a year himss says ceo but aha doesnt
himss_ceos <- cleaned_individuals %>% distinct(entity_aha, year, ceo_himss_title_fuzzy) %>%
  filter(ceo_himss_title_fuzzy) %>%
  rename(ahanumber = entity_aha) %>%
  anti_join(all_aha_ceos %>% select(ahanumber, year), by = c("ahanumber", "year")) %>%
  distinct(ahanumber)

cat("IV.\n", file = summary_file, append = TRUE)
cat("There are", nrow(himss_ceos), "hospitals for which HIMSS ever says there is a CEO but AHA does not.\n", file = summary_file, append = TRUE)

# ever a year where aha says ceo but himss doesnt
aha_ceos <- all_aha_ceos %>% distinct(ahanumber, year) %>%
  anti_join(cleaned_individuals %>% 
              distinct(entity_aha, year, ceo_himss_title_fuzzy) %>%
              rename(ahanumber = entity_aha) %>%
              filter(ceo_himss_title_fuzzy), by = c("ahanumber", "year")) %>%
  distinct(ahanumber)

cat("V.\n", file = summary_file, append = TRUE)
cat("There are", nrow(aha_ceos), "hospitals for which AHA ever says there is a CEO but HIMSS does not.", file = summary_file, append = TRUE)

