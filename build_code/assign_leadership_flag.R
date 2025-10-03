library(tibble)
library(rstudioapi)
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
    all_leader_flag = aha_leader_flag | std_ceo | ceo_himss_title_fuzzy | ceo_himss_title_exact
  ) 

# make sure umbrella flag is only assigned once per entity_uniqueid, year
final_flag_df <- combined_df %>% 
  group_by(entity_uniqueid, year) %>%
  mutate(multiple_ceos = n_distinct(full_name[all_leader_flag]) > 1,
         distinct_std_ceos = n_distinct(full_name[std_ceo])) %>%
  ungroup() %>%
  mutate(
    all_leader_flag = case_when(
      multiple_ceos & distinct_std_ceos == 1 & std_ceo ~ TRUE,
      multiple_ceos & distinct_std_ceos == 1 & !std_ceo ~ FALSE,
      multiple_ceos ~ FALSE, 
      TRUE ~ all_leader_flag
    )
  ) %>%
  distinct(id, aha_leader_flag, all_leader_flag)

# combine with import df for clean export
export_df <- individuals %>% left_join(final_flag_df, by = "id") %>%
  mutate(all_leader_flag = ifelse(is.na(all_leader_flag), FALSE, all_leader_flag),
         aha_leader_flag = ifelse(is.na(aha_leader_flag), FALSE, aha_leader_flag),
         firstname = ifelse(nan_flag, "nan", firstname)) %>%
  select(-nan_flag)

write_feather(export_df, paste0(derived_data, "/individuals_final.feather"))


### create leader flag summary statistics 
summary_file <- paste0(output_dir, "/leader_flags_summary.tex")
if (file.exists(summary_file)) {
  file.remove(summary_file)
}

valid_obs <- export_df %>%
  mutate(std_ceo = title_standardized == "CEO:  Chief Executive Officer",
         himss_ceo = std_ceo|ceo_himss_title_fuzzy|ceo_himss_title_exact) %>%
  filter(!is.na(entity_aha))
denom <- nrow(valid_obs)
vars   <- c("std_ceo", "himss_ceo", "aha_leader_flag", "all_leader_flag") 

summ <- valid_obs %>%
  summarise(across(all_of(vars), ~ sum(.x, na.rm = TRUE))) %>%
  pivot_longer(everything(), names_to = "variable", values_to = "count_true") %>%
  mutate(
    denominator = denom,
    pct         = 100 * count_true / denominator,
    pct_str     = sprintf("%.1f\\%%", pct)   # escape % for LaTeX
  )

## export sum stats
label_map <- c(
  std_ceo = "HIMSS \texttt{title\\_standardized} is CEO: ",
  himss_ceo = "HIMSS \texttt{title} contains CEO or similar: ",
  aha_leader_flag  = "HIMSS obs corresponds to AHA \texttt{madmin}: ",
  all_leader_flag = "Any of the previous conditions hold: "
)

# Replace variable names with descriptive labels
summ <- summ %>%
  mutate(label = label_map[variable])

# Now build LaTeX
tex_out <- paste0(
  "\\begin{tabular}{lr}\n\\toprule\n",
  "Condition & Count True \\\\\n\\midrule\n",
  paste(sprintf("%s & %d \\\\",
                summ$label, summ$count_true),
        collapse = "\n"),
  "\n\\bottomrule\n\\end{tabular}\n"
)

writeLines(tex_out, summary_file)

## get number of people who first appear in himss as std_ceo but are ever a leader before
prev_leader <- export_df %>%
  filter(confirmed) %>%
  distinct(title, title_standardized, all_leader_flag, entity_name, year, full_name, contact_uniqueid) %>%
  mutate(std_ceo = title_standardized == "CEO:  Chief Executive Officer") %>%
  group_by(contact_uniqueid) %>%
  mutate(first_std_ceo_year = if (any(std_ceo)) min(year[std_ceo], na.rm = TRUE) else NA_integer_,
         first_leader_year = if (any(all_leader_flag)) min(year[all_leader_flag], na.rm = TRUE) else NA_integer_) %>%
  filter(first_leader_year < first_std_ceo_year) %>%
  ungroup() %>%
  arrange(contact_uniqueid)

cat("It's rare for an individual with title_standardized == CEO to be a leader in a previous year.\n", file = summary_file, append = TRUE)
cat("Only", n_distinct(prev_leader$contact_uniqueid), "confirmed individuals have this occur.", file = summary_file, append = TRUE)

