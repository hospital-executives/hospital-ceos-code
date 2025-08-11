## file to generate sample

# libraries
library(rstudioapi)

rm(list = ls())

selected_seed <- 810
sample_dict <- list(
  "CEO:  Chief Executive Officer" = 0,
  "COO:  Chief Operating Officer" = 5,
  "CFO:  Chief Financial Officer" = 5,
  "CIO:  Chief Information Officer" = 5,
  "Chief Medical Officer" = 5,
  "Chief Nursing Head" = 2,
  "Chief Compliance Officer" = 5,
  "Chief Experience/Patient Engagement Officer" = 0,
  "Marketing Head" = 0
)
output_file <- "/himss_check.xlsx"

if (rstudioapi::isAvailable()) {
  script_directory <- dirname(dirname(rstudioapi::getActiveDocumentContext()$path))
} else {
  script_directory <- params$code_dir
}
config_path <- file.path(script_directory, "config.R")
source(config_path)
rm(script_directory, config_path)

source(paste0(code_directory,"/himss_sample_helper.R"))

# load cleaned data
df <- read_feather(paste0(derived_data, '/final_confirmed_aha_final.feather'))

## process titles
check_titles(df, sample_dict)
valid_titles <- names(sample_dict)[sample_dict > 0]
df_indicators <- df %>%
  mutate(value = 1) %>%
  distinct(contact_uniqueid, title_standardized, .keep_all = TRUE) %>%
  pivot_wider(names_from = title_standardized, 
              values_from = value, values_fill = 0) %>%
  rename_with(~ paste0("has_", .), -contact_uniqueid) %>%
  select(contact_uniqueid, all_of(paste0("has_", valid_titles))) %>%
  group_by(contact_uniqueid) %>%
  summarise(across(starts_with("has_"), max))

years <- unique(df$year)


# Apply function
sampled_results <- sample_ids_by_title(df_indicators, sample_dict)
sampled_dataframes <- extract_sampled_data(df, sampled_results)

# Apply to each subset in sampled_dataframes
sampled_dataframes_filled <- lapply(sampled_dataframes, fill_missing_years, years = years)

# add columns to be filled out
new_columns <- c("confirm_position", "confirm_hospital", 
                 "updated_position", "updated_hospital", "source")

sampled_dataframes_filled <- lapply(sampled_dataframes_filled, add_na_columns, column_names = new_columns)

# Clean names in the list
names(sampled_dataframes_filled) <- sapply(names(sampled_dataframes_filled), clean_sheet_name)

write_xlsx(sampled_dataframes_filled, path = paste0(derived_data, output_file))