
## libraries
library(rstudioapi)

rm(list = ls())

## set up scripts
if (rstudioapi::isAvailable()) {
  script_directory <- dirname(dirname(rstudioapi::getActiveDocumentContext()$path))
} else {
  script_directory <- params$code_dir
}
config_path <- file.path(script_directory, "config.R")
source(config_path)
rm(script_directory, config_path)

final_path <- paste0(derived_data, "/final_aha.feather")
final <- read_feather(final_path)
hospitals <- read_feather(paste0(derived_data,'/himss_aha_hospitals_final.feather'))

##### SET UP NAME INFRASTRUCTURE ####
names1 <- read.csv(paste0(supplemental_data, "/carltonnorthernnames.csv"), header = FALSE)
female <- read.csv(paste0(supplemental_data, "/female_diminutives.csv"), header = FALSE)
female_lower <- female
female_lower[] <- lapply(female_lower, function(x) {
  if (is.character(x)) tolower(x) else x
})
male <- read.csv(paste0(supplemental_data, "/male_diminutives.csv"), header = FALSE)
male_lower <- male
male_lower[] <- lapply(male_lower, function(x) {
  if (is.character(x)) tolower(x) else x
})

build_cooccurrence_dict <- function(df_all) {
  df_all_lower <- df_all %>% mutate(across(everything(), ~ tolower(as.character(.))))
  
  # Get all rows as sets of names
  rows_as_sets <- apply(df_all_lower, 1, unique)
  
  # Flatten into (name -> set of co-names)
  name_to_neighbors <- list()
  
  for (row in rows_as_sets) {
    for (name in row) {
      others <- setdiff(row, name)
      if (is.null(name_to_neighbors[[name]])) {
        name_to_neighbors[[name]] <- others
      } else {
        name_to_neighbors[[name]] <- union(name_to_neighbors[[name]], others)
      }
    }
  }
  
  name_to_neighbors
}

names_in_same_row_dict <- function(name1, name2, cooccur_dict) {
  name1 <- tolower(name1)
  name2 <- tolower(name2)
  !is.null(cooccur_dict[[name1]]) && name2 %in% cooccur_dict[[name1]]
}

dict1 <- build_cooccurrence_dict(names1)
dict2 <- build_cooccurrence_dict(female_lower)
dict3 <- build_cooccurrence_dict(male_lower)

last_name_overlap <- function(last_aha, last_himss) {
  last_aha <- tolower(last_aha)
  last_himss <- tolower(last_himss)
  
  grepl(last_aha, last_himss, fixed = TRUE) || grepl(last_himss, last_aha, fixed = TRUE)
}

#### load AHA data #####
columns_to_keep <- c( 
  "ID", 
  "YEAR", 
  "MNAME", 
  "HOSPN", 
  "MCRNUM",
  # "CBSANAME", 
  "HRRNAME", 
  "HSANAME", 
  "MADMIN", 
  "SYSID", 
  "SYSNAME", 
  "CNTRL", 
  "SERV", 
  "HOSPBD", 
  "BDTOT", 
  "ADMTOT", 
  "IPDTOT", 
  "MCRDC", 
  "MCRIPD", 
  "MCDDC", 
  "MCDIPD", 
  "BIRTHS", 
  "FTEMD", 
  "FTERN", 
  "FTE",
  "LAT",
  "LONG",
  "MLOCADDR", #street address
  "MLOCCITY", #city
  "MLOCZIP" #zip code
)

# set file path using manual input at beginning of script
file_path <- paste0(supplemental_data,"/AHA_2004_2017.csv")
#import the CSV
aha_data <- read_csv(file_path, col_types = cols(.default = col_character()))

# Use problems() function to identify parsing issues
parsing_issues <- problems(aha_data)

# Check if any parsing issues were detected
if (nrow(parsing_issues) > 0) {
  print(parsing_issues)
} else {
  print("No parsing problems detected.")
}

aha_data <- aha_data %>%
  select(all_of(columns_to_keep)) %>% 
  rename_all(tolower) %>% 
  rename(ahanumber = id,
         latitude_aha = lat,
         longitude_aha = long) %>% 
  mutate(
    # Convert to UTF-8
    mloczip = stri_trans_general(mloczip, "latin-ascii"),
    # cbsaname = stri_trans_general(cbsaname, "latin-ascii"),
    mlocaddr = stri_trans_general(mlocaddr, "latin-ascii") %>% 
      str_replace_all("-", " ") %>%  # Replace hyphens with a space
      str_remove_all("[^[:alnum:],.\\s]") %>%  # Remove other punctuation except commas, periods, and spaces
      str_to_lower() %>%
      str_squish(),  # Remove extra spaces
    mname = stri_trans_general(mname, "latin-ascii") %>% 
      str_replace_all("-", " ") %>%  # Replace hyphens with a space
      str_remove_all("[^[:alnum:],.\\s]") %>%  # Remove other punctuation except commas, periods, and spaces
      str_to_lower() %>%
      str_squish(),  # Remove extra spaces
    # Extract first five digits of the zip code
    mloczip_five = str_extract(mloczip, "^\\d{5}")
  )

aha_data <- aha_data %>%
  mutate(ahanumber = str_remove_all(ahanumber, "[A-Za-z]"),
         ahanumber = as.numeric(ahanumber),
         year = as.numeric(year),
         mcrnum = str_remove_all(mcrnum, "[A-Za-z]"),
         mcrnum = as.numeric(mcrnum))


#### clean aha and filter to only chief executives ####

# clean aha to extract names and titles from madmin
suffixes <- c("i", "ii", "iii", "iv", "v", "jr", "sr", "j.r.", "s.r.", "md", "rn")
cleaned_aha <- aha_data %>%
  separate(madmin, into = c("aha_name", "title_aha"), 
           sep = ",", extra = "merge", fill = "right") %>%
  mutate(cleaned_title_aha = title_aha %>%
           str_to_lower() %>%
           str_replace_all("[[:punct:]]", "")) %>%
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

all_himss_ceos <- aha_data %>% distinct(ahanumber,year) %>% 
  left_join(
    final %>% filter(haentitytypeid == 1) %>%
      distinct(contact_uniqueid ,full_name, firstname, lastname,
               ahanumber, entity_aha, entity_uniqueid, entity_name, year, 
               title_standardized, title, madmin, haentitytypeid) %>%
      mutate(ahanumber = entity_aha)) %>% 
  filter(year > 2008 & !is.na(full_name) & !is.na(ahanumber))  %>%
  filter(str_detect(title, "ceo|chief executive") |
           str_detect(title_standardized , "CEO"))

num_total_combos =nrow(all_himss_ceos %>% 
                         distinct(entity_uniqueid, year, contact_uniqueid))

cat("There are ", num_total_combos,
    "(hospital, year, CEO) observations in HIMSS for which",
    "the ahanumber and year also appear in the AHA data.")

## first step - get HIMSS ceos that we already matched by ------------------
## name similarity in the aha merge
aha_combos <- read_feather(paste0(auxiliary_data, "/matched_aha_ceos.feather"))

already_matched <- all_himss_ceos %>% left_join(
  aha_combos %>% filter(match_type == "jw") %>% select(ahanumber, year, full_aha)
) %>% filter(!is.na(full_aha))

matched_pairs <- already_matched %>% distinct(ahanumber, contact_uniqueid, year)

cat("Of these,", 
    nrow(matched_pairs)/num_total_combos,
    "are already matched by jw similarity and year in the AHA code.")