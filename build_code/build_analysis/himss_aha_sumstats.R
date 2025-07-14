#### HIMSS TO AHA SUMMARY STATISTICS ####
# this file creates the summary statistics to answer the following questions:
# 1. how many hospitals from HIMSS don't mtch the AHA data initially?
# 2. how many don't match after implementing the fuzzy match?
# 3. how many hospitals in AHA don't match HIMSS?


##### SET UP: LOAD DATA #####
library(haven)
library(dplyr)
library(ggplot2)
library(arrow)

full_xwalk <-  read_feather(paste0(derived_data,
                                   '/final_confirmed_aha_final.feather'))
himss <- read_feather(paste0(derived_data, '/final_himss.feather'))

#### load AHA data #####

# set file path using manual input at beginning of script
file_path <- paste0(supplemental_data,"/",file_name_aha)
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



##### Questions 1 & 2#####
q1 <- full_xwalk %>%
  filter(haentitytypeid  == 1 ) %>%
  group_by(entity_uniqueid) %>%
  mutate(
    initial_match = all(entity_fuzzy_flag == 0, na.rm = FALSE),
    fuzzy_match = all(entity_fuzzy_flag %in% c(0, 1)) & 
      !any(is.na(entity_fuzzy_flag))
  ) %>% ungroup()

q1_graph <- q1 %>%
  group_by(entity_uniqueid) %>%
  summarise(
    initial_match = any(initial_match, na.rm = TRUE),
    fuzzy_match = any(fuzzy_match, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  summarise(
    initial_match = sum(initial_match),
    fuzzy_match = sum(fuzzy_match)
  ) %>%
  tidyr::pivot_longer(cols = everything(), names_to = "match_type", values_to = "count")

total_entities <- q1 %>% 
  distinct(entity_uniqueid) %>% 
  nrow()

q1_graph <- q1_graph %>%
  mutate(
    percent = count / total_entities * 100,
    label = paste0(count, " (", round(percent, 1), "%)")
  )

ggplot(q1_graph, aes(x = match_type, y = count, fill = match_type)) +
  geom_col() +
  geom_text(aes(label = label), vjust = -0.5) +
  labs(
    title = "Entity Match Summary",
    x = "Match Type",
    y = "Number of Matches"
  ) +
  theme_minimal() +
  theme(legend.position = "none") +
  expand_limits(y = max(q1_graph$count) * 1.1)

## answering the question:
cat("There are", n_distinct(q1$entity_uniqueid), "unique entities.\n",
    "This means that in the initial match,", 
    n_distinct(q1$entity_uniqueid) - 4626, "don't match.\n",
    "In the fuzzy match",
    n_distinct(q1$entity_uniqueid) - 5060, "don't match.\n")




##### Question 3 #####
xwalk_vars <- full_xwalk %>% filter(haentitytypeid == 1) %>%
  filter(!(as.numeric(latitude_aha) <=20 & as.numeric(longitude_aha >=-70) )) %>%
  distinct(clean_aha, year) %>%
  rename(ahanumber = clean_aha)
  
q3 <- anti_join(aha_data %>% distinct(ahanumber, year), 
                xwalk_vars, by = c("ahanumber", "year"))

aha_filtered <- aha_data %>%
  semi_join(q3, by = c("ahanumber", "year")) %>%
  filter(!(as.numeric(latitude_aha) <= 20 & as.numeric(longitude_aha) >= -70)) %>%
  filter(as.numeric(longitude_aha) >= -140)


cat("There are", n_distinct(aha_filtered$ahanumber), 
    "hospitals in the AHA that do not appear in HIMSS.")

dta <- read_dta(paste0(data_file_path, "/supplemental/hospital_ownership.dta"))

type_labels <- c(
  "0" = "ALTC",
  "1" = "Cancer",
  "2" = "Childrens ALTC",
  "3" = "Childrens chronic",
  "4" = "Childrens general",
  "5" = "Childrens orthopedic",
  "6" = "Childrens other",
  "7" = "Childrens other specialty",
  "8" = "Childrens psychiatric",
  "9" = "Childrens rehabilitation",
  "10" = "Chronic",
  "11" = "Dependency rehab",
  "12" = "Facial",
  "13" = "General",
  "14" = "Heart",
  "15" = "Intellectual disabilities",
  "16" = "Obgyn",
  "17" = "Orthopedic",
  "18" = "Other",
  "19" = "Other speciality",
  "20" = "Psychiatric",
  "21" = "Rehabilitation",
  "22" = "Respiratory",
  "23" = "Surgical"
)

mini_dta <- dta %>%
  select(ahaid_noletter, year, hospital_type) %>%
  mutate(
    hospital_type_label = recode(as.character(hospital_type), !!!type_labels),
    ahanumber = as.numeric(ahaid_noletter)
  )

unmerged_aha <- left_join(aha_filtered, mini_dta)

hospitals_all_na <- unmerged_aha %>%
  group_by(ahanumber) %>%
  summarise(all_na = all(is.na(hospital_type_label)), .groups = "drop") %>%
  filter(all_na) %>%
  select(ahanumber)

pie_data <- unmerged_aha %>%
  group_by(ahanumber) %>%
  summarise(
    hospital_type_label = first(na.omit(hospital_type_label)),
    .groups = "drop"
  ) %>%
  mutate(hospital_type_label = ifelse(
    ahanumber %in% hospitals_all_na$ahanumber,
    "Missing",
    hospital_type_label
  )) %>%
  count(hospital_type_label)

ggplot(pie_data, aes(x = "", y = n, fill = hospital_type_label)) +
  geom_col(width = 1) +
  coord_polar(theta = "y") +
  labs(
    title = "Hospital Type (First Non-NA or Missing)",
    fill = "Hospital Type"
  ) +
  theme_void()

## check location
state_data <- aha_filtered %>%
  mutate(
    entity_state = str_trim(str_extract(hrrname, "(?<=,).*"))
  ) %>% select(ahanumber, entity_state) %>%
  distinct(ahanumber, .keep_all = TRUE)

state_counts <- state_data %>%
  count(entity_state)

ggplot(state_counts, aes(x = "", y = n, fill = entity_state)) +
  geom_col(width = 1) +
  coord_polar(theta = "y") +
  labs(
    title = "Distribution of States (One per Entity)",
    fill = "State"
  ) +
  theme_void()

