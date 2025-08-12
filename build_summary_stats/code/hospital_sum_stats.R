
#setwd(dirname(rstudioapi::getActiveDocumentContext()$path))


# load dictionaries
config_path <- file.path("../input/config_path.R")
source(config_path)

aha_data <- read_csv("../temp/cleaned_aha.csv")
himss_data <- read_feather("../input/hospitals_final.feather")

#### total merge by year ----
type_1 <- himss_data %>% filter(haentitytypeid == 1)
all_types <- himss_data %>% filter(!is.na(entity_aha) & !is.na(mname))

dfs <- list(type_1 = type_1, all_types = all_types)

for (nm in names(dfs)) {
  df <- dfs[[nm]]

  ha_tagged <- df %>%
    select(year, campus_aha,entity_uniqueid) %>%
    mutate(source_ha = TRUE,
           ahanumber = campus_aha)

  aha_tagged <- aha_data %>%
    mutate(source_aha = TRUE)

  # Join on shared columns (adjust 'by=' if you want specific keys)
  merged_all_original <- full_join(
    ha_tagged,
    aha_tagged,
    by = intersect(names(ha_tagged), names(aha_tagged))
  )

  merge_stats <- merged_all_original %>%
    mutate(merge_status = case_when(
      source_ha  == TRUE & source_aha == TRUE ~ "both",
      source_ha  == TRUE & is.na(source_aha)  ~ "haentity_hosp only",
      is.na(source_ha) & source_aha == TRUE    ~ "aha_data only",
      TRUE ~ NA_character_
    )) %>%
    count(merge_status, name = "n")

  merge_stats_by_year <- merged_all_original %>%
    mutate(merge_status = case_when(
      source_ha  == TRUE & source_aha == TRUE ~ "Both",
      source_ha  == TRUE & is.na(source_aha)  ~ "HIMSS Only",
      is.na(source_ha) & source_aha == TRUE    ~ "AHA Only",
      TRUE ~ NA_character_
    )) %>%
    count(year, merge_status, name = "n")

  p <- ggplot(merge_stats_by_year, aes(x = factor(year), y = n, fill = merge_status)) +
    geom_col() +
    geom_text(aes(label = n),
              position = position_stack(vjust = 0.5),
              color = "white", size = 3.5) +
    labs(title = paste("Merge Status by Year —", nm),
         x = "Year", y = "Number of Hospitals", fill = "Merge Status") +
    theme_minimal()

  ggsave(
    filename = file.path("../output/hospitals/", paste0("merge_status_by_year_", nm, ".png")),
    plot = p, width = 8, height = 5, dpi = 300
  )

  # (Optional) save the counts too
  # write.csv(merge_stats, file.path("../output", paste0("merge_stats_", nm, ".csv")), row.names = FALSE)
  # write.csv(merge_stats_by_year, file.path("../output", paste0("merge_stats_by_year_", nm, ".csv")), row.names = FALSE)
}

#### get number of hospitals/entites by year ----
type_1_and_2 <- himss_data %>% filter(haentitytypeid == 1 | haentitytypeid == 2)
dfs <- list(type_1 = type_1, type_1_and_2 = type_1_and_2)

for (nm in names(dfs)) {
  df <- dfs[[nm]]
  
  ha_counts <- df %>%
    select(year, campus_aha,entity_uniqueid) %>%
    group_by(year) %>%
    summarise(count = n_distinct(entity_uniqueid)) %>%
    mutate(source = "HIMSS")
  
  aha_counts <- aha_tagged %>%
    group_by(year) %>%
    summarise(count = n_distinct(ahanumber)) %>%
    mutate(source = "AHA")
  
  # Combine into one data frame
  combined_counts <- bind_rows(ha_counts, aha_counts)
  
  p <- ggplot(combined_counts, aes(x = year, y = count, color = source)) +
    geom_line() +
    geom_point() +
    labs(
      title = "Unique Hospitals by Year",
      x = "Year",
      y = "Count",
      color = "Source"
    ) +
    theme_minimal()
  
  ggsave(
    filename = file.path("../output/hospitals/", paste0("hospital_counts_by_year_", nm, ".png")),
    plot = p, width = 8, height = 5, dpi = 300
  )
  
  # (Optional) save the counts too
  # write.csv(merge_stats, file.path("../output", paste0("merge_stats_", nm, ".csv")), row.names = FALSE)
  # write.csv(merge_stats_by_year, file.path("../output", paste0("merge_stats_by_year_", nm, ".csv")), row.names = FALSE)
}

#### HIMSS to AHA match all time ----
haentity <- read_feather("../input/haentity.feather")

cat("\nThere are ", n_distinct(haentity$entity_uniqueid), "in HIMSS.",
    "\nOf these,", n_distinct((haentity %>% filter(haentitytypeid == 1))$entity_uniqueid),
    "are hospitals (haentitytypeid == 1).")

q1 <- himss_data %>%
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

himss_entity_plot <- ggplot(q1_graph, aes(x = match_type, y = count, fill = match_type)) +
  geom_col() +
  geom_text(aes(label = label), vjust = -0.5) +
  labs(
    title = "Entity Match Statistics",
    x = "Match Type",
    y = "Number of Matches"
  ) +
  theme_minimal() +
  theme(legend.position = "none") +
  expand_limits(y = max(q1_graph$count) * 1.1)

ggsave("../output/hospitals/entity_match.png", plot = himss_entity_plot, width = 6, height = 4, dpi = 300)

#### HIMSS to AHA match by year ----
# get number of hospital, year pairs in himss
hosp_year_pairs <- himss_data %>%
  filter(haentitytypeid == 1) %>% distinct(entity_uniqueid, year)

cat("\nThere are ", nrow(hosp_year_pairs), "in HIMSS")

fuzzy_stats <- himss_data %>%
  filter(haentitytypeid == 1) %>%
  group_by(entity_uniqueid) %>%
  mutate(
    initial_match = all(entity_fuzzy_flag == 0, na.rm = FALSE),
    fuzzy_match = all(entity_fuzzy_flag %in% c(0, 1)) & 
      !any(is.na(entity_fuzzy_flag))
  ) %>% 
  ungroup() %>%
  filter(!is.na(mname) & !is.na(entity_aha)) %>%
  distinct(entity_uniqueid, year, fuzzy_match, initial_match)

# Step 2: Summarize by year
fuzzy_graph_by_year <- fuzzy_stats %>%
  group_by(year) %>%
  summarise(
    initial_match = sum(initial_match, na.rm = TRUE),
    fuzzy_match = sum(fuzzy_match, na.rm = TRUE),
    total = n(),
    .groups = "drop"
  ) %>%
  pivot_longer(cols = c(initial_match, fuzzy_match), names_to = "match_type", values_to = "count") %>%
  mutate(
    percent = count / total * 100,
    label = paste0(count) # "(", round(percent, 1), "%)")
  )

# Step 3: Plot
entity_by_year <- ggplot(fuzzy_graph_by_year, aes(x = factor(year), y = count, fill = match_type)) +
  geom_col(position = "dodge") +
  geom_text(aes(label = label), position = position_dodge(width = 0.9), vjust = -0.3, size = 3) +
  labs(
    title = "Initial and Fuzzy Matches by Year",
    x = "Year",
    y = "Number of Matches",
    fill = "Match Type"
  ) +
  theme_minimal()

ggsave("../output/hospitals/entity_match_by_year.png", plot = entity_by_year, width = 6, height = 4, dpi = 300)

cat("\nWe have a match for", 
    sum((fuzzy_graph_by_year %>% filter(match_type == "fuzzy_match"))$count)/
      nrow(himss_data %>% filter(haentitytypeid == 1) %>% distinct(entity_uniqueid, year)))

## remaining cases
dont_merge <- himss_data %>% filter(haentitytypeid == 1) %>%
  filter(!is.na(entity_aha) & is.na(mname)) %>% distinct(entity_uniqueid, year)

cat("\nThere are", nrow(dont_merge), "cases that don't merge.")

campus_no_entity <- himss_data %>% filter(haentitytypeid == 1) %>%
  filter(is.na(entity_aha) & !is.na(campus_aha))

cat("\nThere are", nrow(campus_no_entity), "cases that don't merge",
    "that have a campus AHA but not an entity AHA.")

never_assigned <- himss_data %>% filter(haentitytypeid == 1) %>% 
  filter(is.na(campus_aha)) %>%
  select(entity_name, entity_address, entity_state)

cat("\nThere are", nrow(never_assigned), "cases that don't merge",
    "that have are never assigned a campus AHA.")

#### AHA to HIMSS merge ----
cat("There are", n_distinct(aha_data$ahanumber), "US hospitals in the AHA.")

## ever match 
aha_ahanumber <- unique(aha_data$ahanumber)
himss_ahanumber <- unique(himss_data$entity_aha)

overlap <- intersect(aha_ahanumber,himss_ahanumber)
cat("\nThere is ever a match on", length(overlap), "ahanumbers, (",
    length(overlap)/length(aha_ahanumber),"%) out of",
    length(aha_ahanumber), "ahanumbers in the AHA data.")

## entity, year match 
aha_ahanumber <- aha_data %>% distinct(ahanumber, year)
himss_ahanumber <- himss_data %>% distinct(entity_aha, year, entity_name) %>%
  rename(ahanumber = entity_aha)

overlap <- left_join(aha_ahanumber,himss_ahanumber) %>% filter(!is.na(entity_name))

cat("\nThere are", nrow(aha_ahanumber), "hospital-year pairs in the AHA.")
cat("\nThere is a match on ", nrow(overlap)/nrow(aha_ahanumber), 
    "hospital-year pairs,  or", nrow(overlap))

#### check vets cases ----
check_vets <- aha_data %>% filter((sysid != "9295" & sysid!= "9395")| is.na(sysid)) 
all_vets <- aha_data %>% filter(sysid == "9295"|sysid== "9395")

aha_ahanumber <- unique(check_vets$ahanumber)
himss_ahanumber <- unique(himss_data$entity_aha)

overlap <- intersect(aha_ahanumber,himss_ahanumber)
cat("\nThere is ever a match on", length(overlap), "ahanumbers, (",
    length(overlap)/length(aha_ahanumber),"%) out of",
    length(aha_ahanumber), "ahanumbers in the AHA data.")

## entity, year match 
aha_ahanumber <- check_vets %>% distinct(ahanumber, year)
himss_ahanumber <- himss_data %>% distinct(entity_aha, year, entity_name) %>%
  rename(ahanumber = entity_aha)

overlap <- left_join(aha_ahanumber,himss_ahanumber) %>% filter(!is.na(entity_name))

cat("\nThere are", nrow(aha_ahanumber), "hospital-year pairs in the AHA.")
cat("\nThere is a match on ", nrow(overlap)/nrow(aha_ahanumber), 
    "hospital-year pairs,  or", nrow(overlap))

#### types of unmerged AHA hospitals
dta <- read_dta("../input/hospital_ownership.dta")

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

aha_not_in_himss <- aha_data %>%
  anti_join(himss_data %>% select(entity_aha, year) %>%
              rename(ahanumber = entity_aha), by = c("ahanumber", "year")) %>%
  left_join(dta %>% select(year, ahaid_noletter, hospital_type) %>% 
              mutate(ahanumber = as.numeric(ahaid_noletter)), by = c("year", "ahanumber")) %>%
  mutate(hospital_type_label = recode(as.character(hospital_type), !!!type_labels))

check_unlabeled <- aha_not_in_himss %>% filter(is.na(hospital_type))

type_breakdown_unmerged <- aha_not_in_himss %>%
  distinct(mname, hospital_type_label) %>%
  count(hospital_type_label, name = "n") %>%
  mutate(percent = 100 * n / sum(n)) %>%
  arrange(desc(percent))


## compare to all aha

aha_merged <- aha_data %>%
  left_join(dta %>% select(year, ahaid_noletter, hospital_type) %>% 
              mutate(ahanumber = as.numeric(ahaid_noletter)), by = c("year", "ahanumber")) %>%
  mutate(hospital_type_label = recode(as.character(hospital_type), !!!type_labels))

type_breakdown_all_aha <- aha_merged %>%
  distinct(mname, hospital_type_label) %>%
  count(hospital_type_label, name = "n") %>%
  mutate(percent = 100 * n / sum(n)) %>%
  arrange(desc(percent))

## merge
combined_table <- type_breakdown_unmerged %>%
  rename(n_unmerged = n, pct_unmerged = percent) %>%
  inner_join(type_breakdown_all_aha %>%
               rename(n_all = n, pct_all = percent),
             by = "hospital_type_label")

combined_table <- combined_table %>%
  mutate(
    pct_unmerged = sprintf("%.1f%%", pct_unmerged),
    pct_all = sprintf("%.1f%%", pct_all)
  )

kable(combined_table, format = "latex", booktabs = TRUE,
      caption = "Comparison of Type Breakdown: Unmerged vs All AHA") %>%
  cat(file = "../output/hospitals/type_breakdown_comparison.tex")

general_aha <- aha_merged %>% filter(hospital_type == 13)

general_pairs <- general_aha %>% distinct(ahanumber, year)

missing_general <- general_aha %>% anti_join(himss_data %>% select(entity_aha, year) %>%
                                               rename(ahanumber = entity_aha), by = c("ahanumber", "year"))

missing_pairs <- missing_general %>% distinct(ahanumber,year)

cat("\nWe are missing", nrow(missing_pairs)/nrow(general_pairs), "general observations.")