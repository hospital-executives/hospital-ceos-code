### get distinct non-standardized titles for each standardized title

library(dplyr)
library(haven)
library(rstudioapi)
library(stringr)
library(openxlsx)
library(tibble)
library(ggplot2)
library(scales)

rm(list = ls())
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("../build_code/config.R")
}

# load upstream data
hospital_xwalk <- read_stata(paste0(derived_data, "/temp/merged_ma_sysid_xwalk.dta"))
hospitals <- read_feather(paste0(derived_data, "/hospitals_final.feather"))
individuals <- read_feather(paste0(derived_data, "/individuals_final.feather"))
type <- read_stata(paste0(derived_data, "/temp/merged_ma_nonharmonized.dta")) %>%
  distinct(entity_uniqueid, year, type)

hosp_sample <- hospital_xwalk %>% left_join(type, by = c("entity_uniqueid", "year")) %>%
  filter(is_hospital == 1) %>%
  mutate(
    partofsample = type %in% c("General Medical","General Medical & Surgical","Critical Access")
  ) %>%
  group_by(entity_uniqueid) %>%
  mutate(ever_partofsample = any(partofsample)) %>%
  ungroup() %>%
  filter(ever_partofsample) %>%
  distinct(entity_uniqueid, year)

# get only titles in our sample
merged <- hosp_sample %>% left_join(individuals %>% distinct(entity_uniqueid, year, title, title_standardized)) 

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(tidyr)
  library(ggplot2)
  library(scales)
})

# ----------------------------
# 0) Set your time variable
# ----------------------------
YEAR_COL <- "year"   # <- change if needed
ENTITY_COL <- "entity_uniqueid"

# ----------------------------
# 1) Normalize standardized titles (helps with double spaces etc.)
# ----------------------------
normalize_title_std <- function(x) {
  x %>%
    as.character() %>%
    str_replace_all("\u00A0", " ") %>%        # NBSP -> space
    str_replace_all("[[:space:]]+", " ") %>%  # collapse whitespace
    str_squish()
}

# ----------------------------
# 2) Define tier x group mapping
# ----------------------------
title_tier_group_map <- tibble::tribble(
  ~title_standardized,                         ~tier,    ~group,
  
  # ---- TIER 1: Business ----
  "CEO:  Chief Executive Officer",              "TIER 1", "Business",
  "CFO:  Chief Financial Officer",              "TIER 1", "Business",
  "COO:  Chief Operating Officer",              "TIER 1", "Business",
  
  # ---- TIER 1: Clinical ----
  "Medical Staff Chief",                        "TIER 1", "Clinical",
  "Chief Nursing Head",                         "TIER 1", "Clinical",
  
  # ---- TIER 1: IT/Legal/HR ----
  "CIO:  Chief Information Officer",             "TIER 1", "IT/Legal/HR",
  "Chief Compliance Officer",                   "TIER 1", "IT/Legal/HR",
  "CSIO/IT Security Officer",                   "TIER 1", "IT/Legal/HR",
  "Chief Medical Information Officer",          "TIER 1", "IT/Legal/HR",
  "CNIS:  Chief Nursing Informatics Officer",   "TIER 1", "IT/Legal/HR",
  "Chief Experience/Patient Engagement Officer","TIER 1", "IT/Legal/HR",
  
  # ---- TIER 2: Business ----
  "Business Office Head",                       "TIER 2", "Business",
  "Marketing Head",                             "TIER 2", "Business",
  "Purchasing Head",                            "TIER 2", "Business",
  "Patient Accounting/Revenue Cycle Head",      "TIER 2", "Business",
  
  # ---- TIER 2: Clinical ----
  "Quality Head",                               "TIER 2", "Clinical",
  "OB Head",                                    "TIER 2", "Clinical",
  "Cardiology Head",                            "TIER 2", "Clinical",
  "ER Director",                                "TIER 2", "Clinical",
  "OR Head",                                    "TIER 2", "Clinical",
  "Ambulatory Care Head",                       "TIER 2", "Clinical",
  "Patient Safety Head",                        "TIER 2", "Clinical",
  "Pathology Chief",                            "TIER 2", "Clinical",
  "Laboratory Director",                        "TIER 2", "Clinical",
  "Pharmacy Head",                              "TIER 2", "Clinical",
  "Radiology Med Dir",                          "TIER 2", "Clinical",
  
  # ---- TIER 2: IT/Legal/HR ----
  "IT Director",                                "TIER 2", "IT/Legal/HR",
  "HR Head",                                    "TIER 2", "IT/Legal/HR",
  "HIM Director",                               "TIER 2", "IT/Legal/HR",
  "Facility Management Head",                   "TIER 2", "IT/Legal/HR",
  "Director of Technology",                     "TIER 2", "IT/Legal/HR",
  "Clinical Systems Director",                  "TIER 2", "IT/Legal/HR"
) %>%
  mutate(title_standardized_key = normalize_title_std(title_standardized)) %>%
  select(title_standardized_key, tier, group) %>%
  distinct()

# ----------------------------
# 3) Attach tier/group to your data
# ----------------------------
df_tg <- merged %>%
  mutate(
    title_standardized_key = normalize_title_std(title_standardized),
    title_standardized     = normalize_title_std(title_standardized),
    year                   = .data[[YEAR_COL]]
  ) %>%
  left_join(title_tier_group_map, by = "title_standardized_key")

# Optional insurance if your C-suite spacing varies:
df_tg <- df_tg %>%
  mutate(
    tier = if_else(str_detect(title_standardized, "^CEO:"), "TIER 1", tier),
    group = if_else(str_detect(title_standardized, "^CEO:"), "Business", group),
    tier = if_else(str_detect(title_standardized, "^CFO:"), "TIER 1", tier),
    group = if_else(str_detect(title_standardized, "^CFO:"), "Business", group),
    tier = if_else(str_detect(title_standardized, "^COO:"), "TIER 1", tier),
    group = if_else(str_detect(title_standardized, "^COO:"), "Business", group),
    tier = if_else(str_detect(title_standardized, "^CIO:"), "TIER 1", tier),
    group = if_else(str_detect(title_standardized, "^CIO:"), "IT/Legal/HR", group)
  )

# ----------------------------
# 4) Count over time within tier x group
# ----------------------------
counts <- df_tg %>%
  filter(!is.na(tier), !is.na(group), !is.na(title_standardized), !is.na(year)) %>%
  count(tier, group, year, title_standardized, name = "n") %>%
  arrange(tier, group, title_standardized, year)

entities_per_year <- df_tg %>%
  filter(!is.na(.data[[ENTITY_COL]]), !is.na(.data[[YEAR_COL]])) %>%
  distinct(year = .data[[YEAR_COL]], entity = .data[[ENTITY_COL]]) %>%
  count(year, name = "n_entities_year")

# Optional: fill missing years with 0 so lines don’t break
all_years <- sort(unique(counts$year))
counts_full <- counts %>%
  complete(tier, group, title_standardized, year = all_years, fill = list(n = 0L))

# ----------------------------
# 5) One plot per tier x group, legend only for shown titles
# ----------------------------
plot_tier_group <- function(tier_i, group_i, df) {
  df_sub <- df %>%
    filter(tier == tier_i, group == group_i, n > 0) %>%  # only titles actually present
    mutate(title_standardized = droplevels(factor(title_standardized)))
  
  ggplot(df_sub, aes(x = year, y = n, color = title_standardized, group = title_standardized)) +
    geom_line(linewidth = 1) +
    geom_point(size = 1.6) +
    scale_y_continuous(labels = comma) +
    labs(
      title = paste(tier_i, "—", group_i),
      x = "Year",
      y = "Number of observations",
      color = "Title (standardized)"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      legend.position = "right",
      panel.grid.minor = element_blank()
    )
}

tiers  <- c("TIER 1", "TIER 2")
groups <- c("Business", "Clinical", "IT/Legal/HR")

plots <- list()
for (t in tiers) {
  for (g in groups) {
    key <- paste(t, g, sep = " | ")
    plots[[key]] <- plot_tier_group(t, g, counts_full)
  }
}

# Example: display one
plots[["TIER 1 | Business"]]


# ----------------------------
# 5) Get shares
# ----------------------------
title_entities <- df_tg %>%
  filter(
    !is.na(tier),
    !is.na(group),
    !is.na(title_standardized),
    !is.na(.data[[ENTITY_COL]]),
    !is.na(.data[[YEAR_COL]])
  ) %>%
  mutate(
    year   = .data[[YEAR_COL]],
    entity = .data[[ENTITY_COL]],
    title_standardized = str_squish(title_standardized)
  ) %>%
  distinct(tier, group, year, entity, title_standardized) %>%
  count(tier, group, year, title_standardized,
        name = "n_entities_with_title")

shares <- title_entities %>%
  left_join(entities_per_year, by = "year") %>%
  mutate(
    share_entities = n_entities_with_title / n_entities_year
  ) %>%
  arrange(tier, group, title_standardized, year)


all_years <- sort(unique(entities_per_year$year))

shares_full <- shares %>%
  tidyr::complete(
    tier, group, title_standardized, year = all_years,
    fill = list(n_entities_with_title = 0L, share_entities = 0)
  )

plot_tier_group_share <- function(tier_i, group_i, df) {
  
  df_sub <- df %>%
    filter(
      tier == tier_i,
      group == group_i,
      share_entities > 0
    ) %>%
    mutate(
      title_standardized = droplevels(factor(title_standardized))
    )
  
  ggplot(
    df_sub,
    aes(
      x = year,
      y = share_entities,
      color = title_standardized,
      group = title_standardized
    )
  ) +
    geom_line(linewidth = 1) +
    geom_point(size = 1.6) +
    scale_y_continuous(labels = scales::percent_format(accuracy = 1)) +
    labs(
      title = paste(tier_i, "—", group_i),
      x = "Year",
      y = "Share of entities in that year",
      color = "Title (standardized)"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      legend.position = "right",
      panel.grid.minor = element_blank()
    )
}

tiers  <- c("TIER 1", "TIER 2")
groups <- c("Business", "Clinical", "IT/Legal/HR")

plots <- list()
for (t in tiers) {
  for (g in groups) {
    key <- paste(t, g, sep = " | ")
    plots[[key]] <- plot_tier_group_share(t, g, shares_full)
  }
}

plots[["TIER 1 | Business"]]
