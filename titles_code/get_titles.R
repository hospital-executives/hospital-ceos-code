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

# create mapping from title_standardized to group
suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(openxlsx)
  library(tibble)
})

# -----------------------------
# 0) Helpers: normalize + sanitize
# -----------------------------
normalize_title_std <- function(x) {
  x %>%
    as.character() %>%
    str_replace_all("\u00A0", " ") %>%        # NBSP -> space
    str_replace_all("[[:space:]]+", " ") %>%  # collapse whitespace
    str_squish()
}

sanitize_for_excel <- function(x) {
  x <- as.character(x)
  x <- iconv(x, from = "", to = "UTF-8", sub = "")
  x <- str_replace_all(x, "\u00A0", " ")
  # remove illegal XML control chars (keep \t \n \r)
  x <- str_replace_all(x, "[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]", "")
  x
}

# Excel sheet names max 31 chars; column names max 255 (practically)
safe_sheet_name <- function(x) {
  x <- sanitize_for_excel(x)
  x <- str_replace_all(x, "[:\\\\/?*\\[\\]]", " ") # illegal sheet chars
  x <- str_squish(x)
  substr(x, 1, 31)
}

safe_col_name <- function(x) {
  x <- sanitize_for_excel(x)
  x <- str_replace_all(x, "[\\r\\n\\t]", " ")
  x <- str_squish(x)
  substr(x, 1, 200)
}

# -----------------------------
# 1) Your many-to-many mapping
#    (edit / extend this)
# -----------------------------
title_category_map <- tribble(
  ~title_standardized,                         ~category,
  
  "Director of Technology",                    "IT head",
  "IT Director",                               "IT head",
  "CIO:  Chief Information Officer",            "IT head",
  "CSIO/IT Security Officer",                   "IT head",
  "HIM Director",                              "IT head",
  "Chief Medical Information Officer",          "IT head",
  "Clinical Systems Director",                 "IT head",

  # CNIS in BOTH
  "CNIS:  Chief Nursing Informatics Officer",   "IT head",
  "CNIS:  Chief Nursing Informatics Officer",   "Nursing head",
  
  "Chief Nursing Head",                        "Nursing head",
  
  "COO:  Chief Operating Officer",              "Operations head",
  
  "Quality Head",                              "Quality/compliance head",
  "Chief Compliance Officer",                  "Quality/compliance head",
  "Patient Safety Head",                       "Quality/compliance head",
  
  "Business Office Head",                      "Accounting/office manager",
  "Patient Accounting/Revenue Cycle Head",     "Accounting/office manager",
  
  # If you want CFO to map too:
  "CFO:  Chief Financial Officer",              "Accounting/office manager"
)

# Normalize keys on mapping table
title_category_map2 <- title_category_map %>%
  mutate(title_standardized_key = normalize_title_std(title_standardized)) %>%
  select(title_standardized_key, category) %>%
  distinct()

# -----------------------------
# 2) Attach categories (many-to-many)
# -----------------------------
merged_cat <- merged %>%
  mutate(
    title_standardized_key = normalize_title_std(title_standardized),
    title_standardized     = normalize_title_std(title_standardized),
    title                  = sanitize_for_excel(str_squish(title))
  ) %>%
  left_join(title_category_map2, by = "title_standardized_key")

# OPTIONAL insurance if your C-suite strings vary:
merged_cat <- merged_cat %>%
  mutate(
    category = case_when(
      str_detect(title_standardized, "^COO:") ~ "Operations head",
      str_detect(title_standardized, "^CIO:") ~ "IT head",
      str_detect(title_standardized, "^CFO:") ~ "Accounting/office manager",
      TRUE ~ category
    )
  )

# -----------------------------
# 3) Build "wide audit" sheet for a single category
# -----------------------------
make_wide_audit_sheet <- function(df_cat) {
  counts <- df_cat %>%
    filter(!is.na(title_standardized), !is.na(title), title != "") %>%
    group_by(title_standardized, title) %>%
    summarise(n = n(), .groups = "drop") %>%
    arrange(title_standardized, desc(n), title)
  
  stds <- sort(unique(counts$title_standardized))
  
  cols_list <- lapply(stds, function(std) {
    d <- counts %>% filter(title_standardized == std)
    tibble(
      !!safe_col_name(std) := d$title,
      !!safe_col_name(paste0(std, " — n")) := d$n
    )
  })
  
  max_n <- max(vapply(cols_list, nrow, integer(1)), 0L)
  
  pad_to <- function(tbl, nrows) {
    if (nrow(tbl) >= nrows) return(tbl)
    extra <- as_tibble(setNames(
      replicate(ncol(tbl), rep(NA, nrows - nrow(tbl)), simplify = FALSE),
      names(tbl)
    ))
    bind_rows(tbl, extra)
  }
  
  cols_list_padded <- lapply(cols_list, pad_to, nrows = max_n)
  out <- bind_cols(cols_list_padded)
  
  # optional "distinct titles" line in first data row
  distinct_counts <- counts %>%
    count(title_standardized, name = "n_distinct_titles")
  
  for (std in stds) {
    n_dist <- distinct_counts$n_distinct_titles[distinct_counts$title_standardized == std]
    title_col <- safe_col_name(std)
    count_col <- safe_col_name(paste0(std, " — n"))
    out[[title_col]][1] <- "distinct titles:"
    out[[count_col]][1] <- n_dist
  }
  
  out
}

# -----------------------------
# 4) Export: one sheet per category
# -----------------------------
wb <- createWorkbook()

cats <- merged_cat %>%
  filter(!is.na(category)) %>%
  distinct(category) %>%
  arrange(category) %>%
  pull(category)

for (cat in cats) {
  df_cat <- merged_cat %>% filter(category == cat)
  wide <- make_wide_audit_sheet(df_cat)
  
  sh <- safe_sheet_name(cat)
  addWorksheet(wb, sh)
  
  writeData(wb, sh, wide, startRow = 1, startCol = 1, colNames = TRUE)
  freezePane(wb, sh, firstRow = TRUE)
  setRowHeights(wb, sh, rows = 1, heights = 20)
  setColWidths(wb, sh, cols = 1:ncol(wide), widths = "auto")
}

out_path <- "title_groups.xlsx"
saveWorkbook(wb, out_path, overwrite = TRUE)

out_path


### plot number of titles over time

YEAR_COL <- "year"   # change if needed

counts <- merged_cat %>%
  filter(!is.na(category), !is.na(title_standardized)) %>%
  mutate(
    year = .data[[YEAR_COL]],
    title_standardized = str_squish(title_standardized)
  ) %>%
  filter(!is.na(year)) %>%
  count(category, year, title_standardized, name = "n") %>%
  arrange(category, title_standardized, year)

# Optional but recommended: ensure missing years show as zero
all_years <- sort(unique(counts$year))

counts_full <- counts %>%
  complete(category, title_standardized, year = all_years, fill = list(n = 0L))

plot_category_timeseries <- function(cat, df) {
  
  df_cat <- df %>%
    filter(category == cat, n > 0) %>%   # drop zero-only lines
    mutate(
      title_standardized = droplevels(factor(title_standardized))
    )
  
  ggplot(
    df_cat,
    aes(
      x = year,
      y = n,
      color = title_standardized,
      group = title_standardized
    )
  ) +
    geom_line(linewidth = 1) +
    geom_point(size = 1.5) +
    scale_y_continuous(labels = scales::comma) +
    labs(
      title = paste("Title composition over time —", cat),
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


categories <- counts_full %>%
  distinct(category) %>%
  arrange(category) %>%
  pull(category)

plots <- lapply(categories, plot_category_timeseries, df = counts_full)
names(plots) <- categories

# Display first one (or print any you want)
plots[[1]]

#### get co-occurrences by entity_uniqueid ####
ENTITY_COL <- "entity_uniqueid"   # <-- change if needed
YEAR_COL   <- "year"

presence <- merged_cat %>%
  filter(
    !is.na(category),
    !is.na(title_standardized),
    !is.na(.data[[ENTITY_COL]]),
    !is.na(.data[[YEAR_COL]])
  ) %>%
  mutate(
    entity = .data[[ENTITY_COL]],
    year   = .data[[YEAR_COL]],
    title_standardized = str_squish(title_standardized)
  ) %>%
  distinct(category, entity, year, title_standardized) %>%
  mutate(present = 1L)

library(purrr)

cooccur_pairs <- presence %>%
  group_by(category, entity, year) %>%
  summarise(
    titles = list(sort(unique(title_standardized))),
    .groups = "drop"
  ) %>%
  filter(lengths(titles) >= 2) %>%
  mutate(
    pairs = map(titles, ~ t(combn(.x, 2)))
  ) %>%
  select(category, entity, year, pairs) %>%
  tidyr::unnest(pairs) %>%
  transmute(
    category,
    entity,
    year,
    title_a = pairs[,1],
    title_b = pairs[,2]
  )

pair_counts <- cooccur_pairs %>%
  count(category, year, title_a, title_b, name = "n_entities") %>%
  arrange(category, title_a, title_b, year)

entities_per_year <- presence %>%
  distinct(category, entity, year) %>%
  count(category, year, name = "n_entities_total")

pair_rates <- pair_counts %>%
  left_join(entities_per_year, by = c("category", "year")) %>%
  mutate(
    share_entities = n_entities / n_entities_total
  )

library(ggplot2)

plot_cooccur_heatmap <- function(cat, yr) {
  df <- pair_rates %>%
    filter(category == cat, year == yr)
  
  ggplot(df, aes(x = title_a, y = title_b, fill = share_entities)) +
    geom_tile() +
    scale_fill_viridis_c(labels = scales::percent) +
    labs(
      title = paste("Co-occurrence share —", cat, "(", yr, ")"),
      x = NULL, y = NULL, fill = "Share of entities"
    ) +
    theme_minimal(base_size = 11) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      panel.grid = element_blank()
    )
}

plot_cooccur_heatmap("Quality/compliance head", 2018)


