
library(rstudioapi)
library(xtable)
library(forcats)
library(scales)
library(grid)

rm(list = ls())

args <- commandArgs(trailingOnly = TRUE)

# load data
if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
  setwd(dirname(getActiveDocumentContext()$path))
  source("config.R")
  hospitals <- read_feather(paste0(derived_data, "/hospitals_with_xwalk.feather"))
  final <- read_feather(paste0(derived_data, "/individuals_with_xwalk.feather"))
  supp_path <- supplemental_data
  output_dir <- paste0(data_file_path, "/summary_stats/execs")
} else {
  source("config.R")
  hospitals <- read_feather(args[1])
  final <- read_feather(args[2])
  supp_path <- args[3]
  output_dir <- args[4] 
}

##### SET UP NAME INFRASTRUCTURE ####
source("helper.R")
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

dict1 <- build_cooccurrence_dict(names1)
dict2 <- build_cooccurrence_dict(female_lower)
dict3 <- build_cooccurrence_dict(male_lower)

### load aha
cleaned_aha <- read_csv("temp/cleaned_aha_madmin.csv") 

## check titles 
title_check <- cleaned_aha %>% 
  count(cleaned_title_aha) 

temp_titles <- cleaned_aha %>% 
  filter(str_detect(cleaned_title_aha, "act|int\b|interim")) %>% 
  count(cleaned_title_aha) 

na_titles <- cleaned_aha %>% 
  filter(is.na(cleaned_title_aha)) %>%
  select(first_aha, last_aha, cleaned_title_aha,madmin,  mname, ahanumber, year)

# subset AHA to only (aha, year) observations that are in HIMSS
all_aha_ceos <- hospitals %>% distinct(ahanumber,year) %>% left_join(
  cleaned_aha %>% distinct(full_aha, ahanumber, year, cleaned_title_aha)) %>% 
  filter(year > 2008 & !is.na(full_aha))  %>%
  filter(str_detect(cleaned_title_aha, "ceo|chief executive")) %>% 
  distinct(full_aha, ahanumber, year)

aha_madmin <- hospitals %>% distinct(ahanumber,year) %>% 
  filter(year > 2008) %>%
  left_join(cleaned_aha %>% distinct(full_aha, ahanumber, year, cleaned_title_aha)) %>%
  filter(!is.na(full_aha))

ceo_matches <- read_feather(paste0(auxiliary_data, "/matched_aha_ceos.feather"))
madmin_matches <- read_feather(paste0(auxiliary_data, "/matched_aha_no_ceos.feather"))

## aggregated graph
merged_madmin <- aha_madmin %>% 
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

## plot results
df_counts <- merged_madmin %>% 
  count(match_type) %>%
  mutate(percent = n / nrow(aha_madmin) * 100) 

print(
  xtable(df_counts, caption = "AHA CEO to HIMSS Matches", label = "tab:example"),
  file = paste0(output_dir, "/aha_madmin_all.tex"),
  include.rownames = FALSE
)

p <- ggplot(df_counts %>% filter(percent >= 1 & !is.na(match_type)), aes(x = reorder(match_type, -n), y = n)) +
  geom_col(fill = "steelblue") +
  geom_text(aes(label = paste0(round(percent, 1), "%")), vjust = -0.5) +
  labs(title = "Category Counts with % of Total", x = "Category", y = "Count") +
  scale_y_continuous(expand = expansion(mult = c(0.02, 0.15))) +  # add top space
  coord_cartesian(clip = "off") +                                  # don't clip text
  theme_minimal() +
  theme(plot.margin = margin(t = 12, r = 10, b = 10, l = 10))

ggsave(paste0(output_dir, "/aha_madmin_to_himss.png"), plot = p, width = 6, height = 4, dpi = 300)

#### graph by origin ####


## 1) Long data with clean origin labels
df_long <- merged_madmin %>%
  transmute(
    match_type_ceo    = match_type.x,
    match_type_madmin = match_type.y
  ) %>%
  pivot_longer(everything(), names_to = "origin", values_to = "match_type") %>%
  filter(!is.na(match_type)) %>%
  mutate(
    origin = recode(origin,
                    match_type_ceo    = "CEO matches",
                    match_type_madmin = "Non-CEO matches"
    ),
    origin     = factor(origin, levels = c("CEO matches", "Non-CEO matches")),
    match_type = as.character(match_type)
  )

## 2) Counts per (match_type, origin)
df_counts <- df_long %>%
  count(match_type, origin, name = "n")

## 3) Totals and wide table for labels
df_wide <- df_counts %>%
  tidyr::pivot_wider(
    names_from = origin, values_from = n, values_fill = 0
  ) %>%
  rename(ceo_n = `CEO matches`, non_n = `Non-CEO matches`) %>%
  mutate(total = ceo_n + non_n)

grand_total <- sum(df_wide$total)
df_totals <- df_wide %>%
  mutate(pct_total = total / grand_total)

## Order bars (largest first)
order_levels <- df_wide %>% arrange(desc(total)) %>% pull(match_type)
df_counts <- df_counts %>% mutate(match_type = forcats::fct_relevel(match_type, order_levels))
df_wide   <- df_wide   %>% mutate(match_type = forcats::fct_relevel(match_type, order_levels))
df_totals <- df_totals %>% mutate(match_type = forcats::fct_relevel(match_type, order_levels))

## 4) Spacing for labels (relative to tallest bar)
ymax      <- max(df_wide$total)
pad_below <- 0.03 * ymax     # for Non-CEO label under bar
pad_above1<- 0.03 * ymax     # for CEO label above bar
pad_above2<- 0.1 * ymax     # for total+percent above CEO label

## 5) Colors so text matches bar fills
col_ceo  <- "#1f77b4"
col_non  <- "#ff7f0e"

p <- ggplot(df_counts %>% filter(n >= 100), aes(x = match_type, y = n, fill = origin)) +
  geom_col() +
  scale_fill_manual(values = c("CEO matches" = col_ceo, "Non-CEO matches" = col_non)) +
  
  ## Non-CEO count UNDER the bar (same color as Non-CEO fill)
  geom_text(
    data = df_wide %>% filter(total >= 100),
    aes(x = match_type, y = -pad_below, label = non_n),
    inherit.aes = FALSE,
    color = col_non, size = 3, vjust = 1
  ) +
  
  ## CEO count ABOVE the bar (same color as CEO fill)
  geom_text(
    data = df_wide %>% filter(total >= 100),
    aes(x = match_type, y = total + pad_above1, label = ceo_n),
    inherit.aes = FALSE,
    color = col_ceo, size = 3, vjust = 0
  ) +
  
  ## TOTAL + percent (two lines) above everything, in black
  geom_text(
    data = df_totals  %>% filter(total >= 100),
    aes(x = match_type, y = total + pad_above2,
        label = paste0(total, "\n", scales::percent(pct_total, accuracy = 0.1))),
    inherit.aes = FALSE,
    color = "black", size = 3, vjust = 0, lineheight = 0.95
  ) +
  
  ## Extra headroom (top) and room below 0 for the under-bar labels
  scale_y_continuous(expand = expansion(mult = c(0.12, 0.22))) +
  coord_cartesian(clip = "off") +
  labs(
    x = "match_type", y = "Count", fill = "Origin",
    title = "Match types"
  ) +
  theme_minimal() +
  theme(
    legend.position = "bottom",
    axis.text.x = element_text(angle = 30, hjust = 1)
  ) + 
  labs(
    caption = "Note: We may need to revisit how match types are assigned within the non-CEOs in madmin.\nCurrently, all are title mismatches by default."
  ) +
  theme(
    plot.caption = element_text(hjust = 0, size = 9, face = "italic") # left-aligned, smaller, italic
  )

print(p)

ggsave(paste0(output_dir, "/aha_madmin_to_himss_by_origin.png"), plot = p, width = 6, height = 4, dpi = 300)