
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))
rm(list = ls())
# load dictionaries
library(xtable)
config_path <- file.path("../input/config_path.R")
source(config_path)

final <- read_feather("../input/individuals_final.feather")
hospitals <- read_feather("../input/hospitals_final.feather")

### AHA CEO to HIMSS left join

## libraries
library(rstudioapi)

## set up scripts
config_path <- file.path("../input/config_path.R")
source(config_path)

final_path <- paste0(derived_data, "/final_aha.feather")
final <- read_feather(final_path)
hospitals <- read_feather(paste0(derived_data,'/himss_aha_hospitals_final.feather'))

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
cleaned_aha <- read_csv("../temp/cleaned_aha_madmin.csv") 

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

ceo_matches <- read_feather("../temp/matched_aha_ceos.feather")
madmin_matches <- read_feather("../temp/matched_aha_leadership.feather")

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

check <- merged_madmin %>% filter(match_type == "misc")

## plot results
df_counts <- merged_madmin %>% 
  count(match_type) %>%
  mutate(percent = n / nrow(aha_madmin) * 100) 

print(
  xtable(df_counts, caption = "AHA CEO to HIMSS Matches", label = "tab:example"),
  file = "../output/execs/aha_madmin_all.tex",
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

ggsave("../output/execs/aha_madmin_to_himss.png", plot = p, width = 6, height = 4, dpi = 300)

## extract titles
title_df <- final %>% distinct(id, title, title_standardized)

df_titles <- merged_madmin %>% 
  mutate(id = coalesce(id.x, id.y)) %>%
  left_join(title_df) %>%
  count(title, sort = TRUE) %>%
  mutate(percent = 100 * n / sum(n)) %>%
  filter(n > 100)

max_count <- max(df_titles$n)

freeform_title_plot <- df_titles %>%
  ggplot(aes(x = reorder(title, -n), y = n)) +
  geom_col(fill = "steelblue") +
  geom_text(aes(label = paste0(round(percent, 1), "%")), 
            vjust = -0.5, size = 3) +
  labs(
    x = "Title",
    y = "Count",
    title = "Counts by Title (>1%)"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  expand_limits(y = max_count * 1.1)






## view mismatched
mismatched <- merged_madmin %>%
  filter(is.na(match_type)) %>%
  distinct(ahanumber, year, is_ceo)

num <- merged_madmin %>%
  filter(is.na(match_type)) %>%
  distinct(ahanumber, year, is_ceo) %>%
  count(year, is_ceo, name = "num")

# Denominator: all distinct hospital-years
denom <- merged_madmin %>%
  distinct(ahanumber, year, is_ceo) %>%
  count(year, is_ceo, name = "denom")

# Join and compute percent
mismatched_years <- denom %>%
  left_join(num, by = c("year", "is_ceo")) %>%
  mutate(
    num = replace_na(num, 0L),
    percent = 100 * num / denom
  )

mismatched_wide <- mismatched_years %>%
  pivot_wider(
    names_from = is_ceo,
    values_from = c(num, denom, percent),
    names_glue = "{.value}_ceo{is_ceo}"
  )

ggplot(
  mismatched_years,
  aes(
    x = year, y = percent,
    color = ifelse(is_ceo, "CEO", "Not CEO"),
    group = ifelse(is_ceo, "CEO", "Not CEO")
  )
) +
  geom_line(size = 1) + geom_point() +
  #scale_color_manual(values = c("Not CEO" = "steelblue", "CEO" = "firebrick")) + 
  labs(
    title = "Mismatched % by CEO status",
    x = "Year",
    y = "Percent",
    color = NULL  # remove "is_ceo" legend title
  ) +
  theme_minimal() +
  theme(legend.position = "bottom")

## get common tites


## sample missing cases

set.seed(123)  # for reproducibility
sampled_pairs <- mismatched %>%
  filter(year == 2017) %>%
  distinct(ahanumber, year) %>% 
  slice_sample(n = 5)

# get madmin
paired <- sampled_pairs %>% left_join(aha_madmin) %>%
  left_join(final %>% 
              distinct(entity_aha, full_name, firstname, lastname, title, title_standardized,
                       mname, entity_name) %>%
              rename(ahanumber = entity_aha) %>% 
              filter(title_standardized == "CEO:  Chief Executive Officer" |
                       str_detect(title, "CEO") | str_detect(title, "C.E.O") | 
                       str_detect(title, "Chief Executive")))

check_final <- final %>% 
  filter(entity_name == "healthsouth rehabilitation hospital of arlington") %>%
  distinct(year, entity_name, firstname, lastname, title, title_standardized, mname, entity_aha, campus_aha, unfiltered_campus_aha)

check_himss <- final %>% filter(entity_state == "IN" & str_detect(entity_name, "lutheran")) %>%
  distinct(year, entity_name, firstname, lastname, title, title_standardized, mname, entity_aha, campus_aha, unfiltered_campus_aha)

# 2015 seed
# 6740016 is because there was no ceo in HIMSS
# 6521207 AHA was an interim
# 6380995 AHA was correct
# 6420196 entity_aha wasn't assigned ; brian bauer was the CEO

##
check_year_mismatches <- merged_madmin %>%
  filter(str_detect(match_type, "\\d")) %>%
  mutate(himss_after_aha = himss_after_aha.x | himss_after_aha.y,
         himss_before_aha = himss_before_aha.x | himss_before_aha.y) %>%
  summarise(
    non_na_match_types = sum(!is.na(match_type)),
    himss_after_aha_pct = sum(himss_after_aha & !is.na(match_type), na.rm = TRUE)/non_na_match_types,
    himss_before_aha_pct = sum(himss_before_aha & !is.na(match_type), na.rm = TRUE)/non_na_match_types
  ) 

## check year mismatch cases
set.seed(123)  # for reproducibility
sampled_pairs <- merged_madmin %>%
  filter(str_detect(match_type, "\\d") ) %>%
  distinct(ahanumber, year, match_type) %>%
  slice_sample(n = 5)

# ryan cassedy - 6710230 - 2012 (himss CEO 2013) 
# reed edmundson - 6742590 - 2012 (in himss 2015-2017 as CEO)
# gregory h burfitt - 6232690 - 2009 (in himss 2010)
# matthew j perry - 6410020 - 2014 (maybe in 2011, definitely 2016 and 2017)
# duane runyan - 6710050 - 2011 (in himss in 2012)

# get madmin
paired <- sampled_pairs %>% left_join(aha_madmin)

check_himss <- final %>% 
  #filter(entity_uniqueid == 50138) %>%
  filter(entity_aha == 6221385) %>%
  #filter(contact_uniqueid == 2360615) %>%
  #filter(title_standardized == "CEO:  Chief Executive Officer") %>%
  #filter(str_detect(firstname, "matt")) %>%
  distinct(contact_uniqueid, firstname, lastname, title, title_standardized, year, entity_name, 
           entity_city, entity_state, entity_uniqueid, entity_aha)


#set.seed(123)  # for reproducibility
set.seed(1029)
sampled_pairs <- final %>%
  mutate(lower_title = tolower(title)) %>%
  filter(str_detect(lower_title, "chief excutive|ceo|c.e.o") | str_detect(title_standardized, "C.E.0.") ) %>%
  distinct(firstname, lastname, title, title_standardized, entity_name, year) %>%
  slice_sample(n = 5)