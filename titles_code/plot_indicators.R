
library(ggplot2)

# First, pivot longer for easier plotting
merged_long <- merged_export %>%
  pivot_longer(
    cols = -c(entity_uniqueid, year),
    names_to = "variable",
    values_to = "value"
  ) %>%
  # Parse the variable name into tier_group and metric
  mutate(
    tier_group = str_extract(variable, "^[a-z]+\\d"),
    metric = str_remove(variable, "^[a-z]+\\d_")
  )

# Aggregate to mean by year and tier_group (across entities)
plot_data <- merged_long %>%
  group_by(year, tier_group, metric) %>%
  summarize(
    mean_value = mean(value, na.rm = TRUE),
    .groups = "drop"
  )

# Plot 1: Share Active over time
ggplot(plot_data %>% filter(metric == "sh_active"),
       aes(x = year, y = mean_value, color = tier_group)) +
  geom_line(linewidth = 1) +
  geom_point() +
  labs(
    title = "Share of Roles Filled (Active) Over Time",
    x = "Year",
    y = "Share Active",
    color = "Tier Group"
  ) +
  theme_minimal() +
  scale_y_continuous(labels = scales::percent_format())

# Plot 2: Share Vacant over time
ggplot(plot_data %>% filter(metric == "sh_vacant"),
       aes(x = year, y = mean_value, color = tier_group)) +
  geom_line(linewidth = 1) +
  geom_point() +
  labs(
    title = "Share of Roles Vacant Over Time",
    x = "Year",
    y = "Share Vacant",
    color = "Tier Group"
  ) +
  theme_minimal() +
  scale_y_continuous(labels = scales::percent_format())

# Plot 3: Share DNE over time
ggplot(plot_data %>% filter(metric == "sh_dne"),
       aes(x = year, y = mean_value, color = tier_group)) +
  geom_line(linewidth = 1) +
  geom_point() +
  labs(
    title = "Share of Roles That Do Not Exist Over Time",
    x = "Year",
    y = "Share DNE",
    color = "Tier Group"
  ) +
  theme_minimal() +
  scale_y_continuous(labels = scales::percent_format())

# Plot 4: People per Role over time
ggplot(plot_data %>% filter(metric == "people_per_role"),
       aes(x = year, y = mean_value, color = tier_group)) +
  geom_line(linewidth = 1) +
  geom_point() +
  labs(
    title = "People per Role Over Time",
    x = "Year",
    y = "People per Role",
    color = "Tier Group"
  ) +
  theme_minimal()