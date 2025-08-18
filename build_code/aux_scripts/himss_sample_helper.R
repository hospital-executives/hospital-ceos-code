## helper functions to generate sample

get_script_directory <- function() {
  # Check if rstudioapi is available and RStudio is running
  if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    # Running in RStudio
    script_directory <- dirname(rstudioapi::getActiveDocumentContext()$path)
  } else {
    # Not running in RStudio, try to get the script path via commandArgs
    cmdArgs <- commandArgs(trailingOnly = FALSE)
    fileArgName <- "--file="
    scriptPath <- NULL
    for (arg in cmdArgs) {
      if (startsWith(arg, fileArgName)) {
        scriptPath <- substring(arg, nchar(fileArgName) + 1)
        break
      }
    }
    if (!is.null(scriptPath)) {
      script_directory <- dirname(normalizePath(scriptPath))
    } else {
      # Cannot determine script directory
      stop("Cannot determine the script directory.")
    }
  }
  
  return(dirname(script_directory))
}

check_titles <-  function(df, sample_dict) {
  # Get unique titles from the data
  available_titles <- unique(df$title_standardized)
  
  # Check if any title in sample_dict is not in df$title
  invalid_titles <- setdiff(names(sample_dict), available_titles)
  
  # Raise an error if invalid titles exist
  if (length(invalid_titles) > 0) {
    stop(paste("Error: The following titles are not present in the dataset:", paste(invalid_titles, collapse = ", ")))
  }}

sample_ids_by_title <- function(df, sample_dict, seed = selected_seed) {
  seed_to_use <- ifelse(is.na(selected_seed), 42, selected_seed)
  set.seed(seed_to_use)
  
  sampled_ids <- list()
  used_ids <- c()  # Track already sampled IDs
  
  for (title in valid_titles) {
    col_name <- paste0("has_", title)  # Corresponding column in df
    
    # Get IDs where the indicator is 1 and not already used
    eligible_ids <- setdiff(df$contact_uniqueid[df[[col_name]] == 1], used_ids)
    
    # Sample without replacement, ensuring unique selection
    sample_size <- min(length(eligible_ids), sample_dict[[title]])
    
    if (sample_size > 0) {
      sampled <- sample(eligible_ids, sample_size, replace = FALSE)
      sampled_ids[[title]] <- sampled
      used_ids <- c(used_ids, sampled)  # Add sampled IDs to exclusion list
    } else {
      sampled_ids[[title]] <- c()  # Empty if no valid IDs left
    }
  }
  
  return(sampled_ids)
}

extract_sampled_data <- function(df, sampled_results) {
  sampled_dfs <- list()
  
  for (title in names(sampled_results)) {
    sampled_ids <- sampled_results[[title]]
    
    # Filter the original dataset for the sampled IDs
    sampled_dfs[[title]] <- df %>%
      filter(contact_uniqueid %in% sampled_ids) %>%
      select(contact_uniqueid,firstname,lastname, year, 
             title_standardized, title, entity_name, entity_state, 
             mname, entity_type) %>%
      rename(himss_name = entity_name, aha_name = mname)
  }
  
  return(sampled_dfs)
}

fill_missing_years <- function(sub_df, years) {
  complete_data <- list()
  
  for (id in unique(sub_df$contact_uniqueid)) {
    id_data <- sub_df %>% filter(contact_uniqueid == id)
    
    # Initialize `is_imputed` for original rows
    id_data <- id_data %>% mutate(missing_himss = 0)  # Original data gets `0`
    
    # Get first available firstname/lastname
    first_firstname <- id_data$firstname[1]
    first_lastname <- id_data$lastname[1]
    
    # Identify missing years for this ID
    existing_years <- id_data$year
    missing_years <- setdiff(years, existing_years)
    
    # Create missing rows with first available firstname/lastname, filling other columns with NA
    if (length(missing_years) > 0) {
      missing_rows <- data.frame(
        contact_uniqueid = id,
        year = missing_years,
        firstname = first_firstname,
        lastname = first_lastname,
        missing_himss = 1  # Mark as imputed
      )
      
      # Add NA for other columns
      other_cols <- setdiff(names(sub_df), c("contact_uniqueid", "year", "firstname", "lastname", "missing_himss"))
      missing_rows[other_cols] <- NA
      
      # Combine existing and missing data
      id_data <- bind_rows(id_data, missing_rows)
    }
    
    complete_data[[as.character(id)]] <- id_data
  }
  
  # Return combined data frame sorted by ID and Year
  return(bind_rows(complete_data) %>% arrange(contact_uniqueid, year))
}

add_na_columns <- function(df, column_names) {
  # Use mutate to create new columns filled with NA
  df <- df %>%
    mutate(!!!setNames(rep(list(NA), length(column_names)), column_names))
  
  return(df)
}

clean_sheet_name <- function(name) {
  name <- str_replace_all(name, "[:\\[\\]\\*\\?/\\\\]", "")  # Remove invalid characters
  name <- str_trim(name)  # Trim leading/trailing spaces
  return(name)
}