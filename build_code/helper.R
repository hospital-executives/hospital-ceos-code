
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

last_name_overlap <- function(last_aha, last_himss) {
  last_aha <- tolower(last_aha)
  last_himss <- tolower(last_himss)
  
  grepl(last_aha, last_himss, fixed = TRUE) || grepl(last_himss, last_aha, fixed = TRUE)
}

truthy <- function(x) {
  if (is.logical(x)) return(isTRUE(x))                 # TRUE only if exactly TRUE
  if (is.character(x)) return(!is.na(x) && nzchar(x))  # any non-empty string
  if (is.numeric(x)) return(!is.na(x) && x > 0)        # any positive number
  if (is.list(x) && length(x)) return(truthy(x[[1]]))  # unwrap one level
  FALSE
}

process_chunk <- function(df) {
  df %>%
    mutate(
      last_jw  = stringdist(last_aha,  last_himss,  method = "jw", p = 0.1, nthread = 1),
      first_jw = stringdist(first_aha, first_himss, method = "jw", p = 0.1, nthread = 1),
      full_jw  = stringdist(full_aha,  himss_full,  method = "jw", p = 0.1, nthread = 1),
      
      # booleans, not lists:
      nick_1 = map2_lgl(first_aha, first_himss, ~ isTRUE(names_in_same_row_dict(.x, .y, dict1))),
      nick_2 = map2_lgl(first_aha, first_himss, ~ isTRUE(names_in_same_row_dict(.x, .y, dict2))),
      nick_3 = map2_lgl(first_aha, first_himss, ~ isTRUE(names_in_same_row_dict(.x, .y, dict3))),
      
      last_substring  = map2_lgl(last_aha,  last_himss,  ~ truthy(last_name_overlap(.x, .y))),
      first_substring = map2_lgl(first_aha, first_himss, ~ truthy(last_name_overlap(.x, .y)))
    )
}