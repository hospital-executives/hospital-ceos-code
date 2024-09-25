
matching_one_name_m_ids <- function(df, outliers) {
  
  # get names that correspond to multiple ids
  names_with_mult_ids <- df %>%
    group_by(full_name) %>%
    filter(n_distinct(contact_uniqueid) > 1) %>%
    ungroup() %>%
    distinct(full_name) %>%
    pull(full_name)
  
  # run matching algo
  case1a <- df %>%
    #select(full_name, contact_uniqueid, entity_address, entity_zip, entity_state, cbsa,
         #  entity_name, entity_uniqueid, system_id, title, title_standardized,year) %>%
    filter(full_name %in% names_with_mult_ids & !contact_uniqueid %in% outliers$contact_uniqueid) %>%
    mutate(newzip = substr(entity_zip, 1, 5)) %>%
    arrange(full_name, contact_uniqueid) %>%
    group_by(full_name) %>%
    mutate(
      first_id = first(contact_uniqueid),
      add_list = list(unique(entity_address[contact_uniqueid == first_id])),
      entity_name_list = list(unique(entity_name[contact_uniqueid == first_id])),
      entityid_list = list(unique(entity_uniqueid[contact_uniqueid == first_id]))) %>%
    ungroup() %>%
    group_by(full_name, contact_uniqueid) %>%
    mutate(
      add_match = any(entity_address %in% unlist(add_list)),
      entity_name_match = any(entity_name %in% unlist(entity_name_list)),
      entityid_match = any(entity_uniqueid %in% unlist(entityid_list))) %>%
    ungroup()
  
  case1a$true_count <- rowSums(case1a[, c("add_match","entity_name_match", "entityid_match")])
  
  updated_ids <- case1a %>%
    arrange(full_name, desc(year)) %>%
    group_by(full_name) %>%
    mutate(newid = first(contact_uniqueid)) %>%
    ungroup() %>%
    mutate(updated_id = ifelse(!(is.na(newid)) & min(true_count) > 0 , newid, 
                               contact_uniqueid))
  
  # df of people with multiple ids that are the same person 
  same <- updated_ids %>%
    group_by(full_name) %>%
    filter((n_distinct(updated_id) ==1 | min(true_count) > 0)) 
  
  # remainder
  new_remaining <- updated_ids %>%
    anti_join(same)
  
  # update ids
  same <- same %>%
    mutate(updated_id = newid)
  
  # change to dt to make more efficient
  setDT(df)
  setDT(new_remaining)
  setDT(same)
  
  to_confirm <- df[!new_remaining, on = .(full_name, contact_uniqueid)]
  
  # Add one_id column based on the distinct count of contact_uniqueid within full_name
  to_confirm[, one_id := .N == 1, by = full_name]
  
  # Separate rows with one_id and multiple ids
  one_id <- to_confirm[one_id == TRUE]
  mult_ids <- to_confirm[one_id == FALSE]
  
  # Filter mult_ids to update remainder
  update_remainder <- mult_ids[!same, on = .(full_name, contact_uniqueid)]
  
  # Combine results for confirmed and remaining
  confirmed_result <- bind_rows(one_id, same) %>%
    mutate(contact_uniqueid = ifelse(!is.na(updated_id), updated_id, contact_uniqueid)) %>%
    select(-updated_id)
 
  # bind together
  df1 <- as.data.frame(new_remaining)
  df2 <- as.data.frame(update_remainder)
  all_columns <- union(names(df1), names(df2))
  
  # Add missing columns to df1 and df2, filling with NA
  df1[setdiff(all_columns, names(df1))] <- NA
  df2[setdiff(all_columns, names(df2))] <- NA
  
  # Reorder the columns to match
  df1 <- df1[all_columns]
  df2 <- df2[all_columns]
  
  # Bind the rows
  remaining_result <- rbind(df1, df2)
  
  return(list(remaining = remaining_result, updated_id = same, confirmed = confirmed_result))
}


combine_chunks_iteratively_to_disk <- function(dt1, dt2, chunk_size = 1e5, temp_dir = "temp_chunks") {
  if (!dir.exists(temp_dir)) dir.create(temp_dir)
  
  total_rows <- max(nrow(dt1), nrow(dt2))
  chunk_files <- list()
  
  for (start in seq(1, total_rows, by = chunk_size)) {
    end <- min(start + chunk_size - 1, total_rows)
    chunk1 <- dt1[start:min(end, nrow(dt1)), ]
    chunk2 <- dt2[start:min(end, nrow(dt2)), ]
    combined_chunk <- rbindlist(list(chunk1, chunk2), fill = TRUE)
    chunk_file <- file.path(temp_dir, paste0("chunk_", start, "_", end, ".rds"))
    saveRDS(combined_chunk, chunk_file)
    chunk_files <- c(chunk_files, chunk_file)
  }
  
  return(chunk_files)
}

recombine_chunks_from_disk <- function(chunk_files) {
  combined_result <- rbindlist(lapply(chunk_files, readRDS), fill = TRUE)
  return(combined_result)
}

zip_algo <- function(remaining, confirmed, outliers){
  zip_df <- remaining %>%
    filter(!contact_uniqueid %in% outliers$contact_uniqueid) %>%
    mutate(newzip = substr(entity_zip, 1, 5)) %>%
    arrange(full_name, contact_uniqueid) %>%
    group_by(full_name) %>%
    mutate(
      first_id = first(contact_uniqueid),
      zip_list = list(unique(newzip[contact_uniqueid == first_id]))) %>%
    ungroup() %>%
    group_by(full_name, contact_uniqueid) %>%
    mutate(
      zip_match = any(newzip %in% unlist(zip_list))) %>%
    ungroup() %>%
    group_by(full_name) %>%
    mutate(different = !all(zip_match)) 
  
  different <- zip_df %>%
    filter(different)
  
  same <- zip_df %>%
    filter(!different) %>%
    arrange(full_name, desc(year)) %>%
    group_by(full_name) %>%
    mutate(newid = first(contact_uniqueid),
           contact_uniqueid = newid) %>%
    ungroup() 
  
  remainder1 <- remaining %>%
    filter(contact_uniqueid %in% outliers$contact_uniqueid) 
  
  new_remaining <- bind_rows(different, remainder1) 
  new_confirmed <- bind_rows(confirmed, same)
  
  return(list(remaining = new_remaining, confirmed = new_confirmed))
}
  
frequency_processing <- function(confirmed_df, remaining, outliers){
  
  ## get frequencies from verified data frame
  distinct_df <- confirmed_df %>%
    filter(!contact_uniqueid %in% outliers$contact_uniqueid) %>%
    filter(!is.na(firstname), !is.na(lastname)) %>%
    distinct(contact_uniqueid, firstname, lastname)
  
  # Calculate numfirst and numlast
  numfirst <- distinct_df %>%
    distinct(firstname, contact_uniqueid) %>%
    nrow()
  
  numlast <- distinct_df %>%
    distinct(lastname, contact_uniqueid) %>%
    nrow()
  
  # Calculate first frequencies
  firstfreqs <- distinct_df %>%
    group_by(firstname) %>%
    summarise(firstfreq = n() / numfirst, .groups = 'drop')
  
  # Calculate last frequencies
  lastfreqs <- distinct_df %>%
    group_by(lastname) %>%
    summarise(lastfreq = n() / numlast, .groups = 'drop')
  
  ## merge frequencies with remaining observations
  frequency_df <- remaining %>%
    group_by(full_name, year) %>%
    mutate(num_states = n_distinct(entity_state)) %>%
    ungroup() %>%
    group_by(full_name) %>%
    mutate(mult_states = any(num_states >1),
           num_states = n_distinct(entity_state)) %>%
    ungroup() %>%
    # make sure you don't need to filter on names_with_mult_ids
    left_join(firstfreqs %>% select(firstname, firstfreq), by = "firstname") %>%
    left_join(lastfreqs %>% select(lastname, lastfreq), by = "lastname") %>%
    distinct()
  
  frequency_df$lastfreq[is.na(frequency_df$lastfreq)] <- 0
  frequency_df$firstfreq[is.na(frequency_df$firstfreq)] <- 0
  
  ## derive df for determining percentiles
  quantile_sample <- frequency_df %>%
    distinct(full_name, firstfreq, lastfreq)
  
  ## separate cases with one id vs multiple ids
  num_ids <- frequency_df %>%
    group_by(full_name) %>%
    mutate(num_ids = n_distinct(contact_uniqueid))
  one_id <- num_ids %>%
    filter(num_ids == 1)
  mult_id <- num_ids %>%
    filter(num_ids > 1)
  
  return(list(first = firstfreqs, last = lastfreqs, frequency_df = frequency_df,
              quantile_sample = quantile_sample, one_id = one_id, mult_id = mult_id))
  
}

group_by_quantile <- function(input_data, first_percentile_1 = 0.5,
                              first_percentile_2 = NA, 
                              last_percentile_1 = 0.5, 
                              last_percentile_2 = NA) {
  
  two_cutoffs = !(is.na(first_percentile_2)) & !is.na(last_percentile_2)
  
  first_cutoff_1 <- quantile(quantile_sample$firstfreq, probs = 
                             first_percentile_1, na.rm = TRUE)
  last_cutoff_1 <- quantile(quantile_sample$lastfreq, probs = 
                            last_percentile_1, na.rm = TRUE)
  
  if (two_cutoffs) {
    first_cutoff_2 <- quantile(quantile_sample$firstfreq, probs = 
                                 first_percentile_2, na.rm = TRUE)
    last_cutoff_2 <- quantile(quantile_sample$lastfreq, probs = 
                                last_percentile_2, na.rm = TRUE)
    
    to_update <- input_data %>%
      filter((firstfreq < first_cutoff_1 & lastfreq < last_cutoff_1) |
               (firstfreq < first_cutoff_2 & lastfreq < last_cutoff_2)) %>%
      arrange(full_name, desc(year)) %>%
      group_by(full_name) %>%
      mutate(newid = first(contact_uniqueid),
             contact_uniqueid = newid) %>%
      select(-newid) %>%
      ungroup() 
    
    temp_remaining <- input_data %>%
      filter((firstfreq >= first_cutoff_1 | lastfreq >= last_cutoff_1) & 
               (firstfreq >= first_cutoff_2 | lastfreq >= last_cutoff_2))
    
    return(list(remaining = temp_remaining, confirmed = to_update))
  }
  
  else{
  to_update <- input_data %>%
    filter(firstfreq < first_cutoff_1 & lastfreq < last_cutoff_1) %>%
    arrange(full_name, desc(year)) %>%
    group_by(full_name) %>%
    mutate(newid = first(contact_uniqueid),
           contact_uniqueid = newid) %>%
    select(-newid) %>%
    ungroup() 
  
  temp_remaining <- input_data %>%
    filter(firstfreq >= first_cutoff_1 | lastfreq >= last_cutoff_1)
  
  return(list(remaining = temp_remaining, confirmed = to_update))
  }
}