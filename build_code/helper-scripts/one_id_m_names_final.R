
########### MAIN FUNCTIONS FOR PROCESSING ########### 
manual_one_id_m_names <- function(df) {
  
  ############    GET NICKNAMES DICTIONARIES   ############ 
  if (exists("data_path")) {
    # Print the provided arguments
    names1 <- read.csv(paste0(data_path, "supplemental/carltonnorthernnames.csv"), header = FALSE)
    
    female <- read.csv(paste0(data_path, "supplemental/female_diminutives.csv"), header = FALSE)
    female_lower <- female
    female_lower[] <- lapply(female_lower, function(x) {
      if (is.character(x)) tolower(x) else x
    })
    
    male <- read.csv(paste0(data_path, "supplemental/male_diminutives.csv"), header = FALSE)
    male_lower <- male
    male_lower[] <- lapply(male_lower, function(x) {
      if (is.character(x)) tolower(x) else x
    })
  }
    else {
      print("Please set data_path.")
    }
  
  
  
  ############ GET CONTACT IDS WITH MORE THAN ONE NAME  ############ 
  result <- df %>%
    group_by(contact_uniqueid)  %>%
    summarise(num_fullnames = n_distinct(full_name)) %>%
    filter(num_fullnames >1) 
  
  merge <- df %>% semi_join(result, by = "contact_uniqueid") %>%
    #select(firstname, lastname, middleinitial, contact_uniqueid, entity_name,year,
          # email, credentials, entity_uniqueid, entity_zip) %>%
    #mutate(full_name = str_to_lower(str_replace_all(paste0(firstname, lastname), 
                                                    #"[[:punct:]\\s]", ""))) %>%
    group_by(contact_uniqueid) %>%
    summarise(namecount = n_distinct(full_name))
  
  multiple_ids <- merge %>% subset(namecount > 1) #15831
  multiple_df <- df %>% filter(contact_uniqueid %in% 
                                 multiple_ids$contact_uniqueid) #%>%
    #select(contact_uniqueid, firstname, lastname, full_name, entity_name, 
          # system_id, contactid, title, year, phone, entity_zip)
  
  
  ####### FORMAT DATA - add necessary similarity/dissimilarity measures #######
  measures <- multiple_df %>%
    group_by(contact_uniqueid) %>%
    mutate(firstcount = n_distinct(paste(firstname)),
           lastcount = n_distinct(paste(lastname)),
           first_lev = max_levenshtein(firstname),
           last_lev = max_levenshtein(lastname),
           full_lev = max_levenshtein(full_name)) %>%
    ungroup() 
  
  
  ################  TELEPHONES ################ 
  numbers <- measures %>%
    group_by(contact_uniqueid) %>%
    mutate(
      telecount = ifelse(
        any(is.na(phone)) & any(!is.na(phone)),
        NA,
        n_distinct(paste(phone[!is.na(phone)]))
      )) %>%
    filter(!all(telecount==1 & ((firstcount==1 & last_lev<2) | (lastcount==1 & 
                                                                  first_lev < 2)))) %>%
    select(-c(telecount,phone))
  
  
  ################  MARRIAGE ################ 
  marriage <- numbers %>%
    group_by(contact_uniqueid) %>%
    mutate(systemid_count = n_distinct(paste(firstname, system_id)),
           entityname_count = n_distinct(paste(firstname, entity_name))) %>%
    arrange(contact_uniqueid, year) %>%
    mutate(
      lastnames_match = ifelse(row_number() == 1, TRUE, lastname==lag(lastname)),
      titlesmatch = ifelse(row_number() == 1, TRUE, title == lag(title))
    ) %>%
    group_by(contact_uniqueid) %>%
    mutate(
      all_lastnames_match = all(lastnames_match),
      is_married1 = ifelse(all_lastnames_match, TRUE, 
                           any(!lastnames_match & titlesmatch))
    ) %>%
    ungroup()  %>% 
    # part a
    filter(!(first_lev < 2 & lastcount == 2 & is_married1 & 
               (systemid_count==1 | entityname_count ==1))) %>%
    select(-is_married1) %>%
    group_by(contact_uniqueid) %>%
    mutate(
      troubleshooting = (first_lev < 2 & lastcount==2 & 
                           (system_id == lag(system_id) | 
                              entity_name == lag(entity_name)))) %>%
    ungroup() %>%
    group_by(contact_uniqueid) %>%
    mutate(any_true = any(troubleshooting)) %>%
    ungroup() %>%
    # part b
    filter(!any_true) %>%
    select(-c(troubleshooting, any_true, lastnames_match, titlesmatch, all_lastnames_match)) %>%
    group_by(contact_uniqueid) %>%
    mutate(num_titles = n_distinct(title)) %>%
    # part c
    filter(!(first_lev < 2 & lastcount == 3 & (systemid_count == 1 | entityname_count == 1 |num_titles == 1))) %>%
    group_by(contact_uniqueid) %>%
    # part d
    filter(!(firstcount == 1)) %>%
    ungroup()
  
  
  
  
  ################  SINGLE LETTER TYPOS ################ 
  typos <- marriage %>%
    group_by(contact_uniqueid) %>%
    filter(!((first_lev < 2 & lastcount ==1) | (firstcount ==1 & last_lev < 2) | full_lev == 1)) %>% #5360
    ungroup()
  
  
  
  ################  NAMES SWITCHED ################ 
  drop_switched <- typos %>%
    group_by(contact_uniqueid) %>%
    mutate(switched = is_switched(firstname, lastname)) %>%
    ungroup() %>%
    filter(!(firstcount == 2 & lastcount == 2 & switched)) %>%
    select(-switched)
  
  
  ################  SUBSTRINGS   ################  
  drop_substrings <- drop_switched %>% 
    group_by(contact_uniqueid) %>%
    mutate(firstname_is_subset1 = drop_substrings_fn1(unique(firstname)), # standard
           firstname_is_subset2 = drop_substrings_fn2(unique(firstname))) %>% # joe/joseph
    ungroup() %>%
    filter(!((firstname_is_subset1 |firstname_is_subset2) & last_lev<2)) %>% #first name subset
    group_by(contact_uniqueid) %>%
    mutate(lastname_is_subset = drop_substrings_fn1(unique(lastname))) %>%
    ungroup() %>%
    filter(!(lastname_is_subset & first_lev<2)) %>% # last name subset
    group_by(contact_uniqueid) %>%
    mutate(fullname_is_subset = drop_substrings_fn1(unique(full_name))) %>%
    ungroup() %>%
    filter(!fullname_is_subset)
  
  
  ################  INITIALS ################  
  drop_initials <- drop_substrings %>%
    mutate(stripped = tolower(gsub("[[:punct:]]", "", firstname))) %>%
    mutate(is_initial = (nchar(stripped) <= 2)) %>%
    group_by(contact_uniqueid) %>%
    mutate(has_initials = any(nchar(stripped) <= 2 | str_detect(firstname, "\\.")),
           comparison = ifelse(is_initial, substr(stripped,1,1),stripped),
           confirmed_initial = drop_substrings_fn3(unique(stripped))) %>%
    filter(!(confirmed_initial & has_initials & last_lev < 2))
  
  ################  NICKNAMES   ################  
  drop_nicknames1 <- drop_initials %>%
    group_by(contact_uniqueid) %>%
    mutate(
      close_last = (last_lev < 2),
      # first name dictionary
      all_in_same_row = names_in_same_row(unique(firstname), names1)) %>%
    ungroup() %>%
    filter(!(all_in_same_row & close_last))  %>%
    group_by(contact_uniqueid) %>%
    mutate(
      # second name dictionary
      fem_same_row = names_in_same_row(unique(firstname), female_lower),
      male_same_row = names_in_same_row(unique(firstname), male_lower)) %>%
    ungroup() %>%
    filter(!((fem_same_row | male_same_row) & last_lev <2)) 
  
  drop_nicknames2 <- drop_nicknames1 %>%
    group_by(contact_uniqueid) %>%
    mutate(
      manual_nickname = same_names2(firstname)) %>%
    ungroup() %>%
    filter(!(manual_nickname & last_lev <2))
  
  
  
  ################  LEVENSHTEIN AS PRIMARY CUTOFF   ################  
  if (!exists("normalized_levenshtein_cutoff")) {
    normalized_levenshtein_cutoff <- 0.3
    }
    
  drop_similar_names <- drop_nicknames2 %>%
    group_by(contact_uniqueid) %>%
    mutate(max_lev_dist_norm = max_normalized_levenshtein(full_name)) %>%
    ungroup() %>% 
    filter(max_lev_dist_norm > normalized_levenshtein_cutoff)
  
  
  ################  DROP COMBINATION FIRST NICKNAME/SMALL DIFFERENCE IN LAST   ################  
  combos <- drop_similar_names %>%
    filter(!((firstname_is_subset1|firstname_is_subset2|first_lev < 2|all_in_same_row|
                fem_same_row|male_same_row|manual_nickname|(confirmed_initial & has_initials))
             & (lastname_is_subset|last_lev < 2)))

  
  
  ################  METAPHONE CODE   ################  
  combos$cleanfirst <- clean_string(combos$firstname)
  combos$cleanlast <- clean_string(combos$lastname)
  metaphone <- combos %>%
    mutate(first_metaphone = metaphone(cleanfirst),
           last_metaphone = metaphone(cleanlast),
           full_metaphone = metaphone(full_name)) %>%
    group_by(contact_uniqueid) %>%
    filter(!(n_distinct(full_name) == 1 | 
             (n_distinct(first_metaphone)==1 & (last_lev < 2 | 
                                                  lastname_is_subset |n_distinct(last_metaphone)==1)) |
             ((firstname_is_subset1|firstname_is_subset2|(confirmed_initial & 
                                                            has_initials)|
                 all_in_same_row|manual_nickname|first_lev < 2) & (n_distinct(last_metaphone)==1 ))))
  
  
  ################  FORMAT FOR RETURN   ################ 
  confirmed <- df %>%
    anti_join(metaphone)
  
  return(list(remaining = metaphone, confirmed = confirmed))
}

matching_one_id_m_names <- function(df, confirmed, remaining) {
  
  ########### PT 1: UPDATE IDS AND DROP IF NEW IDS ARE UNIQUE ########### 
  # determine one-to-one ids 
  ids_with_one_name <- df %>%
    group_by(contact_uniqueid) %>%
    filter(n_distinct(full_name) == 1) %>%
    pull(contact_uniqueid)
  
  # determine problem cases
  problems <- df %>%
    filter((full_name %in% remaining$full_name |
              contact_uniqueid %in% remaining$contact_uniqueid) &
             !grepl("[A-Za-z]", entity_zip)) %>%
    group_by(contact_uniqueid) %>%
    mutate(num_names = n_distinct(full_name)) %>%
    ungroup() %>%
    group_by(full_name) %>%
    mutate(num_ids = n_distinct(contact_uniqueid)) %>%
    ungroup() %>%
    filter(!(num_names == 1 & num_ids == 1))
  
  # update ids 
  case1 <- problems %>%
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
      entityid_match = any(entity_uniqueid %in% unlist(entityid_list))
    ) %>%
    ungroup()
  
  case1$true_count <- rowSums(case1[, c("add_match", "entity_name_match", 
                                        "entityid_match")])
  
  updated_ids <- case1 %>%
    arrange(full_name, desc(year)) %>%
    group_by(full_name) %>%
    mutate(potential_id = first(contact_uniqueid),
           min = min(true_count)) %>%
    ungroup() %>%
    mutate(updated_id = ifelse((!is.na(potential_id)) & min > 0 & 
                                 potential_id %in% ids_with_one_name, 
                               contact_uniqueid, potential_id)) %>%
    group_by(full_name) %>%
    mutate(is_new_id = (n_distinct(updated_id) ==1 | min > 0)) %>%
    ungroup() %>%
    mutate(is_new_id = ifelse(updated_id %in% ids_with_one_name & is_new_id, TRUE, FALSE))
  
  unique1 <- updated_ids %>%
    group_by(full_name) %>%
    filter((n_distinct(updated_id) == 1 | min > 0) & all(is_new_id)) %>%
    ungroup() %>%
    filter(updated_id %in% ids_with_one_name) %>% 
    group_by(full_name) %>%
    filter(n_distinct(updated_id) ==1) # to add to confirmed data frame
  
  remaining1 <- updated_ids %>%
    anti_join(unique1)

  ########### PT 2: CHECK IF REMAINING IDS ARE UNIQUE AND DROP IF SO ########### 
  updated_ids_with_one_name <- updated_ids %>%
    group_by(contact_uniqueid) %>%
    filter(n_distinct(full_name) == 1) %>%
    pull(contact_uniqueid)
  
  case2 <- remaining1 %>%
    select(-c(min)) %>%
    arrange(full_name, desc(year)) %>%
    group_by(full_name) %>%
    mutate(
      first_id = first(contact_uniqueid),
      add_list = list(unique(entity_address[contact_uniqueid == first_id])),
      entity_name_list = list(unique(entity_name[contact_uniqueid == first_id])),
      entityid_list = list(unique(entity_uniqueid[contact_uniqueid == first_id]))) %>%
    ungroup() %>%
    group_by(full_name) %>%
    mutate(
      add_match = any(entity_address %in% unlist(add_list)),
      entity_name_match = any(entity_name %in% unlist(entity_name_list)),
      entityid_match = any(entity_uniqueid %in% unlist(entityid_list))
    ) %>%
    ungroup() %>%
    mutate(true_count = rowSums(select(., add_match, entity_name_match, entityid_match))) %>%
    arrange(full_name, desc(year)) %>%
    group_by(full_name) %>%
    mutate(potential_id = first(contact_uniqueid),
           min = min(true_count)) %>%
    ungroup() %>%
    mutate(updated_id = ifelse((!is.na(potential_id)) & min > 0 & 
                                 potential_id %in% updated_ids_with_one_name, potential_id, contact_uniqueid))
  
  unique2 <- case2 %>%
    group_by(full_name) %>%
    filter((n_distinct(updated_id) ==1 | min > 0) & updated_id %in% updated_ids_with_one_name) 
  
  remaining3 <- case2 %>%
    anti_join(unique2) %>%
    group_by(full_name) %>%
    mutate(num_id = n_distinct(contact_uniqueid)) %>%
    ungroup() %>%
    group_by(contact_uniqueid) %>%
    mutate(num_name = n_distinct(full_name)) %>%
    ungroup()
  
  
  ########### PT 3: FORMAT RETURN ###########
  new_confirmed_ids <- bind_rows(unique1, unique2)

  
  return(list(remaining = remaining3, updated_ids = new_confirmed_ids))
  
  
}

update_ids <- function(confirmed){
  case1 <- confirmed %>%
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
      entityid_match = any(entity_uniqueid %in% unlist(entityid_list))
    ) %>%
    ungroup()
  
  case1$true_count <- rowSums(case1[, c("add_match", "entity_name_match", 
                                        "entityid_match")])
  
  updated_ids <- case1 %>%
    arrange(full_name, desc(year)) %>%
    group_by(full_name) %>%
    mutate(potential_id = first(contact_uniqueid),
           min = min(true_count)) %>%
    ungroup() %>%
    mutate(updated_id = ifelse((!is.na(potential_id)) & min > 0 , 
                               potential_id, contact_uniqueid))
  
    result <- updated_ids %>%
      mutate(contact_uniqueid = updated_id) %>%
      select(-updated_id)
    
    return(result)
}
  
################  HELPER FUNCTIONS   ################  
max_levenshtein <- function(names) {
  if (length(names) < 2) return(0)
  dist_matrix <- stringdistmatrix(names, names, method = "lv")
  max(dist_matrix[upper.tri(dist_matrix)])
}

is_switched <- function(first_names, last_names) {
  for (i in seq_along(first_names)) {
    for (j in seq_along(last_names)) {
      if (i != j && paste(first_names[i], last_names[i]) == paste(last_names[j], first_names[j])) {
        return(TRUE)
      }
    }
  }
  return(FALSE)
}

drop_substrings_fn1 <- function(names) {
  if (length(names) <= 1) {
    return(FALSE)
  }
  
  names <- tolower(names)
  n <- length(names)
  is_subset <- TRUE
  
  for (i in 1:(n-1)) {
    subset_found <- FALSE
    for (j in (i+1):n) {
      if ((str_detect(names[j], fixed(names[i])) && nchar(names[j]) != nchar(names[i])) || 
          (str_detect(names[i], fixed(names[j])) && nchar(names[i]) != nchar(names[j]))) {
        subset_found <- TRUE
        break
      }
    }
    if (!subset_found) {
      is_subset <- FALSE
      break
    }
  }
  
  return(is_subset)
}

generate_variations <- function(name) {
  if (nchar(name) == 1) {
    return(c(name))
  }
  
  variations <- c(substr(name, 1, nchar(name) - 1))
  unique(variations)
}

drop_substrings_fn2 <- function(names) {
  if (length(names) != 2) {
    return(FALSE)
  }
  
  names <- tolower(names)
  n <- length(names)
  is_subset <- TRUE
  
  for (i in 1:n) {
    variations <- generate_variations(names[i])    
    
    for (j in 1:n) {
      if (i != j) {  # Ensure we're not comparing the name to itself
        for (variation in variations) {
          if (nchar(variation) > 0 & str_detect(names[j], fixed(variation)) && nchar(names[j]) != nchar(variation)) {
            return(TRUE)
          }
        }
      }
    }
  }
  return(FALSE)
}

drop_substrings_fn3 <- function(names) {
  if (length(names) != 2) {
    return(FALSE)
  }
  
  names <- tolower(names)
  n <- length(names)
  
  for (i in 1:(n-1)) {
    for (j in (i+1):n) {
      if ((str_starts(names[j], substr(fixed(names[i]),1,1)) && nchar(names[j]) != nchar(names[i])) || 
          (str_starts(names[i], substr(fixed(names[j]),1,1)) && nchar(names[i]) != nchar(names[j]))) {
        return(TRUE)
      }
    }
  }
  
  return(FALSE)
}

names_in_same_row <- function(names, df) {
  names <- tolower(names)
  
  formatted_df <- data.frame(combined = apply(df, 1, function(row) 
    paste(row, collapse = ",")))
  
  for (i in 1:nrow(formatted_df)) {
    diminutive_names <- unlist(strsplit(as.character(formatted_df$combined[i]), ","))
    full_names <- c(diminutive_names)
    if (all(names %in% full_names)) {
      return(TRUE)
    }
  }
  return(FALSE)
}

same_names2 <- function(names) {
  names <- tolower(names)
  for (i in 1:nrow(names_df)) {
    diminutive_names <- unlist(names_df$names[i])
    if (all(names %in% diminutive_names)) {
      return(TRUE)
    }
  }
  return(FALSE)
}

max_normalized_levenshtein <- function(names) {
  if (length(names) < 2) return(0)
  n <- length(names)
  max_distance <- 0
  for (i in 1:(n-1)) {
    for (j in (i+1):n) {
      dist <- stringdist(names[i], names[j], method = "lv")
      max_len <- max(nchar(names[i]), nchar(names[j]))
      normalized_dist <- dist / max_len
      if (normalized_dist > max_distance) {
        max_distance <- normalized_dist
      }
    }
  }
  return(max_distance)
}

clean_string <- function(s) {
  # Convert to lowercase
  s <- tolower(s)
  # Remove non-standard letters (retain only a-z)
  s <- str_replace_all(s, "[^a-z]", "")
  return(s)
}



################  MANUAL NICKNAMES DICTIONARY ################  
names_df <- tibble(names = I(list()),
                   stringsAsFactors = FALSE)
names_df <- rbind(names_df, data.frame(names = I(list(c("carolyn", "caroline")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("stephanie", "stefanie")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("andy", "andreas")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("stephen", "steven")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("melanie", "melany")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("margaret", "peggy")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("jeffry", "jeffery")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("suann", "susan")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("dell", "dal")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("john william", "bill")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("gail", "gale")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("maureen", "maurine")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("alice", "allison")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("nate", "nathanial")))))
names_df <- rbind(names_df, data.frame(names = I(list(c("maylan", "mahlon")))))

