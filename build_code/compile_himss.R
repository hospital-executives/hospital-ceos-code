# Compile_himss.R 

##### GET PARAMS #####
args <- commandArgs(trailingOnly = TRUE)
code_dir <- args[1]
cat(code_dir)

##### SET UP FILE PATHS ######
get_script_directory <- function() {
  # Try to get the path of the current Rmd file during rendering
  if (!is.null(knitr::current_input())) {
    script_directory <- dirname(normalizePath(knitr::current_input()))
    return(script_directory)
  }
  
  # Fallback to params$code_dir if provided
  if (!is.null(code_dir) && code_dir != "None") {
    return(code_dir)
  }
  
  # Try to get the script path when running via Rscript
  args <- commandArgs(trailingOnly = FALSE)
  file_arg_index <- grep("--file=", args)
  if (length(file_arg_index) > 0) {
    # Running via Rscript
    file_arg <- args[file_arg_index]
    script_path <- normalizePath(sub("--file=", "", file_arg))
    return(dirname(script_path))
  } else if (interactive()) {
    # Running interactively in RStudio
    if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
      script_path <- rstudioapi::getActiveDocumentContext()$path
      if (nzchar(script_path)) {
        return(dirname(normalizePath(script_path)))
      } else {
        stop("Cannot determine script directory: No active document found in RStudio.")
      }
    } else {
      stop("Cannot determine script directory: Not running in RStudio or rstudioapi not available.")
    }
  } else {
    # Fallback to current working directory
    script_directory <- getwd()
    return(script_directory)
  }
}

# Detect the script directory
script_directory <- get_script_directory()

# Construct the path to the config.R file
config_path <- file.path(script_directory, "config.R")

# Source the config file dynamically
source(config_path)

######## SET UP ########
#rm(list = ls()) # #to make compatible with R file

#####  IMPORTING RAW HIMS##### 
# Define the range of years to import
year_range <- 2005:2017 

# Specify the prefixes of files you want to import
file_prefixes <- c("Contact_", "HAEntityContact_","HAEntity_", "ContactType_","ContactSource_")


#####  *** IMPORTING AHA DATA##### 
file_name_aha <- c("AHA_2004_2017.csv")

columns_to_keep <- c( 
  "ID", 
  "YEAR", 
  "MNAME", 
  "HOSPN", 
  "MCRNUM",
  # "CBSANAME", 
  "HRRNAME", 
  "HSANAME", 
  "MADMIN", 
  "SYSID", 
  "SYSNAME", 
  "CNTRL", 
  "SERV", 
  "HOSPBD", 
  "BDTOT", 
  "ADMTOT", 
  "IPDTOT", 
  "MCRDC", 
  "MCRIPD", 
  "MCDDC", 
  "MCDIPD", 
  "BIRTHS", 
  "FTEMD", 
  "FTERN", 
  "FTE",
  "LAT",
  "LONG",
  "MLOCADDR", #street address
  "MLOCCITY", #city
  "MLOCZIP" #zip code
) # Replace with your desired column names
# *** IMPORTING AHA<>MCR CROSSWALK ***
file_name_aha_mcr_xwalk <- c("hospital_ownership.dta")

# Note, there were several file options for both the AHA data and the crosswalk. See the exploratory .Rmd file called "aha_crosswalk_comparison.Rmd" for an analysis of all the options, and the final selection, which is reflected in this file.


##### IMPORTING GENDER ASSIGNMENT DATA ##### 
file_name_wgnd2 <- c("wgnd_2.csv")
file_name_f_diminutives <- c("female_diminutives.csv")
file_name_m_diminutives <- c("male_diminutives.csv")
file_path_ssa_directory <- c("/names")
file_name_wgnd2.0 <- c("wgnd2_short.csv")






##### IMPORT ALL ORIGINAL HIMSS
folders <- list.dirs(path = raw_data, full.names = TRUE, recursive = FALSE)

# Loop through each file prefix
for (prefix in file_prefixes) {
  temp_dfs <- list()  # Temporary list to store dataframes for this prefix
  
  # Loop through each year
  for (year in year_range) {
    file_path <- paste0(prefix, year, ".csv")  # Construct the file name
    full_path <- file.path(raw_data, as.character(year), file_path)  # Full file path
    
    #Check if the file exists, import
    if (file.exists(full_path)) {
      #Importing all as character. 
      #There are some differences within year 
      #(e.g. most phone numbers will be ##########, but some will be ###-###-####. 
      #Without converting to character, we see data loss when the mismatched value is not imported 
      #e.g. ###-###-#### would be NA
      df <- read_csv(full_path, show_col_types = FALSE, col_types = cols(.default = col_character())) %>%
        # Add a year value and and make all vars lowercase
        mutate(year = year) %>%  # Add the year as a new column
        rename_with(tolower)
      
      #add a comment
      # In some years the same fields data storage type changed. Dynamically adjust data types for combination: Convert numeric and logical columns to character.
      # df <- df %>%
      #   mutate(across(where(is.numeric), ~if_else(is.na(.), as.character(.), as.character(.)))) %>%
      #   mutate(across(where(is.logical), as.character))
      
      temp_dfs[[length(temp_dfs) + 1]] <- df
    }
  }
  
  # Combine all dataframes, now with consistent data types
  combined_df <- bind_rows(temp_dfs)
  
  # Dynamically assign the combined dataframe to a variable in the global environment
  var_name <- tolower(str_remove(prefix, "_$"))
  assign(var_name, combined_df, envir = .GlobalEnv)
}

# Clean up temporary variables
rm(list = c("combined_df", "df", "temp_dfs","var_name","year", "year_range"))

##### calculate original import misses ######
#In original loop, mismatched col values (within a given year) were being dropped. this section calculates the total impact of that.

# Define the range of years to import
year_range <- 2005:2017 

# Specify the prefixes of files you want to import
file_prefixes <- c("Contact_", "HAEntityContact_", "HAEntity_", "ContactType_", "ContactSource_")

# Initialize a list to store all parsing issues
all_parsing_issues <- list()
# Initialize a variable to count the total number of issues
total_issues_count <- 0

# Loop through each year and file prefix to import files and check for parsing issues
for (year in year_range) {
  for (prefix in file_prefixes) {
    file_path <- paste0(prefix, year, ".csv")  # Construct the file name
    full_path <- file.path(raw_data, as.character(year), file_path)  # Full file path
    
    # Check if the file exists, import it, and collect parsing issues
    if (file.exists(full_path)) {
      # Read the data
      test_df <- read_csv(full_path, show_col_types = TRUE)
      
      # Check for parsing problems
      parsing_issues <- problems(test_df)
      issue_count <- nrow(parsing_issues)  # Count the number of issues
      if (issue_count > 0) {
        # Store parsing issues in the list with the file path as the name
        all_parsing_issues[[paste0(prefix, year)]] <- parsing_issues
        # Add to total issues count
        total_issues_count <- total_issues_count + issue_count
      }
    } else {
      message(paste("File does not exist at:", full_path))
    }
  }
}

# Function to print all parsing issues
print_all_parsing_issues <- function(all_issues) {
  if (length(all_issues) > 0) {
    for (name in names(all_issues)) {
      issue_count <- nrow(all_issues[[name]])
      cat("\nParsing issues for file:", name, " - Total issues:", issue_count, "\n")
      print(all_issues[[name]])
    }
    cat("\nTotal number of parsing issues across all files:", total_issues_count, "\n")
  } else {
    message("No parsing issues detected for any files.")
  }
}

# Print all parsing issues for review
print_all_parsing_issues(all_parsing_issues)


###### clean contact ######
# Update the contact cleanup code to remove extensions (but leave incomplete phone number), and then remove hyphens from other numbers. Imputing phone number tricky, do not do it here
contact <- contact %>% 
  rename(contact_uniqueid = uniqueid) %>% 
  mutate(
    year = as.numeric(year),  # Convert year to numeric
    # Check if phone number is in the format "#######-###" (not "###-###-####") using regex and handle NAs
    has_extension_format = ifelse(!is.na(phone) & grepl("^\\d{6,}-\\d+$", phone), TRUE, FALSE),
    # Extract the digits after the hyphen in the phone column, if it exists
    extracted_extension = ifelse(has_extension_format, sub(".*-(\\d+)$", "\\1", phone), NA),
    # Compare the extracted extension with the extension column and flag matches
    is_correct_extension = ifelse(!is.na(extracted_extension) & extracted_extension == ext, TRUE, FALSE),
    # If the format matches and the extension is correct, clean up the phone number by removing the hyphen and extension part
    phone = ifelse(is_correct_extension, sub("-(\\d+)$", "", phone), phone),
    # Standardize phone number by removing all non-digit characters (e.g., hyphens, parentheses)
    phone = gsub("[^0-9]", "", phone)
  ) %>%
  # Remove temporary columns
  select(-has_extension_format, -extracted_extension, -is_correct_extension)


##### clean contactsource ##### 
contactsource <- contactsource %>% 
  rename(title_standardized = name) %>% 
  mutate(year = as.numeric(year))

#The contact source files map the numeric job id (e.g. 219) to standardized job titles (e.g. CEO). Each contact has their own free text title (e.g. "CEO and Interim CFO") but they must always map to standardized job ids (e.g. 219 and 221).
#However, there's a new crosswalk every year, and sometimes the titles change slightly. e.g. CEO became CEO: Chief Executive Officer in 2015. 
#To standardize further, take the max title for a given id. 
#Reviewed every entry to confirm that the id never switched. ie. never went from "Patient Care coordinator" to "CEO". It did not. Only changes were small, and defintely described the same job role. 

contactsource <- contactsource %>% 
  group_by(contactsourceid) %>%
  filter(year == max(year)) %>%
  ungroup() %>% 
  select(-year)

#Create is_c_suite flag
# IDs for c suite:219 - CEO: Chief Executive Officer, 221 - CFO: Chief Financial Officer, 217 - Chief Compliance Officer, 294 - Chief Experience/Patient Engagement Officer, 279 - Chief Medical Information Officer, 282 - Chief Medical Officer, 214 - Chief Nursing Head, 220 - CIO: Chief Information Officer, 289 - CNIS: Chief Nursing Informatics Officer, 224 - COO: Chief Operating Officer, 229 - CSIO/IT Security Officer, 

contactsource <- contactsource %>%
  mutate(c_suite = ifelse(contactsourceid %in% c(219, 221, 217, 294, 279, 282, 214, 220, 289, 224, 229), 1, 0))

#Note: some contacts have an additional entry that represents their role on the "Steering committee". These entries in early years seem to not be associated with a contactsourceid, leading to null job titles for those obs. e.g. contact id 715247, they have three entries in 2005. 2 have the same contactsourceid (one for the parent hosp and one for the child), and then 1 entry for his role on the steering committe, with no contactsourceid.

##### clean contacttype ######
contacttype <- contacttype %>% 
  rename(job_category = name) %>% 
  mutate(year = as.numeric(year))

##### clean haentity ######
haentity <- haentity %>% 
  rename_with(tolower) %>% 
  rename(himss_entityid = haentityid,
         entity_type = haentitytype,
         entity_name = name,
         entity_address = address1,
         entity_city = city,
         entity_state = state,
         entity_zip = zip,
         entity_profitstatus = profitstatus,
         entity_parentid = parentid,
         entity_uniqueid = uniqueid,
         entity_bedsize = nofbeds,
         entity_phone = phone,
         entity_email = email
  ) %>% 
  mutate(entity_type = case_when(
    entity_type == "IDS" ~ "IDS/RHA",
    entity_type == "Independent Hospital" ~ "Single Hospital Health System", 
    TRUE ~ entity_type  # Keep all other values as they are
  ),
  year = as.character(year)
  )

haentity <- haentity %>%
  mutate(system_id = case_when(
    himss_entityid %in% entity_parentid ~ entity_uniqueid,
    entity_parentid %in% himss_entityid ~ 
      entity_uniqueid[match(entity_parentid, himss_entityid)],
    .default = NA
  ),
  # year = as.numeric(year),
  entity_phone = gsub("[^0-9]", "", entity_phone),
  fax = gsub("[^0-9]", "", fax) 
  )

#Remove non-US entities
haentity <- haentity %>% 
  filter(!grepl("[A-Za-z]", entity_zip) & 
           !str_detect(entity_state, "AB|BC|PE|NB|NL|NS|MB|SK|PR"))

# Clean names and addresses to remove capitalization and punctuation. Helps with AHA fuzzyjoin, and generally better to join with
haentity <- haentity %>% 
  mutate(
    # Convert to UTF-8
    entity_zip = stri_trans_general(entity_zip, "latin-ascii"),
    cbsa = stri_trans_general(cbsa, "latin-ascii"),
    entity_address = stri_trans_general(entity_address, "latin-ascii") %>%
      str_replace_all("-", " ") %>%  # Replace hyphens with a space
      str_remove_all("[^[:alnum:],.\\s]") %>%  # Remove other punctuation except commas, periods, and spaces
      str_to_lower() %>%
      str_squish(),  # Remove extra spaces
    entity_name = stri_trans_general(entity_name, "latin-ascii") %>%
      str_replace_all("-", " ") %>%  # Replace hyphens with a space
      str_remove_all("[^[:alnum:],.\\s]") %>%  # Remove other punctuation except commas, periods, and spaces
      str_to_lower() %>%
      str_squish(),  # Remove extra spaces
    # Extract first five digits of the zip code
    entity_zip_five = str_extract(entity_zip, "^\\d{5}")
  )

#feather file has corruption issues, due to presence of ~ tilde. identified cbsa and website as the problem fields

#Random Qa
haentity %>% filter(system_id ==39438)
#Interestingly, it seems the rebuild has resulted in a more complete dataset. HC Watkins was one of the systems flagged as child-no-parent, but both show up correctly. For some reason the parent hospital is missing in 2005 in the healtchare_execs file, but both records are here just fine. Not sure why

#### clean haentity contact ####
haentitycontact <- haentitycontact %>% 
  rename(himss_entityid = haentityid) %>% 
  mutate(year = as.numeric(year))

#### create combined contacts table #### 
combined_contacts <- haentitycontact %>%
  left_join(contactsource, by = c("hacontactsourceid" = "contactsourceid")) %>% 
  left_join(contact,by = c("contactid","year")) %>% 
  left_join(contacttype, by = c("typeid", "year")) %>% 
  mutate(surveyid = coalesce(surveyid.y, surveyid.x)) %>% 
  select(-surveyid.y, -surveyid.x)

#### load AHA data and crosswalk ####
# See AHA Crosswalk Comparison Code for a deeper dive into the AHA data, the completeness of the Medicare Number in the HIMSS data, and how this crosswalk was selected.

#### Pull in AHA<>MCR Crosswalk 
# set file path using manual input at beginning of script
file_path <- paste0(supplemental_data,"/",file_name_aha_mcr_xwalk)

aha_xwalk <- read_dta(file_path) %>%
  rename(ahanumber = ahaid,
         medicarenumber = mcrnum) %>%
  mutate(year = as.character(year)) %>% 
  select(ahanumber, medicarenumber, year) %>%
  unique()

aha_xwalk <- aha_xwalk %>% 
  group_by(year, medicarenumber) %>%
  #sometimes 2 AHA numbers per MCR number, select one
  summarize(ahanumber = max(ahanumber, na.rm = TRUE), .groups = 'drop')

#### load AHA data

# set file path using manual input at beginning of script
file_path <- paste0(supplemental_data,"/",file_name_aha)
#import the CSV
aha_data <- read_csv(file_path, col_types = cols(.default = col_character()))

# Use problems() function to identify parsing issues
parsing_issues <- problems(aha_data)

# Check if any parsing issues were detected
if (nrow(parsing_issues) > 0) {
  print(parsing_issues)
} else {
  print("No parsing problems detected.")
}

aha_data <- aha_data %>%
  select(all_of(columns_to_keep)) %>% 
  rename_all(tolower) %>% 
  rename(ahanumber = id,
         latitude_aha = lat,
         longitude_aha = long) %>% 
  mutate(
    # Convert to UTF-8
    mloczip = stri_trans_general(mloczip, "latin-ascii"),
    # cbsaname = stri_trans_general(cbsaname, "latin-ascii"),
    mlocaddr = stri_trans_general(mlocaddr, "latin-ascii") %>% 
      str_replace_all("-", " ") %>%  # Replace hyphens with a space
      str_remove_all("[^[:alnum:],.\\s]") %>%  # Remove other punctuation except commas, periods, and spaces
      str_to_lower() %>%
      str_squish(),  # Remove extra spaces
    mname = stri_trans_general(mname, "latin-ascii") %>% 
      str_replace_all("-", " ") %>%  # Replace hyphens with a space
      str_remove_all("[^[:alnum:],.\\s]") %>%  # Remove other punctuation except commas, periods, and spaces
      str_to_lower() %>%
      str_squish(),  # Remove extra spaces
    # Extract first five digits of the zip code
    mloczip_five = str_extract(mloczip, "^\\d{5}")
  )

#### merge AHA data to HIMSS #####
#### STEP 1: Use crosswalk to pull in AHA numbers for hospitals ####
# Have to separate NAs, they mess up the join
# Create two NA datasets, 1 for hospitals 1 for all others
haentity_isna_hosp <- haentity %>%
  filter(is.na(medicarenumber)) %>% 
  filter(entity_type == "Hospital")

haentity_isna_not_hosp <- haentity %>%
  filter(is.na(medicarenumber))%>% 
  filter(entity_type != "Hospital")

# Handle the rows with non-NA medicarenumber
haentity_not_na <- haentity %>%
  filter(!is.na(medicarenumber)) %>%
  left_join(aha_xwalk, by = c("medicarenumber", "year")) %>%
  mutate(ahanumber = coalesce(ahanumber.y, ahanumber.x)) %>%
  select(-ahanumber.x, -ahanumber.y)

# Join the aha_data for rows with non-NA ahanumber
haentity_not_na <- haentity_not_na %>%
  left_join(aha_data, by = c("ahanumber", "year"))

#check if any failed to join
haentity_not_na_failed <- haentity_not_na %>%
  anti_join(aha_data, by = c("ahanumber", "year"))
# empty dataframe, so everything joined successfully

#### STEP 2: Fuzzy Join for entries that don't have Medicare Number ####

#First, fuzzy match
haentity_isna_hosp <- haentity_isna_hosp %>%
  fuzzy_left_join(
    aha_data,
    by = c("entity_zip_five" = "mloczip_five", "year" = "year", "entity_address" = "mlocaddr", "entity_name" = "mname"),
    match_fun = list(
      `==`,  # Exact match for zip
      `==`,  # Exact match for year
      ~ stringdist::stringdist(.x, .y) <= 5,  # Fuzzy match for address
      ~ stringdist::stringdist(.x, .y) <= 5   # Fuzzy match for hospital name
    )
  ) %>%
  mutate(
    # Calculate string distance for names and addresses
    name_distance = stringdist::stringdist(entity_name, mname),
    address_distance = stringdist::stringdist(entity_address, mlocaddr)
  )

#The stringdist::stringdist(.x, .y) <= 2 criterion uses a string distance metric to determine how similar two strings are. Specifically, it measures how many operations (like insertions, deletions, or substitutions of characters) are needed to transform one string into another.This is using the Levenshtein distance. 

# Second, ensure that no particular entry is matched more than once. Rank between fuzzy matches and choose closest
haentity_isna_hosp <- haentity_isna_hosp %>%
  group_by(entity_name, entity_address, entity_zip_five, year.x) %>%
  slice_min(order_by = name_distance + address_distance, n = 1) %>%
  ungroup()
# no change, so there weren't multiple matches

#review the matches
fuzzy_match_review <- haentity_isna_hosp %>% 
  filter(!is.na(mname)) %>% 
  select(entity_name, mname,entity_address, entity_city,entity_zip, entity_zip_five, mlocaddr,mloccity, mloczip, mloczip_five, year.x, year.y, hospn, ahanumber.x,ahanumber.y,medicarenumber,mcrnum,name_distance,address_distance)

# Combine the datasets back together and clean
haentity <- bind_rows(haentity_not_na, haentity_isna_hosp,haentity_isna_not_hosp) %>%
  rename(aha_entity_name = mname,
         aha_entity_address = mlocaddr,
         aha_entity_zip = mloczip,
         aha_entity_zip_five = mloczip_five) %>% 
  mutate(
    # Combine year columns (should be identical)
    year = coalesce(year,year.x, year.y),
    medicarenumber = coalesce(medicarenumber,mcrnum),
    # Keep ahanumber.y if ahanumber.x is NA or blank
    ahanumber = coalesce(ahanumber,ahanumber.y, ahanumber.x)) %>%
  # Remove the original columns after combining
  select(-year.x, -year.y, -ahanumber.x, -ahanumber.y,-mcrnum, -name_distance, -address_distance)

#recheck completeness
xwalk_completeness <- haentity %>% 
  # Single Hospital Health System and IDS/RHA don't have this field completed, ever
  # filter(entity_type %in% c("Hospital", "Single Hospital Health System", "IDS/RHA")) %>%
  filter(entity_type =="Hospital") %>%
  group_by(year) %>%
  summarize(
    total_entries = n(),
    aha_num_na_count = sum(is.na(ahanumber)),
    mcr_num_na_count = sum(is.na(medicarenumber))
  ) %>% 
  group_by(year) %>% 
  summarize (
    aha_num_complete_percent = (1 - (aha_num_na_count / total_entries)) * 100,
    mcr_num_complete_percent = (1 - (mcr_num_na_count / total_entries)) * 100
  )

#### Create Ambar dataset ####
#Create downscoped entities dataset that includes all records from AHA data, regardles sof match

# **Step 1: Downselect haentity to specified columns and save as haentity_reduced**
haentity_reduced <- haentity %>%
  filter(entity_type == "Hospital") %>% 
  select(
    himss_entityid,
    entity_uniqueid,
    system_id,
    ahaid = ahanumber,       # Rename 'ahanumber' to 'ahaid'
    hospn,
    medicarenumber,
    year
  ) 

# **Step 2: Identify unmatched AHA records**
unmatched_aha <- aha_data %>%
  # Use anti_join with correct column mappings
  anti_join(haentity_reduced, by = c("ahanumber" = "ahaid", "year"))

# **Prepare unmatched AHA records**
unmatched_aha_prepared <- unmatched_aha %>%
  select(
    ahaid = ahanumber,      # Rename 'ahanumber' to 'ahaid'
    medicarenumber = mcrnum,
    year
  ) %>%
  mutate(
    himss_entityid = NA_character_,
    entity_uniqueid = NA_character_,
    system_id = NA_character_
  ) %>%
  select(
    himss_entityid,
    entity_uniqueid,
    system_id,
    ahaid,
    medicarenumber,
    year
  )

# **Step 3: Combine haentity_reduced with unmatched AHA records to create full_himss_aha_entities**
full_himss_aha_entities <- bind_rows(haentity_reduced, unmatched_aha_prepared)

# **Step 4: Verify the results**
total_records <- nrow(full_himss_aha_entities)
matched_records <- nrow(haentity_reduced)
unmatched_records <- nrow(unmatched_aha_prepared)

message("Total records in final output: ", total_records)
message("Matched records from haentity: ", matched_records)
message("Unmatched AHA records added: ", unmatched_records)

# **Step 5: Export full_himss_aha_entities to a .dta file**

# Set the file path for the output file
output_file_path <- file.path(derived_data, "full_himss_aha_entities.dta")

# Export to .dta file
write_dta(full_himss_aha_entities, output_file_path)


#### fix swapped names ####
#some contacts have an entry where their first and last name has been swapped

# Step 1: Create dataset w extra 2 columns that swap firstname for lastname, and last for first
swapped_contacts <- combined_contacts %>%
  filter(year>2008, status %in% c("Active","ACTIVE")) %>%
  select(year, contact_uniqueid, firstname, lastname) %>%
  mutate(
    firstname = str_to_lower(firstname),
    lastname = str_to_lower(lastname),
    swapped_firstname = lastname,
    swapped_lastname = firstname
  ) %>%
  group_by(contact_uniqueid, firstname, lastname, swapped_firstname, swapped_lastname) %>%
  summarise(year_count = n_distinct(year), .groups = 'drop') %>% 
  select(contact_uniqueid, swapped_firstname, swapped_lastname, year_count) %>% 
  unique()

# Step 2: Join the original dataset with the swapped dataset on contact_uniqueid. Then, filter on instances where the firstname in the original dataset matches the swapped lastname in the swapped dataset.
swapped_cases <- combined_contacts %>%
  filter(year>2008, status %in% c("Active","ACTIVE")) %>%
  # select(year, contact_uniqueid, firstname, lastname) %>%
  select(contact_uniqueid, firstname, lastname) %>%
  unique() %>% 
  mutate(
    firstname_l = str_to_lower(firstname),
    lastname_l = str_to_lower(lastname)) %>% 
  left_join(swapped_contacts, by = "contact_uniqueid") %>%
  filter(
    firstname_l == swapped_firstname & lastname_l == swapped_lastname) %>%
  group_by(contact_uniqueid) %>%
  #now we have to pick which is wrong, since the result here will include two entries for each contact with both possible orderings of the name. Mary Smith, and Smith Mary. Select the least frequently occurring order as the "incorrect" order.
  slice_min(year_count, n = 1, with_ties = FALSE) %>%
  ungroup() %>% 
  select(contact_uniqueid, swapped_firstname, swapped_lastname) %>%
  #create fields where the names are flipped back correctly
  mutate(fix_firstname = swapped_lastname,
         fix_lastname = swapped_firstname,
         is_swapped = 1) %>%
  unique()

# Step 3: Correct the swapped names in the original dataset
combined_contacts_corrected <- combined_contacts %>%
  mutate(
    firstname_l = str_to_lower(firstname),
    lastname_l = str_to_lower(lastname)
  ) %>%
  left_join(
    swapped_cases,
    by = c("contact_uniqueid", "firstname_l" = "swapped_firstname", "lastname_l" = "swapped_lastname")
  ) %>%
  mutate(
    firstname = if_else(!is.na(is_swapped), fix_firstname, firstname),
    lastname = if_else(!is.na(is_swapped), fix_lastname, lastname)
  ) %>%
  select(-fix_firstname, -fix_lastname, -firstname_l, -lastname_l,-is_swapped)


##### gender based on salutation ##### 
# consider using 80% frequency threshold for salutation frequency, since that's what katherine does later for metaphones
#add gender to combined contacts corrected
# uses modified assign_genders.Rmd code

#First, fix names to lower case and remove punctuation
combined_contacts_corrected <- combined_contacts_corrected %>%
  mutate(full_name = str_to_lower(str_replace_all(paste0(firstname, lastname),
                                                  "[[:punct:]\\s]", "")))
#confirm salutation options
combined_contacts_corrected %>%
  distinct(salutation)

# How often is salutation consistent?

# Calculate the count and proportion of each salutation per contact
salutation_distribution <- combined_contacts_corrected %>%
  group_by(contact_uniqueid, salutation) %>%
  summarize(count = n(), .groups = 'drop') %>%
  group_by(contact_uniqueid) %>%
  mutate(total = sum(count, na.rm = TRUE),
         proportion = count / total) %>%
  ungroup()

# Find the maximum proportion per contact
max_proportion_per_contact <- salutation_distribution %>%
  group_by(contact_uniqueid) %>%
  summarize(max_proportion = max(proportion), .groups = 'drop')

# Create a histogram with value labels - REMOVED

#Only a small proportion have even splits, so this method is reasonable

# Calculate the most frequent salutation per contact
gender_saluations <- combined_contacts_corrected %>%
  select(contact_uniqueid, firstname, lastname, salutation) %>% 
  group_by(contact_uniqueid, salutation) %>%
  summarize(count = n(), .groups = 'drop') %>%
  arrange(contact_uniqueid, desc(count)) %>%
  group_by(contact_uniqueid) %>%
  slice(1) %>%
  # Assign gender based on whether the salutation contains "mr" or "ms"
  mutate(gender = case_when(
    str_detect(str_to_lower(salutation), "mr") ~ "M",
    str_detect(str_to_lower(salutation), "ms") ~ "F",
    TRUE ~ "Unknown"
  )) %>%
  select(contact_uniqueid, gender)

# Join the gender information back to the original dataset
combined_contacts_corrected_gender <- combined_contacts_corrected %>%
  left_join(gender_saluations, by = "contact_uniqueid")

# How complete is the gender assignment?
# Calculate the count and percentage of each gender category
gender_saluations %>%
  group_by(gender) %>% 
  # Count the number of unique contact IDs per gender
  summarize(unique_contacts = n_distinct(contact_uniqueid)) %>%
  # Calculate the total number of unique contacts
  mutate(total_contacts = sum(unique_contacts)) %>%
  # Calculate the percentage for each gender
  mutate(percentage = (unique_contacts / total_contacts) * 100)

# 30% of contacts still don't have gender. For them, either salutation was missing, or they had a title like Doctor.

#####  gender based on first name ####
############################  PART 1 - SALUTATIONS ############################
# use salutations from data that are consistent at the person level and at the level of the firstname       

# Find all first name + salutation pairs which are 100% consistent (ie Alex may show up with both Mr and Ms, it is not consistent)
case1 <- combined_contacts_corrected_gender %>%
  filter(!is.na(firstname)) %>% 
  group_by(full_name) %>%
  filter(n_distinct(salutation) == 1 & !any(is.na(salutation))) %>%
  ungroup() %>%
  select(full_name, firstname, lastname, salutation) %>%
  distinct(firstname, salutation) %>%
  group_by(firstname) %>%
  mutate(count = n()) %>%
  ungroup()

# Create a set of confirmed names, and remove non-gender salutations
# confirmed_1 <- case1 %>% # confirmed 10,301 genders
#     filter(count==1 & (salutation != "Sr." & salutation != "Fr."))

confirmed_1 <- case1 %>% 
  filter(count == 1 & salutation %in% c("Mr", "Ms"))

# List of confirmed first names from confirmed_1, for filtering 
confirmed_names <- confirmed_1$firstname

# Create df of names which we aren't certain of the gender, since the salutations conflicted
remaining_1 <- combined_contacts_corrected_gender %>%
  filter(!is.na(firstname)) %>% 
  filter(!firstname %in% confirmed_names) # leaving 5787 genders


###############################  PART 2 - WGND ###############################
# Use World Gender-Name Dictionary (WGND) to determine gender
# Source: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/YPRQH8

# set file path using manual input at beginning of script
file_path <- paste0(nicknames_dictionaries,"/",file_name_wgnd2)
#import the CSV
wgnd2 <- read_csv(file_path)

us <- wgnd2 %>% 
  filter(code == "US")

unique_names <- remaining_1 %>%
  distinct(firstname) %>%
  mutate(name = tolower(firstname))

# Take the distinct names from our contacts with unknown gender, and join to the US WGND dictionary
merged_df <- left_join(unique_names, us, by = "name")
remaining_2 <- merged_df %>%
  filter(is.na(gender))

# Create an additional set of confirmed name to gender mappings, based on this join. Combines confirmed_1 and the confirmed name to gender mappings from the wgnd.
confirmed_2 <- bind_rows(confirmed_1, merged_df %>%filter(!is.na(gender))) %>%
  mutate(gender = ifelse(is.na(gender) & salutation == "Mr.", "M",
                         ifelse(is.na(gender) & salutation == "Ms.", "F", 
                                ifelse(!is.na(gender), gender, NA))))

############################  PART 3 - dictionaries ############################
### using previous gendered nicknames dictionaries to est. gender            ###
# Source: https://www.notion.so/one_id_m_names_final-R-07ae80131afb45b7adc31f25896da97e?pvs=4#8bc5be5eca194374bae6925963b19e7

file_path <- paste0(nicknames_dictionaries,"/",file_name_f_diminutives)
female <- read_csv(file_path, col_names = FALSE)

file_path <- paste0(nicknames_dictionaries,"/",file_name_m_diminutives)
male <- read_csv(file_path, col_names = FALSE)

female_df <- confirmed_2 %>%
  filter(gender == "F")
male_df <- confirmed_2 %>%
  filter(gender == "M")

# Flatten the data frames of nicknames (diminutives) into matrices, then concatenate these with the list of first names we have confirmed genders so far (confirmed_2) to create vectors of all known female and male names.
female_vector <- c(as.matrix(female), female_df$firstname)
male_vector <- c(as.matrix(male), male_df$firstname)


# Define a function to determine the gender of a name or a set of names.
# The function takes a vector of names (name_split) as input and checks:
# 1. If all names are found in the female vector and none in the male vector, it returns "F" (female).
# 2. If all names are found in the male vector and none in the female vector, it returns "M" (male).
# 3. If names are found in both vectors or do not fully match either, it returns NA to indicate ambiguity.

determine_gender <- function(name_split) {
  if (all(name_split %in% female_vector) & !any(name_split %in% male_vector)) {
    return("F")
  } else if (all(name_split %in% male_vector) & !any(name_split %in% female_vector)) {
    return("M")
  } else {
    return(NA)
  }
}

# Apply the function to each row and create a new 'gender' column
case2 <- remaining_2 %>%
  mutate(firstname_split = strsplit(firstname, "[ -]")) %>%
  distinct(firstname, firstname_split) %>%
  mutate(gender = map_chr(firstname_split, determine_gender)) 

#Now we have successfully mapped genders for a subset of the remaining unknown gender names (remaining_2). We can take those we mapped and make a new confirmed_3, and take those which still need to be mapped and make a remaining_3.

remaining_3 <- case2 %>% #2220 left
  filter(is.na(gender)) %>%
  distinct(firstname)
confirmed_3 <- bind_rows(confirmed_2, subset(case2, !is.na(gender)))

###############################$  PART 4 - SSA ###############################
### uses ssa files to establish gender                                     ###

ssa_directory <- paste0(nicknames_dictionaries, file_path_ssa_directory)

# Get a list of all the text files in the directory
file_list <- list.files(ssa_directory, pattern = "yob(19[3-9][0-9]|1990)\\.txt", 
                        full.names = TRUE)

# Function to read a single file and add a year column
read_file <- function(file) {
  # Extract the year from the file name
  year <- str_extract(basename(file), "\\d{4}")
  
  # Read the file
  df <- read.csv(file, header = FALSE, stringsAsFactors = FALSE)
  
  # Rename the columns
  colnames(df) <- c("name", "sex", "count")
  
  # Add the year column
  df <- df %>%
    mutate(year = as.integer(year))
  
  return(df)
}

# Read all files and combine into a single data frame
all_data <- lapply(file_list, read_file) %>%
  bind_rows()

# pt a - get perfect matches
temp_df <- all_data %>%
  group_by(name) %>%
  filter(n_distinct(sex) == 1)

ssa_female <- temp_df %>%
  filter(sex == "F") %>%
  distinct(name)

ssa_male <- temp_df %>%
  filter(sex == "M") %>%
  distinct(name)

# pt b - get matches with predominately the same gender
# first, identify names that are associated with both genders
collapsed_df <- all_data %>%
  group_by(name) %>%
  filter(n_distinct(sex)>1) %>%
  ungroup() %>%
  group_by(name, sex) %>%
  summarize(total_count = sum(count), .groups = 'drop') %>%
  ungroup()

# for each of these names, calculate the % of the time it's F vs M. If it's usually female or male, classify as such, even splits are unknown.
reshaped_df <- collapsed_df %>%
  pivot_wider(names_from = sex, values_from = total_count, values_fill = 
                list(total_count = 0)) %>%
  mutate(ratio_female_male = F / M,
         gender = ifelse(ratio_female_male >= 6, "Female",
                         ifelse(ratio_female_male <= 1/6, "Male", "Unknown")))

ssa_female2 <- reshaped_df %>%
  filter(gender == "Female") %>%
  distinct(name)

ssa_male2 <- reshaped_df %>%
  filter(gender == "Male") %>%
  distinct(name)

### perfect match ###
ssa_female_vector <- ssa_female$name
ssa_male_vector <- ssa_male$name

ssa_determine_gender <- function(name_split) {
  if (all(name_split %in% ssa_female_vector) & !any(name_split %in% ssa_male_vector)) {
    return("F")
  } else if (all(name_split %in% ssa_male_vector) & !any(name_split %in% ssa_female_vector)) {
    return("M")
  } else {
    return(NA)
  }
}

# Apply the function to each row and create a new 'gender' column
case3 <- remaining_3 %>%
  mutate(firstname_split = strsplit(firstname, "[ -]")) %>%
  distinct(firstname, firstname_split) %>%
  mutate(gender = map_chr(firstname_split, ssa_determine_gender)) 

remaining_4 <- case3 %>% #2128 left
  filter(is.na(gender)) %>%
  distinct(firstname)

confirmed_4 <- bind_rows(confirmed_3, subset(case3, !is.na(gender)))

### likely match ###
ssa_female_vector2 <- ssa_female2$name
ssa_male_vector2 <- ssa_male2$name

ssa_determine_gender2 <- function(name_split) {
  if (all(name_split %in% ssa_female_vector2) & !any(name_split %in% ssa_male_vector2)) {
    return("F")
  } else if (all(name_split %in% ssa_male_vector2) & !any(name_split %in% ssa_female_vector2)) {
    return("M")
  } else {
    return(NA)
  }
}

# Apply the function to each row and create a new 'gender' column
case4 <- remaining_4 %>%
  mutate(firstname_split = strsplit(firstname, "[ -]")) %>%
  distinct(firstname, firstname_split) %>%
  mutate(gender = map_chr(firstname_split, ssa_determine_gender2)) 

remaining_5 <- case4 %>% #2073 left
  filter(is.na(gender)) %>%
  distinct(firstname) %>%
  mutate(name = tolower(firstname))

confirmed_5 <- bind_rows(confirmed_4, subset(case4, !is.na(gender)))

##Put it all together
file_path <- paste0(nicknames_dictionaries,"/",file_name_wgnd2.0)
wgnd2.0 <- read_csv(file_path)

name_gender_count <- wgnd2.0 %>%
  group_by(name) %>%
  summarize(unique_genders = n_distinct(gender), .groups = 'drop')

consistent_names <- name_gender_count %>%
  filter(unique_genders == 1) %>%
  select(name)

#combine the name to gender mappings from wgnd2 and wgnd2.0
final_names <- wgnd2 %>%
  inner_join(consistent_names, by = "name") %>%
  distinct(name, gender)

case5 <- left_join(remaining_5, final_names, by = "name")
remaining_6 <- case5 %>%
  filter(is.na(gender))

#compile all name to gender mappings from this entire chunk of code
confirmed_6<- bind_rows(confirmed_5, subset(case5, !is.na(gender)))

final_confirmed <- confirmed_6 %>%
  select(firstname, gender)

write.csv(final_confirmed, 
          file = paste0(nicknames_dictionaries, "/gender_test.csv"), 
          row.names = FALSE)

# Now use this compiled set of name to gender mappings to map the unknown genders in our dataset (ie those remaining after we infer gender based on salutation)
combined_contacts_corrected_gender <- combined_contacts_corrected_gender %>%
  left_join(final_confirmed, by = "firstname") %>% 
  mutate(gender = ifelse(gender.x != "Unknown", gender.x, gender.y)) %>%
  select(-gender.x, -gender.y)  # Optionally remove the old gender columns

#example of issue: unique id 542074

#### final join to create table ####

# This outputs a fully compiled HIMSS + AHA dataset, with an obs for each entity + year + role + contact
entity_contacts <- combined_contacts_corrected_gender %>% 
  mutate(year = as.character(year)) %>% 
  left_join(haentity, by = c("himss_entityid", "year"))
#This next feeds into Katherine's scripts, which drop non-Active role types, and filter out contacts with low quality unique ids

# This outputs a fully compiled HIMSS + AHA dataset, with an obs for each entity + year + role, ONLY for non-ACTIVE jobs. This is the complement to the dataset that Katherine ultimately creates. 

entity_roles <- entity_contacts %>% 
  mutate(status = str_trim(status)) %>%  # Remove leading/trailing spaces
  filter(!status %in% c("ACTIVE","Active","Contact Corporate","Ontario Contact","RHA Level","***VALIDATION NEEDED***")) %>% 
  filter(is.na(firstname)) %>%  #There's 16 contacts who seem to be incorrectly labelled "Not Reported"
  select(-contactid, -salutation,-firstname, -middleinitial,-lastname, -title, -phone, -ext,-email,-credentials, -contact_uniqueid,-full_name, -gender) #remove contact fields


##### export to dropbox #####
# Export compiled data set
file_name <- "himss_entities_contacts_0517_v1"
file_path_feather <- paste0(derived_data,"/",file_name,".feather")
write_feather(entity_contacts, file_path_feather)

haentity_path <- paste0(derived_data,"/auxiliary/haentity.feather")
write_feather(haentity, haentity_path)
