#######  Config script for hospital ceo's project ####### 

#######  load libraries
library(tidyverse)
library(readr)
library(readxl)
library(dplyr)
library(tools)
library(stringr)
library(haven) # to read new .dta files
library(foreign) #to read old .dta files
library(wordcloud)
library(feather)
library(tibble)
library(jsonlite) # to find the dropbox directory

#######  Manual Inputs - if we change the folder structure, this needs to be updated
# Define the project folder name
project_folder <- "hospital_ceos"
#Define sub-folder structure
data <- "/_data"
code <- "/_code"
raw_files <- "/raw"
derived_files <-  "/derived"
supplemental_files <- "/supplemental"

####### Now, automatically set-up all file paths for the project.

# Step 1: Detect the current user's home directory
home_directory <- Sys.getenv("HOME")

# Step 2: Detect Dropbox folder by searching for any folder containing "Dropbox" and excluding hidden folders
detect_dropbox_path <- function(home_directory) {
  # List all directories in the home directory (excluding hidden ones)
  potential_paths <- list.dirs(home_directory, full.names = TRUE, recursive = FALSE)
  
  # Filter directories that contain "Dropbox" (case insensitive) and are not hidden
  dropbox_paths <- potential_paths[grepl("(?i)dropbox", potential_paths) & !grepl("/\\.", potential_paths)]
  
  # If at least one Dropbox path is found, return it; otherwise, return NULL
  if (length(dropbox_paths) > 0) {
    return(dropbox_paths[1])  # Return the first visible Dropbox match
  } else {
    return(NULL)
  }
}

# Run the Dropbox detection function
user_directory <- detect_dropbox_path(home_directory)

if (is.null(user_directory)) {
  stop("Dropbox directory could not be detected. Please check your Dropbox installation.")
} else {
  message(paste("Dropbox directory detected at:", user_directory))
}

# Step 3: Automatically detect intermediate folder between Dropbox and project folder
detect_project_path <- function(dropbox_path, project_folder) {
  # List all subdirectories within the Dropbox folder
  subdirectories <- list.dirs(dropbox_path, full.names = TRUE, recursive = FALSE)
  
  # Search for the project folder within each subdirectory
  for (subdir in subdirectories) {
    possible_project_path <- file.path(subdir, project_folder)
    if (dir.exists(possible_project_path)) {
      return(possible_project_path)  # Return the path if found
    }
  }
  
  # If the project folder is directly in the Dropbox path, return that
  direct_project_path <- file.path(dropbox_path, project_folder)
  if (dir.exists(direct_project_path)) {
    return(direct_project_path)
  }
  
  # Return NULL if no valid project path is found
  return(NULL)
}


# Run the project path detection function
project_directory <- detect_project_path(user_directory, project_folder)

if (is.null(project_directory)) {
  stop("Project directory could not be detected. Please check your folder structure.")
} else {
  message(paste("Project directory detected at:", project_directory))
}

# Step 4: Set paths within the project folder
#data_path <- file.path(project_directory, "derived", "data.dta")

raw_data <- paste0(project_directory,data,raw_files)
derived_data <- paste0(project_directory,data,derived_files)
supplemental_data <- paste0(project_directory,data,supplemental_files)

# Export only necessary variables for use in other scripts
# Clean up the environment by removing functions that are no longer needed
rm(detect_dropbox_path, 
   detect_project_path, 
   home_directory, 
   user_directory, 
   potential_paths, 
   dropbox_paths,
   code,
   data, 
   derived_files, 
   project_folder, 
   raw_files, 
   supplemental_files)
