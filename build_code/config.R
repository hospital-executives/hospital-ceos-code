#######  Config script for hospital ceo's project ####### 

#######  load libraries
library(tidyverse)
library(readr)
library(readxl)
library(tools)
library(haven) # to read new .dta files
library(foreign) #to read old .dta files
library(wordcloud)
library(feather)
library(tibble)
library(jsonlite) # to find the dropbox directory
library(stringi) # to fix encoding for AHA data
library(fuzzyjoin) # for AHA data fuzzy join
library(arrow) # for gender assignment code
library(stringdist) # for gender assignment code
library(data.table) # for gender assignment code
library(phonics) # for gender assignment code
library(scales) # improved plotting
library(knitr)
library(writexl)
library(stringr)

# Define required packages
required_packages <- c(
  "tidyverse",
  "readr",
  "readxl",
  "tools",
  "haven",
  "foreign",
  "wordcloud",
  "feather",
  "tibble",
  "jsonlite",
  "stringi",
  "fuzzyjoin",
  "arrow",
  "stringdist",
  "data.table",
  "phonics",
  "scales",
  "knitr",
  "writxl",
  "stringr"
)

#######  Manual Inputs - if we change the folder structure, this needs to be updated
# Define the project folder name
project_folder <- "hospital_ceos"
#Define sub-folder structure
data <- "/_data"
code <- "/_code"
raw_files <- "/raw"
derived_files <-  "/derived"
auxiliary_files <- "/derived/auxiliary"
supplemental_files <- "/supplemental"
nicknames_dictionaries <- "/nicknames dictionaries"

# Function to check and install missing packages
install_if_missing <- function(packages) {
  new_packages <- packages[!(packages %in% installed.packages()[, "Package"])]
  if (length(new_packages)) {
    install.packages(new_packages, dependencies = TRUE)
  }
  invisible(lapply(packages, library, character.only = TRUE))
}

# Install and load required packages
install_if_missing(required_packages)


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
code <- paste0(project_directory,code)
data_file_path <- paste0(project_directory,data,"/") #specifically created to align with Katherine's naming conventions
raw_data <- paste0(project_directory,data,raw_files)
derived_data <- paste0(project_directory,data,derived_files)
supplemental_data <- paste0(project_directory,data,supplemental_files)
auxiliary_data <- paste0(project_directory,data,auxiliary_files)
nicknames_dictionaries <- paste0(project_directory,data,nicknames_dictionaries)


# Step 5: Create .do file so that Stata users can configure their scripts
# Since config.R is in the GitHub code directory, we can get its directory
# Function to get the script directory
# get_script_directory <- function() {
#   # Try to get the script path when running via Rscript
#   args <- commandArgs(trailingOnly = FALSE)
#   file_arg_index <- grep("--file=", args)
#   if (length(file_arg_index) > 0) {
#     # Running via Rscript
#     file_arg <- args[file_arg_index]
#     script_path <- normalizePath(sub("--file=", "", file_arg))
#     return(dirname(script_path))
#   } else if (interactive()) {
#     # Running interactively in RStudio
#     if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
#       script_path <- rstudioapi::getActiveDocumentContext()$path
#       if (nzchar(script_path)) {
#         return(dirname(normalizePath(script_path)))
#       } else {
#         stop("Cannot determine script directory: No active document found in RStudio.")
#       }
#     } else {
#       stop("Cannot determine script directory: Not running in RStudio or rstudioapi not available.")
#     }
#   } else {
#     # Cannot determine script directory
#     stop("Cannot determine script directory: Unknown execution environment.")
#   }
# }


###TESTING NEW
# Load knitr package (if not already loaded)
if (!requireNamespace("knitr", quietly = TRUE)) {
  install.packages("knitr")
}
library(knitr)

# Updated get_script_directory function
get_script_directory <- function() {
  # Try to get the path of the current Rmd file during rendering
  if (!is.null(current_input())) {
    script_directory <- dirname(normalizePath(current_input()))
    return(script_directory)
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

####END TEST
# Use the function to get the code directory
code_directory <- get_script_directory()

# Define the path to the Stata configuration file in the code directory
stata_config_path <- file.path(code_directory, "config_stata.do")

# Open a connection to the Stata .do file
stata_config_file <- file(stata_config_path, open = "w")

# Write global macro definitions to the file
writeLines(paste0("global DERIVED_DATA \"", derived_data, "\""), con = stata_config_file)
writeLines(paste0("global RAW_DATA \"", raw_data, "\""), con = stata_config_file)
writeLines(paste0("global SUPPLEMENTAL_DATA \"", supplemental_data, "\""), con = stata_config_file)
writeLines(paste0("global AUXILIARY_DATA \"", auxiliary_data, "\""), con = stata_config_file)
writeLines(paste0("global NICKNAMES_DICTIONARIES \"", nicknames_dictionaries, "\""), con = stata_config_file)

# Close the connection
close(stata_config_file)

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
