# get_project_directory.R

# Function to get the script directory
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
  return(script_directory)
}

# Get the script directory
script_directory <- get_script_directory()

# Construct the path to the config.R file
config_path <- file.path(script_directory, "config.R")

# Suppress messages and warnings to ensure clean output
suppressMessages({
  suppressWarnings({
    source(config_path)
  })
})

# Output the project_directory path
cat(project_directory)
