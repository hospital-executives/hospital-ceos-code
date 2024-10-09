# R script to check for and install required packages

# List of required packages
required_packages <- c(
  "dplyr",
  "ggplot2",
  "tidyr",
  "phonics"
)

# Function to check and install missing packages
check_and_install_packages <- function(packages) {
  for (pkg in packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      install.packages(pkg)
    }
  }
}

# Install any missing packages
check_and_install_packages(required_packages)

# Load all required packages
lapply(required_packages, library, character.only = TRUE)

# Optionally print a message to confirm packages are loaded
message("All required packages are installed and loaded.")