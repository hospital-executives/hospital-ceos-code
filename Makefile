# Define Code Directory
CODE_DIR := "/Users/loaner/hospital-ceos-code"

# Define Data Directories
USER_DIR := "/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos"
DATA_DIR := "$(USER_DIR)/_data"
DERIVED_DIR := "$(DATA_DIR)/derived"
SUPP_DIR := "$(DATA_DIR)/supplemental"

# Other
PANDOC_PATH = "/Applications/RStudio.app/Contents/Resources/app/quarto/bin/tools/x86_64"


# MANUAL ASSIGNMENT DEPENDENCIES
# - HIMSS FEATHER FILE 
# - 3X NICKNAMES FILES - now in supplemental data
# MANUAL ASSIGNMENT TARGETS
# - OUTLIERS.CSV (WRITE NEW)
# - CONFIRMED_R FEATHER
# - REMAINING_R FEATHER

# MANUAL ASSIGNMENT PATHS
# Define file paths
MANUAL_PATH := "$(CODE_DIR)/manual_assignment.Rmd"
HIMSS_ENTITIES_CONTACTS := "$(DERIVED_DIR)/himss_entities_contacts_0517_v1.feather"
OUTLIERS := "$(DERIVED_DIR)/auxiliary/outliers.csv"
CONFIRMED_R := "$(DERIVED_DIR)/r_confirmed.feather"
REMAINING_R := "$(DERIVED_DIR)/r_remaining.feather"

# List of actual target file paths
all: $(OUTLIERS) $(CONFIRMED_R) $(REMAINING_R)

$(OUTLIERS) $(CONFIRMED_R) $(REMAINING_R): run_rmd 


DIR_OUTLIERS := $(subst $(space),\ ,$(dir $(OUTLIERS)))
DIR_CONFIRMED_R := $(subst $(space),\ ,$(dir $(CONFIRMED_R)))
DIR_REMAINING_R := $(subst $(space),\ ,$(dir $(REMAINING_R)))
run_rmd:
	@echo "OUTLIERS path: $(OUTLIERS)"
	@echo "CONFIRMED_R path: $(CONFIRMED_R)"
	@echo "REMAINING_R path: $(REMAINING_R)"
	@echo "Creating necessary directories..."
	@mkdir -p "$(DIR_OUTLIERS)" "$(DIR_CONFIRMED_R)" "$(DIR_REMAINING_R)"

	@echo "Passed"

	@echo "Running R Markdown..."
	Rscript -e "Sys.setenv(RSTUDIO_PANDOC = '$(PANDOC_PATH)'); \
	rmarkdown::render('$(MANUAL_PATH)', \
	params = list( \
		himss_entity_contacts = '$(HIMSS_ENTITIES_CONTACTS)', \
		code_path = '$(CODE_DIR)', \
		data_path = '$(DATA_DIR)', \
		targets = list( \
			outliers = '$(OUTLIERS)', \
			confirmed_r = '$(CONFIRMED_R)', \
			remaining_r = '$(REMAINING_R)' \
		) \
	), \
	output_file = NULL)"
	
	@echo "Touching target files..."
	touch "$(OUTLIERS)" "$(CONFIRMED_R)" "$(REMAINING_R)"

# Clean rule to remove the generated files
clean:
	rm -f $(OUTLIERS) $(CONFIRMED_R) $(REMAINING_R)



#INPUT_PATH = /Users/loaner/hospital-coes-code/
#

#run_rmd:
#Rscript -e "Sys.setenv(RSTUDIO_PANDOC='$(PANDOC_PATH)'); \
	#rmarkdown::render('test.Rmd', \
	#params = list(input_path = '$(INPUT_PATH)'))"

#clean:
#rm -f test.html test.pdf test.md
# rm -rf test_files/