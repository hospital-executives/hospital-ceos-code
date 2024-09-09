# This Makefile should run the following, in order:
# 1. compile_himss.Rmd
# 2. manual_assignment.Rmd
# 3. clean_data.py
# 4. update_gender.py - aux
# 5. generate_blocks.py
# 6. generate_digraph.py - aux
# 7. jaro_helper.py and jaro_algo.py - aux, can combine into one
# 8. clean_problem_3.py
# 9. final_blocks.py


# Define Code Directory
CODE_DIR := /Users/loaner/hospital-ceos-code

# Define Data Directories
USER_DIR := /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos
DATA_DIR := $(USER_DIR)/_data
DERIVED_DIR := $(DATA_DIR)/derived
SUPP_DIR := $(DATA_DIR)/supplemental

# Other
PANDOC_PATH = /Applications/RStudio.app/Contents/Resources/app/quarto/bin/tools/x86_64


# MANUAL ASSIGNMENT DEPENDENCIES
# - HIMSS FEATHER FILE 
# - 3X NICKNAMES FILES - now in supplemental data
# MANUAL ASSIGNMENT TARGETS
# - OUTLIERS.CSV (WRITE NEW)
# - CONFIRMED_R FEATHER
# - REMAINING_R FEATHER

# MANUAL ASSIGNMENT PATHS
# Define file paths
MANUAL_PATH := $(CODE_DIR)/manual_assignment.Rmd
HIMSS_ENTITIES_CONTACTS := $(DERIVED_DIR)/himss_entities_contacts_0517_v1.feather
OUTLIERS := $(DERIVED_DIR)/auxiliary/outliers.csv
CONFIRMED_R := $(DERIVED_DIR)/r_confirmed.feather
REMAINING_R := $(DERIVED_DIR)/r_remaining.feather

# List of actual target file paths

# pt 1 - go back and confirm
#.PHONY: HIMSS_ENTITIES_CONTACTS
#HIMSS_ENTITIES_CONTACTS: compile_himss.Rmd
    #Rscript -e "rmarkdown::render('compile_himss.Rmd', output_file = 'HIMSS_ENTITIES_CONTACTS')"

# pt 2
#TARGETS := "OUTLIERS, CONFIRMED_R, REMAINING_R"

.PHONY: OUTLIERS CONFIRMED_R REMAINING_R

OUTLIERS CONFIRMED_R REMAINING_R: $(HIMSS_ENTITIES_CONTACTS)
	export RSTUDIO_PANDOC=$(PANDOC_PATH); \
	Rscript -e "rmarkdown::render('manual_assignment.Rmd', \
    params = list(code_path='$(CODE_DIR)', \
    data_path='$(DATA_DIR)', \
    himss_entity_contacts='$(HIMSS_ENTITIES_CONTACTS)'))"

# pt 3 - theoretical
#run_script:
    #python3 my_script.py $(CODE_PATH) $(DATA_PATH)