# This Makefile should run the following, in order:
# 1. compile_himss.Rmd
# 2. manual_assignment.Rmd - done
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

# RUN R SCRIPT
OUTLIERS CONFIRMED_R REMAINING_R: $(HIMSS_ENTITIES_CONTACTS)
	export RSTUDIO_PANDOC=$(PANDOC_PATH); \
	Rscript -e "rmarkdown::render('manual_assignment.Rmd', \
    params = list(code_path='$(CODE_DIR)', \
    data_path='$(DATA_DIR)', \
    himss_entity_contacts='$(HIMSS_ENTITIES_CONTACTS)'))"

# CLEAN DATA
# Define directory and file paths
CONFIRMED_1 := $(DERIVED_DIR)/auxiliary/confirmed_1.csv
REMAINING_1 := $(DERIVED_DIR)/auxiliary/remaining_1.csv
HIMSS_1 := $(DERIVED_DIR)/auxiliary/himss_1.csv
HIMSS_NICKNAMES := $(DERIVED_DIR)/auxiliary/himss_nicknames.csv

# Define the cleaned targets
cleaned_targets := $(CONFIRMED_1) $(REMAINING_1) $(HIMSS_1) $(HIMSS_NICKNAMES)

HIMSS_ENTITIES_CONTACTS_NEW := $(DERIVED_DIR)/himss_entities_contacts_0517.feather

# Target to clean data (generate CONFIRMED_1, REMAINING_1, and HIMSS_1)
$(cleaned_targets): $(CONFIRMED_R) $(REMAINING_R) $(HIMSS_ENTITIES_CONTACTS_NEW) $(HIMSS_NICKNAMES)
	python3 clean_data.py $(CONFIRMED_R) $(REMAINING_R) $(HIMSS_ENTITIES_CONTACTS_NEW) $(CONFIRMED_1) $(REMAINING_1) $(HIMSS_1) $(HIMSS_NICKNAMES)

# UPDATE GENDER
UPDATED_GENDER := $(DERIVED_DIR)/auxiliary/updated_gender.csv
$(UPDATED_GENDER): $(CONFIRMED_1)
	python3 helper-scripts/update_gender.py $(CONFIRMED_1) $(UPDATED_GENDER) $(DATA_DIR)

# GENERATE JARO
JARO := $(DERIVED_DIR)/auxiliary/jaro_output.csv
$(JARO): $(CONFIRMED_R) $(UPDATED_GENDER)
	python3 helper-scripts/jaro_algo.py $(CODE_DIR) $(CONFIRMED_R) $(UPDATED_GENDER) $(JARO)


# GENERATE BLOCKS
CONFIRMED_2 := $(DERIVED_DIR)/auxiliary/confirmed_2.csv
REMAINING_2 := $(DERIVED_DIR)/auxiliary/remaining_2.csv

$(CONFIRMED_2) $(REMAINING_2): $(HIMSS_ENTITIES_CONTACTS_NEW) $(CONFIRMED_1) $(REMAINING_1) $(UPDATED_GENDER) $(HIMSS_1)
	python3 generate_blocks.py $(HIMSS_ENTITIES_CONTACTS_NEW) $(CONFIRMED_1) $(REMAINING_1) $(UPDATED_GENDER) $(HIMSS_1) $(CONFIRMED_2) $(REMAINING_2) $(SUPP_DIR)

# /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/

.PHONY: run_test

run_test: $(SUPP_DIR)
	python3 test.py $(SUPP_DIR)

# to run to generate blocks: 
#make /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/auxiliary/confirmed_2.csv /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/auxiliary/remaining_2.csv
# confirmed r: 
# /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/r_confirmed.feather
# updated gender:
# /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/auxiliary/updated_gender.csv