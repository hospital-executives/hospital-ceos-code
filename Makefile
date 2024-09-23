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

# DEFINE FILE PATHS 
# manual paths
MANUAL_PATH := $(CODE_DIR)/manual_assignment.Rmd
HIMSS_ENTITIES_CONTACTS := $(DERIVED_DIR)/himss_entities_contacts_0517_v1.feather
OUTLIERS := $(DERIVED_DIR)/auxiliary/outliers.csv
CONFIRMED_R := $(DERIVED_DIR)/r_confirmed.feather
REMAINING_R := $(DERIVED_DIR)/r_remaining.feather

# clean data paths
CONFIRMED_1 := $(DERIVED_DIR)/auxiliary/confirmed_1.csv
REMAINING_1 := $(DERIVED_DIR)/auxiliary/remaining_1.csv
HIMSS_1 := $(DERIVED_DIR)/auxiliary/himss_1.csv
HIMSS_NICKNAMES := $(DERIVED_DIR)/auxiliary/himss_nicknames.csv
HIMSS_ENTITIES_CONTACTS_NEW := $(DERIVED_DIR)/himss_entities_contacts_0517.feather

# updated gender path
R_GENDER = $(DATA_DIR)/nicknames\ dictionaries/gender_test.csv
UPDATED_GENDER := $(DERIVED_DIR)/auxiliary/updated_gender.csv

# jaro path
JARO := $(DERIVED_DIR)/auxiliary/jaro_output.csv

# digraph paths
DIGRAPHNAME := $(DERIVED_DIR)/auxiliary/digraphname.csv
DIGRAPHMETA := $(DERIVED_DIR)/auxiliary/digraphmeta.csv

# block paths
CONFIRMED_2 := $(DERIVED_DIR)/auxiliary/confirmed_2.csv
REMAINING_2 := $(DERIVED_DIR)/auxiliary/remaining_2.csv

# cleaned paths
FINAL_CLEANED := $(DERIVED_DIR)/py_confirmed.csv
FINAL_REMAINING := $(DERIVED_DIR)/py_remaining.csv
GRAPH_COMPS := $(DERIVED_DIR)/py_graph_components.json 

# Other
PANDOC_PATH = /Applications/RStudio.app/Contents/Resources/app/quarto/bin/tools/x86_64

# HIMSS SCRIPT - FAKE/INCOMPLETE
#FAKE := $(DERIVED_DIR)/auxiliary/fake.csv
#.PHONY: $(FAKE)


$(HIMSS_ENTITIES_CONTACTS) $(R_GENDER): compile_himss.Rmd
	export RSTUDIO_PANDOC=$(PANDOC_PATH); \
	Rscript -e "rmarkdown::render('compile_himss.Rmd', output_file='$(HIMSS_ENTITIES_CONTACTS)', \
	params = list(code_dir = '$(CODE_DIR)', r_gender = '$(R_GENDER)'))"

# RUN R SCRIPT
$(OUTLIERS) $(CONFIRMED_R) $(REMAINING_R): $(HIMSS_ENTITIES_CONTACTS)
	export RSTUDIO_PANDOC=$(PANDOC_PATH); \
	Rscript -e "rmarkdown::render('manual_assignment.Rmd', \
    params = list(code_path='$(CODE_DIR)', \
    data_path='$(DATA_DIR)', \
    himss_entity_contacts='$(HIMSS_ENTITIES_CONTACTS)'))"

# CLEAN DATA
# Define directory and file paths
cleaned_targets := $(CONFIRMED_1) $(REMAINING_1) $(HIMSS_1) $(HIMSS_NICKNAMES)

# was himss_new before
$(cleaned_targets): $(CONFIRMED_R) $(REMAINING_R) $(HIMSS_ENTITIES_CONTACTS)
	python3 clean_data.py $(CONFIRMED_R) $(REMAINING_R) \
	$(HIMSS_ENTITIES_CONTACTS) $(CONFIRMED_1) $(REMAINING_1) \
	$(HIMSS_1) $(HIMSS_NICKNAMES)

# UPDATE GENDER
$(UPDATED_GENDER): $(CONFIRMED_1) $(R_GENDER)
	python3 helper-scripts/update_gender.py $(CONFIRMED_1) $(UPDATED_GENDER) \
	$(DATA_DIR) $(R_GENDER)

# GENERATE JARO - takes ~21 min
$(JARO): $(CONFIRMED_R) $(UPDATED_GENDER)
	python3 helper-scripts/jaro_algo.py $(CODE_DIR) $(CONFIRMED_R) \
	$(UPDATED_GENDER) $(JARO)

# DIGRAPHS
$(DIGRAPHNAME) $(DIGRAPHMETA): $(HIMSS_1) $(HIMSS_NICKNAMES)
	python3 helper-scripts/generate_digraphs.py $(HIMSS_1) $(HIMSS_NICKNAMES) \
	$(DIGRAPHNAME) $(DIGRAPHMETA) $(SUPP_DIR)

# GENERATE BLOCKS - was new
$(CONFIRMED_2) $(REMAINING_2): $(HIMSS_ENTITIES_CONTACTS) $(CONFIRMED_1) $(REMAINING_1) \
    $(UPDATED_GENDER) $(HIMSS_1) $(DIGRAPHNAME) $(DIGRAPHMETA)
	python3 generate_blocks.py $(HIMSS_ENTITIES_CONTACTS) $(CONFIRMED_1) \
    $(REMAINING_1) $(UPDATED_GENDER) $(HIMSS_1) $(CONFIRMED_2) $(REMAINING_2) $(DATA_DIR)


# FIX NON-UNIQUE IDS
$(FINAL_CLEANED) $(FINAL_REMAINING) $(GRAPH_COMPS): $(CONFIRMED_2) \
	$(REMAINING_2) $(DIGRAPHNAMES) $(DIGRAPHMETA) $(JARO)
	python3 -u generate_clean_df.py $(CONFIRMED_2) $(REMAINING_2) \
	$(FINAL_CLEANED) $(FINAL_REMAINING) $(GRAPH_COMPS) \
    $(DATA_DIR) $(CODE_DIR)

# GENERATE FINAL DF
FINAL_HIMSS :=  $(DERIVED_DIR)/final_himss.feather 

# Rule to create final_himss
$(FINAL_HIMSS): $(FINAL_CLEANED) $(FINAL_REMAINING) $(GRAPH_COMPS)
	@echo "Generating final_himss..."
	python3 generate_final.py $(FINAL_CLEANED) $(FINAL_REMAINING) \
	$(GRAPH_COMPS) $(FINAL_HIMSS)
	@echo "final_himss created!"


# /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/


# to run to generate blocks: 
#make /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/auxiliary/confirmed_2.csv /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/auxiliary/remaining_2.csv
# confirmed r: 
# /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/r_confirmed.feather
# updated gender:
# /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/auxiliary/updated_gender.csv
# make final cleand df

# make -B /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/himss_entities_contacts_0517_v1.feather /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/supplemental/gender.csv