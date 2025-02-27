# This Makefile runs the following steps in order:
# 1. compile_himss.Rmd
# 2. manual_assignment.Rmd
# 3. clean_data.py
# 4. update_gender.py
# 5. generate_jaro.py
# 6. generate_digraphs.py
# 7. generate_blocks.py
# 8. generate_clean_df.py
# 9. generate_final.py

# Capture USER_DIR by running get_project_directory.R
USER_DIR := $(shell Rscript get_makefile_paths.R)
R_USER_DIR := $(shell Rscript get_project_directory.R)

# Check if USER_DIR is empty
ifeq ($(strip $(USER_DIR)),)
$(error USER_DIR could not be determined. Please check get_makefile_paths.R)
endif

# Define Code Directory as the directory containing the Makefile
CODE_DIR := $(CURDIR)

# Define Data Directories using USER_DIR
DATA_DIR := $(USER_DIR)/_data
DERIVED_DIR := $(DATA_DIR)/derived
STAMP_DIR := $(DERIVED_DIR)/stamps
AUX_DIR := $(DERIVED_DIR)/auxiliary
SUPP_DIR := $(DATA_DIR)/supplemental

# Define File Paths
# Paths for R scripts
GENDER_R := "$(R_USER_DIR)/_data/nicknames dictionaries/gender_test.csv"
HIMSS_R := $(R_USER_DIR)/_data/derived/himss_entities_contacts_0517_v1.feather

# Manual paths
MANUAL_PATH := $(CODE_DIR)/manual_assignment.Rmd
HIMSS_ENTITIES_CONTACTS := $(DERIVED_DIR)/himss_entities_contacts_0517_v1.feather
OUTLIERS := $(AUX_DIR)/auxiliary/outliers.csv
CONFIRMED_R := $(AUX_DIR)/r_confirmed.feather
REMAINING_R := $(AUX_DIR)/r_remaining.feather

# Clean data paths
CONFIRMED_1 := $(AUX_DIR)/confirmed_1.csv
REMAINING_1 := $(AUX_DIR)/remaining_1.csv
HIMSS_1 := $(AUX_DIR)/himss_1.csv
HIMSS_NICKNAMES := $(AUX_DIR)/himss_nicknames.csv

# Updated gender path
R_GENDER := $(DATA_DIR)/nicknames\ dictionaries/gender_test.csv
UPDATED_GENDER := $(AUX_DIR)/updated_gender.csv

# Jaro path
JARO := $(AUX_DIR)/jaro_output.csv

# Digraph paths
DIGRAPHNAME := $(AUX_DIR)/digraphname.csv
DIGRAPHMETA := $(AUX_DIR)/digraphmeta.csv

# Block paths
CONFIRMED_2 := $(AUX_DIR)/confirmed_2.csv
REMAINING_2 := $(AUX_DIR)/remaining_2.csv

# Cleaned paths
PY_CLEANED := $(AUX_DIR)/py_confirmed.csv
PY_REMAINING := $(AUX_DIR)/py_remaining.csv
GRAPH_COMPS := $(AUX_DIR)/py_graph_components.json

# Final output
FINAL_HIMSS := $(DERIVED_DIR)/final_himss.feather
FINAL_CONFIRMED := $(DERIVED_DIR)/final_confirmed.dta

# Define Stamps
COMPILE_STAMP := $(STAMP_DIR)/compile.stamp
MANUAL_STAMP := $(STAMP_DIR)/manual.stamp
CLEAN_PY_STAMP := $(STAMP_DIR)/clean_py.stamp
GENDER_STAMP := $(STAMP_DIR)/update_gender.stamp
JARO_STAMP := $(STAMP_DIR)/jaro.stamp
DIGRAPHS_STAMP := $(STAMP_DIR)/digraphs.stamp
BLOCKS_STAMP := $(STAMP_DIR)/blocks.stamp
FUZZY_STAMP := $(STAMP_DIR)/fuzzy.stamp
FINAL_STAMP := $(STAMP_DIR)/final.stamp

# Other
PANDOC_PATH := /Applications/RStudio.app/Contents/Resources/app/quarto/bin/tools/x86_64

print-pandoc:
	@echo "PANDOC_PATH = $(PANDOC_PATH)"

debug: 
	@echo "R_PATH = $(R_USER_DIR)"
	@echo "CODE = $(CODE_DIR)"
	@echo "HIMSS = $(HIMSS_R)"

# Stamp Dependencies
all: '$(FINAL_STAMP)'

# Step 1: Compile HIMSS data
compile_himss: $(COMPILE_STAMP)
$(COMPILE_STAMP):
	export RSTUDIO_PANDOC=$(PANDOC_PATH); \
	Rscript -e "rmarkdown::render('compile_himss.Rmd', \
	params = list(code_dir = '$(CODE_DIR)/', r_gender = '$(R_GENDER)'))"
	touch $(COMPILE_STAMP)

# Step 2: Manual assignment
manual_assignment: $(MANUAL_STAMP)
$(MANUAL_STAMP): $(COMPILE_STAMP)
	export RSTUDIO_PANDOC=$(PANDOC_PATH); \
	Rscript -e "rmarkdown::render('clean_data.Rmd', \
	params = list(code_dir = '$(CODE_DIR)', \
	code_path = '$(CODE_DIR)', \
	data_path = '$(R_USER_DIR)', \
	himss_entity_contacts = '$(HIMSS_R)'))"
	touch $(MANUAL_STAMP)

# Step 3: Clean data
clean_py : $(CLEAN_PY_STAMP)
$(CLEAN_PY_STAMP): $(MANUAL_STAMP)
	python3 clean_data.py $(CONFIRMED_R) $(REMAINING_R) \
	$(HIMSS_ENTITIES_CONTACTS) $(CONFIRMED_1) $(REMAINING_1) \
	$(HIMSS_1) $(HIMSS_NICKNAMES)
	touch $(CLEAN_PY_STAMP)

# Step 4: Update gender
update_gender: $(GENDER_STAMP)
$(GENDER_STAMP): $(CLEAN_PY_STAMP)
	python3 helper_scripts/update_gender.py $(CONFIRMED_1) $(UPDATED_GENDER) \
	$(DATA_DIR) $(R_GENDER)
	touch $(GENDER_STAMP)

# Step 5: Generate Jaro
update_jaro : $(JARO_STAMP)
$(JARO_STAMP): $(GENDER_STAMP)
	python3 helper_scripts/jaro_algo.py $(CODE_DIR) $(CONFIRMED_R) \
	$(UPDATED_GENDER) $(JARO)
	touch $(JARO_STAMP)

# Step 6: Generate digraphs
make_digraphs: $(DIGRAPHS_STAMP)
$(DIGRAPHS_STAMP): $(JARO_STAMP)
	python3 helper_scripts/generate_digraphs.py $(HIMSS_1) $(HIMSS_NICKNAMES) \
	$(DIGRAPHNAME) $(DIGRAPHMETA) $(SUPP_DIR)
	touch $(DIGRAPHS_STAMP)

# Step 7: Generate blocks
make_blocks: $(BLOCKS_STAMP)
$(BLOCKS_STAMP): $(DIGRAPHS_STAMP)
	python3 generate_blocks.py $(HIMSS_ENTITIES_CONTACTS) $(CONFIRMED_1) \
	$(REMAINING_1) $(UPDATED_GENDER) $(HIMSS_1) $(CONFIRMED_2) \
	$(REMAINING_2) $(DATA_DIR)
	touch $(BLOCKS_STAMP)

# Step 8: Generate final cleaned data
fuzzy: $(FUZZY_STAMP)
$(FUZZY_STAMP): $(BLOCKS_STAMP)
	python3 -u fuzzy_final.py $(CONFIRMED_2) $(REMAINING_2) \
	$(PY_CLEANED) $(PY_REMAINING) $(GRAPH_COMPS) \
	$(DATA_DIR) $(CODE_DIR)
	touch $(FUZZY_STAMP)

# Step 9: Generate final HIMSS dataset and cleaned dta with flags
make_final: $(FINAL_STAMP)
$(FINAL_STAMP): $(FUZZY_STAMP)
	python3 generate_final.py $(PY_CLEANED) $(PY_REMAINING) \
	$(GRAPH_COMPS) $(FINAL_HIMSS) $(FINAL_CONFIRMED) \
	$(HIMSS_ENTITIES_CONTACTS)
	touch $(FINAL_STAMP)


