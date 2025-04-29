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

# Pair paths
COMP_PAIRS := $(AUX_DIR)/comp_pairs.feather
META_PAIRS := $(AUX_DIR)/meta_pairs.feather
CLEAN_META_PAIRS := $(AUX_DIR)/cleaned_meta_pairs.feather
COMP_CLEANED := $(AUX_DIR)/comp_cleaned.json
META_CLEANED := $(AUX_DIR)/meta_cleaned.json

# Cleaned paths
PY_CLEANED := $(AUX_DIR)/py_confirmed.csv
PY_REMAINING := $(AUX_DIR)/py_remaining.csv
GRAPH_COMPS := $(AUX_DIR)/py_graph_components.json

# Final output
FINAL_HIMSS := $(DERIVED_DIR)/final_himss.feather
FINAL_CONFIRMED_FEATHER := $(DERIVED_DIR)/final_confirmed.feather
FINAL_CONFIRMED_DTA := $(DERIVED_DIR)/final_confirmed.dta

# Define Stamps
COMPILE_STAMP := $(STAMP_DIR)/compile.stamp
MANUAL_STAMP := $(STAMP_DIR)/manual.stamp
CLEAN_PY_STAMP := $(STAMP_DIR)/clean_py.stamp
GENDER_STAMP := $(STAMP_DIR)/update_gender.stamp
JARO_STAMP := $(STAMP_DIR)/jaro.stamp
DIGRAPHS_STAMP := $(STAMP_DIR)/digraphs.stamp
BLOCKS_STAMP := $(STAMP_DIR)/blocks.stamp
PAIRS_STAMP := $(STAMP_DIR)/pairs.stamp
FUZZY_STAMP := $(STAMP_DIR)/fuzzy.stamp
FINAL_STAMP := $(STAMP_DIR)/final.stamp

# Define error message
define safe_run
	@$(1) || { \
		echo 'You may be getting this error if you are selectively syncing files from Dropbox.'; \
		echo ' Double check all relevant files called in this script'; \
		echo ' are “Available Offline” in Dropbox.'; \
		exit 1; \
	}
endef

# Stamp Dependencies
all: $(FINAL_STAMP)

# Step 1: Compile HIMSS data
compile_himss: $(COMPILE_STAMP)
$(COMPILE_STAMP):
	$(call safe_run,Rscript compile_himss.R $(CODE_DIR))
	touch $(COMPILE_STAMP)

# Step 2: Manual assignment
manual_assignment: $(MANUAL_STAMP)
$(MANUAL_STAMP): $(COMPILE_STAMP)
	$(call safe_run,Rscript clean_data.R $(CODE_DIR) '$(R_USER_DIR)' '$(HIMSS_R)'')
	touch $(MANUAL_STAMP)

# Step 3: Clean data
clean_py : $(CLEAN_PY_STAMP)
$(CLEAN_PY_STAMP): $(MANUAL_STAMP)
	$(call safe_run,python3 clean_data.py $(CODE_DIR) $(CONFIRMED_R) $(REMAINING_R) \
	$(HIMSS_ENTITIES_CONTACTS) $(CONFIRMED_1) $(REMAINING_1) \
	$(HIMSS_1) $(HIMSS_NICKNAMES))
	touch $(CLEAN_PY_STAMP)

# Step 4: Update gender
update_gender: $(GENDER_STAMP)
$(GENDER_STAMP): $(CLEAN_PY_STAMP)
	$(call safe_run,python3 helper_scripts/update_gender.py $(CONFIRMED_1) \
	$(UPDATED_GENDER) $(DATA_DIR) $(R_GENDER))
	touch $(GENDER_STAMP)

# Step 5: Generate Jaro
update_jaro : $(JARO_STAMP)
$(JARO_STAMP): $(GENDER_STAMP)
	$(call safe_run,python3 helper_scripts/jaro_algo.py $(CODE_DIR) \
	$(CONFIRMED_R) $(UPDATED_GENDER) $(JARO))
	touch $(JARO_STAMP)

# Step 6: Generate digraphs
make_digraphs: $(DIGRAPHS_STAMP)
$(DIGRAPHS_STAMP): $(JARO_STAMP)
	$(call safe_run,python3 helper_scripts/generate_digraphs.py $(HIMSS_1) \
	$(HIMSS_NICKNAMES) $(DIGRAPHNAME) $(DIGRAPHMETA) $(SUPP_DIR))
	touch $(DIGRAPHS_STAMP)

# Step 7: Generate blocks
make_blocks: $(BLOCKS_STAMP)
$(BLOCKS_STAMP): $(DIGRAPHS_STAMP)
	$(call safe_run,python3 generate_blocks.py $(HIMSS_ENTITIES_CONTACTS) \
	$(CONFIRMED_1) $(REMAINING_1) $(UPDATED_GENDER) $(HIMSS_1) $(CONFIRMED_2) \
	$(REMAINING_2) $(DATA_DIR))
	touch $(BLOCKS_STAMP)

# Step 8: Generate pairwise comparisons
pairs: $(PAIRS_STAMP)
$(PAIRS_STAMP): $(BLOCKS_STAMP)
	$(call safe_run,python3 generate_pairs.py  $(CONFIRMED_2) $(REMAINING_2) \
	$(COMP_PAIRS) $(META_PAIRS) $(CLEAN_META_PAIRS) \
	$(COMP_CLEANED) $(META_CLEANED) \
	$(DATA_DIR) $(CODE_DIR))
	touch $(PAIRS_STAMP)

fuzzy: $(FUZZY_STAMP)
$(FUZZY_STAMP): $(PAIRS_STAMP)
	$(call safe_run,python3 new_fuzzy_match.py $(CONFIRMED_2) $(REMAINING_2) \
	$(COMP_PAIRS) $(META_PAIRS) $(CLEAN_META_PAIRS) $(COMP_CLEANED) $(META_CLEANED) \
	$(PY_CLEANED) $(PY_REMAINING) $(GRAPH_COMPS) \
	$(DATA_DIR) $(CODE_DIR))
	touch $(FUZZY_STAMP)

# Step 9: Generate final HIMSS dataset and cleaned dta with flags
make_final: $(FINAL_STAMP)
$(FINAL_STAMP): $(FUZZY_STAMP)
	$(call safe_run,python3 generate_final.py $(PY_CLEANED) $(PY_REMAINING) \
	$(GRAPH_COMPS) $(FINAL_HIMSS) $(FINAL_CONFIRMED_DTA) \
	$(FINAL_CONFIRMED_FEATHER) $(HIMSS_ENTITIES_CONTACTS))
	touch $(FINAL_STAMP)


