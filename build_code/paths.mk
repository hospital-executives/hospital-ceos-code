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

# Final individual output
FINAL_HIMSS := $(DERIVED_DIR)/final_himss.feather
# FINAL_CONFIRMED_FEATHER := $(DERIVED_DIR)/final_confirmed.feather
# FINAL_CONFIRMED_DTA := $(DERIVED_DIR)/final_confirmed.dta

# Assign AHA output
AHA_OUTPUT := $(AUX_DIR)/aha_himss_xwalk.csv
MSA_OUTPUT := $(AUX_DIR)/aha_himss_xwalk_msa.csv

# Final output
FINAL_HIMSS_AHA := $(DERIVED_DIR)/final_himss_with_aha.feather
FINAL_FEATHER_AHA := $(DERIVED_DIR)/final_confirmed_with_aha.feather
FINAL_DTA_AHA := $(DERIVED_DIR)/final_confirmed_with_aha.dta 

# stamps to compile himss and clean contact_uniqueid
COMPILE_STAMP := $(STAMP_DIR)/compile.stamp
MANUAL_STAMP := $(STAMP_DIR)/manual.stamp
CLEAN_PY_STAMP := $(STAMP_DIR)/clean_py.stamp
GENDER_STAMP := $(STAMP_DIR)/update_gender.stamp
JARO_STAMP := $(STAMP_DIR)/jaro.stamp
DIGRAPHS_STAMP := $(STAMP_DIR)/digraphs.stamp
BLOCKS_STAMP := $(STAMP_DIR)/blocks.stamp
PAIRS_STAMP := $(STAMP_DIR)/pairs.stamp
FUZZY_STAMP := $(STAMP_DIR)/fuzzy.stamp
FINAL_INDIVIDUAL_STAMP := $(STAMP_DIR)/final_individual.stamp

# stamps to assign AHA and add MSA
MAKE_AHA_STAMP := $(STAMP_DIR)/make_aha.stamp
ADD_MSA_STAMP := $(STAMP_DIR)/add_msa.stamp
CLEAN_AHA_STAMP := $(STAMP_DIR)/clean_aha.stamp
FINALIZE_AHA_STAMP := $(STAMP_DIR)/finalize_aha.stamp

# stamps for sumary statistics
HOSPITAL_SUMMARY_STAMP := $(STAMP_DIR)/hospital_summary.stamp
CEO_SUMMARY_STAMP := $(STAMP_DIR)/ceo_summary.stamp
NON_CEO_SUMMARY_STAMP := $(STAMP_DIR)/non_ceo_summary.stamp
MADMIN_SUMMARY_STAMP := $(STAMP_DIR)/madmin_summary.stamp

# stamps to match himss to aha 
HIMSS_TO_AHA_STAMP := $(STAMP_DIR)/himss_to_aha.stamp
AHA_TITLE_FLAG_STAMP := $(STAMP_DIR)/aha_title_flag.stamp
TITLE_MASTER_STAMP := $(STAMP_DIR)/title_master.stamp
FINAL_SUMSTATS_STAMP := $(STAMP_DIR)/final_sumstats.stamp