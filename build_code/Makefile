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

# setup python
PROJ_ENV := ../setup_environment/proj-env
PY  := $(PROJ_ENV)/bin/python

# Capture USER_DIR by running get_project_directory.R
USER_DIR := $(shell Rscript get_makefile_paths.R)
R_USER_DIR := $(shell Rscript get_project_directory.R)
CODE_DIR := $(CURDIR)

# Check if USER_DIR is empty
ifeq ($(strip $(USER_DIR)),)
$(error USER_DIR could not be determined. Please check get_makefile_paths.R)
endif

include paths.mk

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
all: $(FINAL_SUMSTATS_STAMP)

#### CLEAN INDIVIDUAL LEVEL DATA ####

# compile himss from raw files
compile_himss: $(COMPILE_STAMP)
$(COMPILE_STAMP): compile_himss.R
	$(call safe_run,Rscript compile_himss.R $(CODE_DIR))
	touch $(COMPILE_STAMP)

# manually check m:1 and 1:m cases
manual_assignment: $(MANUAL_STAMP)
$(MANUAL_STAMP): $(COMPILE_STAMP) clean_data.R
	$(call safe_run,Rscript clean_data.R $(CODE_DIR) '$(R_USER_DIR)' '$(HIMSS_R)')
	touch $(MANUAL_STAMP)

# clean data
clean_py : $(CLEAN_PY_STAMP) clean_data.py
$(CLEAN_PY_STAMP): $(MANUAL_STAMP) $(CONFIRMED_R) $(REMAINING_R) $(HIMSS_ENTITIES_CONTACTS) clean_data.py
	$(call safe_run,$(PY) clean_data.py $(CODE_DIR) $(CONFIRMED_R) $(REMAINING_R) \
	$(HIMSS_ENTITIES_CONTACTS) $(CONFIRMED_1) $(REMAINING_1) \
	$(HIMSS_1) $(HIMSS_NICKNAMES))
	touch $(CLEAN_PY_STAMP)

# update gender
update_gender: $(GENDER_STAMP)
$(GENDER_STAMP): $(CLEAN_PY_STAMP) $(CONFIRMED_1) helper_scripts/update_gender.py
	$(call safe_run,$(PY) helper_scripts/update_gender.py $(CONFIRMED_1) \
	$(UPDATED_GENDER) $(DATA_DIR) $(R_GENDER))
	touch $(GENDER_STAMP)

# generate jaro-winkler similarity scores
update_jaro : $(JARO_STAMP)
$(JARO_STAMP): $(GENDER_STAMP) $(CONFIRMED_R) $(UPDATED_GENDER) helper_scripts/jaro_algo.py
	$(call safe_run,$(PY) helper_scripts/jaro_algo.py $(CODE_DIR) \
	$(CONFIRMED_R) $(UPDATED_GENDER) $(JARO))
	touch $(JARO_STAMP)

# generate digraphs of nicknames
make_digraphs: $(DIGRAPHS_STAMP)
$(DIGRAPHS_STAMP): $(JARO_STAMP) $(HIMSS_1) $(HIMSS_NICKNAMES) helper_scripts/generate_digraphs.py
	$(call safe_run,$(PY) helper_scripts/generate_digraphs.py $(HIMSS_1) \
	$(HIMSS_NICKNAMES) $(DIGRAPHNAME) $(DIGRAPHMETA) $(SUPP_DIR))
	touch $(DIGRAPHS_STAMP)

# generate blocks
make_blocks: $(BLOCKS_STAMP)
$(BLOCKS_STAMP): $(DIGRAPHS_STAMP) $(HIMSS_ENTITIES_CONTACTS) $(CONFIRMED_1) $(REMAINING_1) \
	$(UPDATED_GENDER) $(HIMSS_1) generate_blocks.py
	$(call safe_run,$(PY) generate_blocks.py $(HIMSS_ENTITIES_CONTACTS) \
	$(CONFIRMED_1) $(REMAINING_1) $(UPDATED_GENDER) $(HIMSS_1) $(CONFIRMED_2) \
	$(REMAINING_2) $(DATA_DIR))
	touch $(BLOCKS_STAMP)

# generate pairwise comparisons
pairs: $(PAIRS_STAMP)
$(PAIRS_STAMP): $(BLOCKS_STAMP) $(CONFIRMED_2) $(REMAINING_2) generate_pairs.py
	$(call safe_run,$(PY) generate_pairs.py  $(CONFIRMED_2) $(REMAINING_2) \
	$(COMP_PAIRS) $(META_PAIRS) $(CLEAN_META_PAIRS) \
	$(COMP_CLEANED) $(META_CLEANED) \
	$(DATA_DIR) $(CODE_DIR))
	touch $(PAIRS_STAMP)

# determine if pairs are the same or different
fuzzy: $(FUZZY_STAMP)
$(FUZZY_STAMP): $(PAIRS_STAMP) $(CONFIRMED_2) $(REMAINING_2) $(COMP_PAIRS) $(META_PAIRS) $(CLEAN_META_PAIRS) \
	$(COMP_CLEANED) $(META_CLEANED) new_fuzzy_match.py
	$(call safe_run,$(PY) new_fuzzy_match.py $(CONFIRMED_2) $(REMAINING_2) \
	$(COMP_PAIRS) $(META_PAIRS) $(CLEAN_META_PAIRS) $(COMP_CLEANED) $(META_CLEANED) \
	$(PY_CLEANED) $(PY_REMAINING) $(GRAPH_COMPS) \
	$(DATA_DIR) $(CODE_DIR))
	touch $(FUZZY_STAMP)

# generate final cleaned individual-level output
individual_output: $(FINAL_INDIVIDUAL_STAMP)
$(FINAL_INDIVIDUAL_STAMP): $(FUZZY_STAMP) $(PY_CLEANED) $(PY_REMAINING) $(GRAPH_COMPS) generate_final.py
	$(call safe_run,$(PY) generate_final.py $(PY_CLEANED) $(PY_REMAINING) \
	$(GRAPH_COMPS) $(FINAL_HIMSS) $(FINAL_CONFIRMED_DTA) \
	$(FINAL_CONFIRMED_FEATHER) $(HIMSS_ENTITIES_CONTACTS))
	touch $(FINAL_INDIVIDUAL_STAMP)

#### CLEAN HOSPITAL LEVEL DATA ####
# build crosswalk between himss and aha + add latitude and longitude
assign_aha: $(MAKE_AHA_STAMP)
$(MAKE_AHA_STAMP): $(FINAL_INDIVIDUAL_STAMP) mcr_aha_xwalk.py
	$(call safe_run,python mcr_aha_xwalk.py $(DATA_DIR) $(AHA_OUTPUT))
	touch $(MAKE_AHA_STAMP)

# add msa based on latitude and longitude
add_msa: $(ADD_MSA_STAMP)
$(ADD_MSA_STAMP): $(MAKE_AHA_STAMP) map_to_msa.py $(AUX_DIR)/aha_himss_xwalk.csv
	$(call safe_run,python map_to_msa.py $(DATA_DIR) $(AHA_OUTPUT))
	touch $(ADD_MSA_STAMP)

# assign entity_aha
clean_aha: $(CLEAN_AHA_STAMP)
$(CLEAN_AHA_STAMP): $(MAKE_AHA_STAMP) himss_to_aha.R
	$(call safe_run,Rscript himss_to_aha.R $(CODE_DIR) '$(R_USER_DIR)')
	touch $(CLEAN_AHA_STAMP)

# finalize cleaning aha numbers for both hospital and individual dfs
finalize_aha: $(FINALIZE_AHA_STAMP)
$(FINALIZE_AHA_STAMP): $(CLEAN_AHA_STAMP) assign_aha.R
	$(call safe_run,Rscript assign_aha.R $(CODE_DIR) '$(R_USER_DIR)')
	touch $(FINALIZE_AHA_STAMP)

#### MATCH AHA MADMIN TO HIMSS CONTACTS ####

# clean aha data to extract madmin (for use in multiple downstream files)
temp/cleaned_aha_madmin.csv temp/cleaned_aha.csv: input/aha_data_raw.csv
	Rscript clean_aha.R

# create aha to himss hospital-level summary statistics
hospital_sum_stats: $(HOSPITAL_SUMMARY_STAMP)
$(HOSPITAL_SUMMARY_STAMP): $(CLEAN_AHA_STAMP) temp/cleaned_aha.csv $(DERIVED_DIR)/hospitals_with_xwalk.feather \
	$(AUX_DIR)/haentity.feather $(SUPP_DIR)/hospital_ownership.dta sumstats_hospitals.R
	Rscript sumstats_hospitals.R  $(DERIVED_DIR)/hospitals_with_xwalk.feather $(AUX_DIR)/haentity.feather \
	$(SUPP_DIR)/hospital_ownership.dta $(USER_DIR)/_data/_outputs/hospitals
	touch $(HOSPITAL_SUMMARY_STAMP)

# create aha to himss summary statistics for ceos (from madmin)
ceo_sum_stats: $(CEO_SUMMARY_STAMP)
$(CEO_SUMMARY_STAMP): temp/cleaned_aha_madmin.csv $(DERIVED_DIR)/hospitals_with_xwalk.feather \
	$(DERIVED_DIR)/individuals_with_xwalk.feather $(HOSPITAL_SUMMARY_STAMP) aha_to_himss_ceos.R
	Rscript aha_to_himss_ceos.R $(DERIVED_DIR)/hospitals_with_xwalk.feather \
	$(DERIVED_DIR)/individuals_with_xwalk.feather $(SUPP_DIR) $(USER_DIR)/_data/_outputs/execs
	touch $(CEO_SUMMARY_STAMP)

# create aha to himss summary statistics for non-ceos (from madmin)
non_ceo_sum_stats: $(NON_CEO_SUMMARY_STAMP)
$(NON_CEO_SUMMARY_STAMP): temp/cleaned_aha_madmin.csv $(DERIVED_DIR)/hospitals_with_xwalk.feather \
	$(DERIVED_DIR)/individuals_with_xwalk.feather $(CEO_SUMMARY_STAMP) aha_to_himss_leadership.R
	Rscript aha_to_himss_leadership.R $(DERIVED_DIR)/hospitals_with_xwalk.feather \
	$(DERIVED_DIR)/individuals_with_xwalk.feather $(SUPP_DIR) $(USER_DIR)/_data/_outputs/execs
	touch $(NON_CEO_SUMMARY_STAMP)

# create aha to himss summary statistics for all madmin
madmin_sum_stats: $(MADMIN_SUMMARY_STAMP)
$(MADMIN_SUMMARY_STAMP): $(NON_CEO_SUMMARY_STAMP) aha_to_himss_all.R temp/cleaned_aha_madmin.csv \
	$(DERIVED_DIR)/hospitals_with_xwalk.feather $(DERIVED_DIR)/individuals_with_xwalk.feather \
	$(AUX_DIR)/matched_aha_ceos.feather $(AUX_DIR)/matched_aha_no_ceos.feather
	Rscript aha_to_himss_all.R $(DERIVED_DIR)/hospitals_with_xwalk.feather \
	$(DERIVED_DIR)/individuals_with_xwalk.feather $(SUPP_DIR) $(USER_DIR)/_data/_outputs/execs
	touch $(MADMIN_SUMMARY_STAMP)

# match himss observations to aha madmin observations and assign flag
himss_to_aha: $(HIMSS_TO_AHA_STAMP)
$(HIMSS_TO_AHA_STAMP): $(MADMIN_SUMMARY_STAMP) himss_to_aha_ceos.R temp/cleaned_aha_madmin.csv $(DERIVED_DIR)/hospitals_with_xwalk.feather \
	$(DERIVED_DIR)/individuals_with_xwalk.feather $(AUX_DIR)/matched_aha_ceos.feather $(AUX_DIR)/matched_aha_no_ceos.feather \
	$(MADMIN_SUMMARY_STAMP)
	Rscript himss_to_aha_ceos.R $(DERIVED_DIR)/hospitals_with_xwalk.feather \
	$(DERIVED_DIR)/individuals_with_xwalk.feather $(SUPP_DIR) $(USER_DIR)/_data/_outputs/execs
	touch $(HIMSS_TO_AHA_STAMP)

# add flag to himss df if individuals show up as madmin in AHA
assign_aha_flag: $(AHA_TITLE_FLAG_STAMP)
$(AHA_TITLE_FLAG_STAMP): $(HIMSS_TO_AHA_STAMP) assign_leadership_flag.R temp/cleaned_aha_madmin.csv temp/himss_to_aha_matches.feather \
	$(DERIVED_DIR)/hospitals_with_xwalk.feather $(DERIVED_DIR)/individuals_with_xwalk.feather $(HIMSS_TO_AHA_STAMP)
	Rscript assign_leadership_flag.R $(DERIVED_DIR)/hospitals_with_xwalk.feather \
	$(DERIVED_DIR)/individuals_with_xwalk.feather $(SUPP_DIR) $(USER_DIR)/_data/_outputs/execs
	touch $(AHA_TITLE_FLAG_STAMP)

# make master himss title file
make_title_xwalk: $(TITLE_MASTER_STAMP)
$(TITLE_MASTER_STAMP): $(AHA_TITLE_FLAG_STAMP)
	Rscript create_himss_title_master.R $(DERIVED_DIR)/hospitals_with_xwalk.feather \
	$(DERIVED_DIR)/individuals_final.feather $(SUPP_DIR) $(USER_DIR)/_data/_outputs/execs

# make final summary statistics using make_title_xwalk 
final_sum_stats: $(FINAL_SUMSTATS_STAMP)
$(FINAL_SUMSTATS_STAMP): $(TITLE_MASTER_STAMP)
	Rscript final_sum_stats.R $(DERIVED_DIR)/hospitals_with_xwalk.feather \
	$(DERIVED_DIR)/individuals_final.feather $(SUPP_DIR) $(USER_DIR)/_data/_outputs/execs

## specify paths
input/aha_data_raw.csv: | input
	@src="$(USER_DIR)/_data/supplemental/AHA_2004_2017.csv"; \
	ln -s "$$src" "$@"
