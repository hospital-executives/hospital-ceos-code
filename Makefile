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
USER_DIR := $(shell Rscript get_project_directory.R)

# Check if USER_DIR is empty
ifeq ($(strip $(USER_DIR)),)
$(error USER_DIR could not be determined. Please check get_project_directory.R)
endif

# Define Code Directory as the directory containing the Makefile
CODE_DIR := $(CURDIR)

# Define Data Directories using USER_DIR
DATA_DIR := $(USER_DIR)/_data
DERIVED_DIR := $(DATA_DIR)/derived
SUPP_DIR := $(DATA_DIR)/supplemental

# Define File Paths
# Manual paths
MANUAL_PATH := $(CODE_DIR)/manual_assignment.Rmd
HIMSS_ENTITIES_CONTACTS := $(DERIVED_DIR)/himss_entities_contacts_0517_v1.feather
OUTLIERS := $(DERIVED_DIR)/auxiliary/outliers.csv
CONFIRMED_R := $(DERIVED_DIR)/r_confirmed.feather
REMAINING_R := $(DERIVED_DIR)/r_remaining.feather

# Clean data paths
CONFIRMED_1 := $(DERIVED_DIR)/auxiliary/confirmed_1.csv
REMAINING_1 := $(DERIVED_DIR)/auxiliary/remaining_1.csv
HIMSS_1 := $(DERIVED_DIR)/auxiliary/himss_1.csv
HIMSS_NICKNAMES := $(DERIVED_DIR)/auxiliary/himss_nicknames.csv

# Updated gender path
R_GENDER := $(DATA_DIR)/nicknames dictionaries/gender_test.csv
UPDATED_GENDER := $(DERIVED_DIR)/auxiliary/updated_gender.csv

# Jaro path
JARO := $(DERIVED_DIR)/auxiliary/jaro_output.csv

# Digraph paths
DIGRAPHNAME := $(DERIVED_DIR)/auxiliary/digraphname.csv
DIGRAPHMETA := $(DERIVED_DIR)/auxiliary/digraphmeta.csv

# Block paths
CONFIRMED_2 := $(DERIVED_DIR)/auxiliary/confirmed_2.csv
REMAINING_2 := $(DERIVED_DIR)/auxiliary/remaining_2.csv

# Cleaned paths
FINAL_CLEANED := $(DERIVED_DIR)/py_confirmed.csv
FINAL_REMAINING := $(DERIVED_DIR)/py_remaining.csv
GRAPH_COMPS := $(DERIVED_DIR)/py_graph_components.json

# Final output
FINAL_HIMSS := $(DERIVED_DIR)/final_himss.feather
FINAL_CONFIRMED := $(DERIVED_DIR)/final_confirmed.dta

# Other
PANDOC_PATH := /Applications/RStudio.app/Contents/Resources/app/quarto/bin/tools/x86_64

# Define phony targets
.PHONY: all compile_himss manual_assignment clean_data update_gender \
generate_jaro generate_digraphs generate_blocks generate_final

# Default target
all: final_himss

# Enforce execution order via phony dependencies
final_himss: generate_final

generate_final: generate_blocks

generate_blocks: generate_digraphs

generate_digraphs: generate_jaro

generate_jaro: update_gender

update_gender: clean_data

clean_data: manual_assignment

manual_assignment: compile_himss

# Step 1: Compile HIMSS data
compile_himss:
	export RSTUDIO_PANDOC=$(PANDOC_PATH); \
	Rscript -e "rmarkdown::render('compile_himss.Rmd', \
	params = list(code_dir = '$(CODE_DIR)', r_gender = '$(R_GENDER)'))"

# Step 2: Manual assignment
manual_assignment:
	export RSTUDIO_PANDOC=$(PANDOC_PATH); \
	Rscript -e "rmarkdown::render(\"manual_assignment.Rmd\", \
	params = list(code_path = \"$(CODE_DIR)\", \
	data_path = \"$(DATA_DIR)\", \
	himss_entity_contacts = \"$(HIMSS_ENTITIES_CONTACTS)\"))"

# Step 3: Clean data
clean_data:
	python3 clean_data.py "$(CONFIRMED_R)" "$(REMAINING_R)" \
	"$(HIMSS_ENTITIES_CONTACTS)" "$(CONFIRMED_1)" "$(REMAINING_1)" \
	"$(HIMSS_1)" "$(HIMSS_NICKNAMES)"

# Step 4: Update gender
update_gender:
	python3 helper-scripts/update_gender.py "$(CONFIRMED_1)" "$(UPDATED_GENDER)" \
	"$(DATA_DIR)" "$(R_GENDER)"

# Step 5: Generate Jaro
generate_jaro:
	python3 helper-scripts/jaro_algo.py "$(CODE_DIR)" "$(CONFIRMED_R)" \
	"$(UPDATED_GENDER)" "$(JARO)"

# Step 6: Generate digraphs
generate_digraphs:
	python3 helper-scripts/generate_digraphs.py "$(HIMSS_1)" "$(HIMSS_NICKNAMES)" \
	"$(DIGRAPHNAME)" "$(DIGRAPHMETA)" "$(SUPP_DIR)"

# Step 7: Generate blocks
generate_blocks:
	python3 generate_blocks.py "$(HIMSS_ENTITIES_CONTACTS)" "$(CONFIRMED_1)" \
	"$(REMAINING_1)" "$(UPDATED_GENDER)" "$(HIMSS_1)" "$(CONFIRMED_2)" \
	"$(REMAINING_2)" "$(DATA_DIR)"

# Step 8: Generate final cleaned data
generate_final:
	python3 -u generate_clean_df.py "$(CONFIRMED_2)" "$(REMAINING_2)" \
	"$(FINAL_CLEANED)" "$(FINAL_REMAINING)" "$(GRAPH_COMPS)" \
	"$(DATA_DIR)" "$(CODE_DIR)"

# Step 9: Generate final HIMSS dataset and cleaned dta with flags
final_himss:
	python3 generate_final.py "$(FINAL_CLEANED)" "$(FINAL_REMAINING)" \
	"$(GRAPH_COMPS)" "$(FINAL_HIMSS)" "$(FINAL_CONFIRMED)" \
	"$(HIMSS_ENTITIES_CONTACTS)"

# debugging code
debug:
	python3 generate_final.py "$(FINAL_CLEANED)" "$(FINAL_REMAINING)" \
	"$(GRAPH_COMPS)" "$(FINAL_HIMSS)" "$(FINAL_CONFIRMED)" \
	"$(HIMSS_ENTITIES_CONTACTS)"

# /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/


# to run to generate blocks: 
#make /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/auxiliary/confirmed_2.csv /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/auxiliary/remaining_2.csv
# confirmed r: 
# /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/r_confirmed.feather
# updated gender:
# /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/auxiliary/updated_gender.csv
# make final cleand df

# make -B /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/derived/himss_entities_contacts_0517_v1.feather /Users/loaner/BFI\ Dropbox/Katherine\ Papen/hospital_ceos/_data/supplemental/gender.csv