USER_DIR := $(shell Rscript ../build_code/get_project_directory.R)
DERIVED_DIR := $(USER_DIR)/_data/derived
FIGURES_DIR := /Users/katherinepapen/Library/CloudStorage/Dropbox/Apps/Overleaf/Hospital\ CEOs/notes/Title\ descriptives/figures

COMMON_CONFIG := ../build_code/config.R
SAMPLE_INPUTS := $(DERIVED_DIR)/temp/merged_ma_sysid_xwalk.dta $(DERIVED_DIR)/temp/merged_ma_nonharmonized.dta

CONCAT_OUT := $(DERIVED_DIR)/temp/individuals_contact_corporate.dta
TITLES_OUT := title_groups.xlsx
TURNOVER_OUT := $(DERIVED_DIR)/hospitals_with_turnover.dta
POSITION_OUT := $(DERIVED_DIR)/positions_by_tier.dta
POSITION_PNGS := \
	$(FIGURES_DIR)/profit_status_forest.png \
	$(FIGURES_DIR)/system_status_forest.png \
	$(FIGURES_DIR)/profit_status_unclear_forest.png \
	$(FIGURES_DIR)/system_status_unclear_forest.png

all: $(CONCAT_OUT) $(TITLES_OUT) $(TURNOVER_OUT) $(POSITION_OUT) $(POSITION_PNGS)

$(CONCAT_OUT): $(COMMON_CONFIG) $(SAMPLE_INPUTS) $(DERIVED_DIR)/hospitals_with_xwalk.feather $(DERIVED_DIR)/individuals_final.feather concat_individuals.R
	Rscript concat_individuals.R

$(TITLES_OUT): $(COMMON_CONFIG) $(SAMPLE_INPUTS) $(DERIVED_DIR)/hospitals_final.feather $(DERIVED_DIR)/individuals_final.feather get_titles.R
	Rscript get_titles.R

$(TURNOVER_OUT): $(COMMON_CONFIG) $(DERIVED_DIR)/hospitals_final.feather $(DERIVED_DIR)/individuals_final.feather non_ceo_turnover.R
	Rscript non_ceo_turnover.R

$(POSITION_OUT): $(COMMON_CONFIG) $(SAMPLE_INPUTS) $(DERIVED_DIR)/temp/systems_nonharmonized_withprofit.dta $(DERIVED_DIR)/hospitals_with_xwalk.feather $(DERIVED_DIR)/individuals_final.feather position_status.R
	Rscript position_status.R

$(POSITION_PNGS): $(POSITION_OUT)
