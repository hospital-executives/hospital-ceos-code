USER_DIR := $(shell Rscript ../build_code/get_makefile_paths.R)
R_USER_DIR := $(shell Rscript ../build_code/get_project_directory.R)
CODE_DIR := $(CURDIR)

# Define paths
STAMP_DIR := $(DERIVED_DIR)/stamps
TURNOVER := $(USER_DIR)/_data/derived/hospitals_with_turnover.dta

all: $(TURNOVER)

# define recipes
$(TURNOVER): non_ceo_turnover.R #$(STAMP_DIR)/final_individual.stamp $(STAMP_DIR)/title_master.stamp
	Rscript non_ceo_turnover.R