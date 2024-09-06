
# Define Code Directory

# Define Data Directories
USER_DIR := /Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos
DATA_DIR := $(USER_DIR)/_data
DERIVED_DIR := $(DATA_DIR)/derived
SUPP_DIR := $(DATA_DIR)/supplemental

# Define targets
HIMSS_ENTITIES_CONTACTS := $(DERIVED_DIR)/himss_entities_contacts_0517_v1.feather

INPUT_PATH = /Users/loaner/hospital-coes-code/
PANDOC_PATH = /Applications/RStudio.app/Contents/Resources/app/quarto/bin/tools/x86_64

run_rmd:
	Rscript -e "Sys.setenv(RSTUDIO_PANDOC='$(PANDOC_PATH)'); \
	rmarkdown::render('test.Rmd', \
	params = list(input_path = '$(INPUT_PATH)'))"

clean:
	rm -f test.html test.pdf test.md
	rm -rf test_files/