
USER_DIR := $(strip $(shell Rscript ../../build_code/get_makefile_paths.R))
R_USER_DIR := $(shell Rscript ../../build_code/get_project_directory.R)
HOSPITAL_SUM_STATS := ../output/hospitals/merge_status_by_year_type_1.png \
../output/hospitals/merge_status_by_year_type_1_and_2.png \
../output/hospitals/hospital_counts_by_year_type_1.png \
../output/hospitals/hospital_counts_by_year_type_1_and_2.png \
../output/hospitals/entity_match.png ../output/hospitals/entity_match_by_year.png \
../output/hospitals/type_breakdown_comparison.tex

## Clean data
../temp/cleaned_aha_madmin.csv: ../input/aha_data_raw.csv ../temp
	Rscript clean_aha.R ../input/aha_data_raw.csv ../temp

## Generate hospital-level summary statistics in ../output/hospitals
hospital_sum_stats: $(HOSPITAL_SUM_STATS)
$(HOSPITAL_SUM_STATS): ../temp/cleaned_aha.csv ../output/hospitals ../input/hospitals_final.feather ../input/haentity.feather ../input/hospital_ownership.dta
	Rscript hospital_sum_stats.R  ../temp/cleaned_aha.csv ../output/hospitals ../input/hospitals_final.feather ../input/haentity.feather ../input/hospital_ownership.dta

## Generate AHA CEO to HIMSS sumary statistics
../output/aha_to_himss.csv: ../temp/cleaned_aha_madmin.csv ../input/hospitals_final.feather ../input/individuals_final.feather ../input/supplemental_path
	Rscript aha_to_himss_ceos.R ../temp/cleaned_aha_madmin.csv ../input/hospitals_final.feather ../input/individuals_final.feather ../input/supplemental_path

## - Inputs 
../input/aha_data_raw.csv: | ../input
	@src="$(USER_DIR)/_data/supplemental/AHA_2004_2017.csv"; \
	ln -s "$$src" "$@"

../input/hospitals_final.feather: | ../input
	@src="$(USER_DIR)/_data/derived/himss_aha_hospitals_final.feather"; \
	ln -s "$$src" "$@"

../input/individuals_final.feather: | ../input
	@src="$(USER_DIR)/_data/derived/final_confirmed_aha.feather"; \
	ln -s "$$src" "$@"

../input/haentity.feather: | ../input
	@src="$(USER_DIR)/_data/derived/auxiliary/haentity.feather"; \
	ln -s "$$src" "$@"

../input/config_path.R: | ../input
	ln -s "../../build_code/config.R" "$@"

../input/hospital_ownership.dta: | ../input
	@src="$(USER_DIR)/_data/supplemental/hospital_ownership.dta"; \
	ln -s "$$src" "$@"

../input/supplemental_path: | ../input
	@src="$(USER_DIR)/_data/supplemental"; \
	ln -s "$$src" "$@"

../input:
	mkdir -p "$@"

../temp:
	mkdir -p "$@"

../output:
	mkdir -p "$@"

../output/hospitals: | ../output
	mkdir -p "$@"