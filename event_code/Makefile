STATA ?= stata-mp
RUNNER := run_with_setup.do

USER_DIR := $(shell Rscript ../build_code/get_makefile_paths.R)
DERIVED_DIR := $(USER_DIR)/_data/derived
OVERLEAF_DIR := /Users/katherinepapen/Library/CloudStorage/Dropbox/Apps/Overleaf/Hospital\ CEOs
EVENT_NOTES_DIR := $(OVERLEAF_DIR)/notes/Non\ CEO\ Event\ Study
FIGURES_DIR := $(EVENT_NOTES_DIR)/figures
TABLES_DIR := $(EVENT_NOTES_DIR)/tables

SETUP_DEPS := 00_master.do 01_set_globals.do $(RUNNER)
COMMON_INPUTS := $(DERIVED_DIR)/temp/merged_ma_sysid_xwalk.dta $(DERIVED_DIR)/temp/merged_ma_nonharmonized.dta
TURNOVER_INPUTS := $(COMMON_INPUTS) $(DERIVED_DIR)/hospitals_with_turnover.dta
POSITIONS_INPUTS := $(COMMON_INPUTS) $(DERIVED_DIR)/positions_by_tier.dta

TURNOVER_OUTCOMES := turnover_ceo turnover_cfo turnover_coo turnover_cmo turnover_cno turnover_cco turnover_cio vacant_ceo vacant_cfo vacant_coo vacant_cmo vacant_cno vacant_cco vacant_cio
TURNOVER_SPECS := 2yrbalanced_never_tar 3yrbalanced_never_tar 2yrbalanced_never_m_and_a 3yrbalanced_never_m_and_a
TURNOVER_OUTPUTS := $(TABLES_DIR)/pre_treatment_means.tex $(foreach s,$(TURNOVER_SPECS),$(foreach o,$(TURNOVER_OUTCOMES),$(FIGURES_DIR)/$(s)_$(o).pdf))

HET_SPLITS := fp bdtot mcddc mcr fte teaching cah
TURNOVER_HET_OUTPUTS := $(foreach fam,turnover vacancy,$(foreach scale,raw norm,$(foreach split,$(HET_SPLITS),$(FIGURES_DIR)/forest_$(fam)_$(scale)_$(split).png)))

INDIV_SPLITS := gender md
INDIV_HET_OUTPUTS := \
	$(FIGURES_DIR)/counts_turnover_gender.csv \
	$(FIGURES_DIR)/counts_turnover_gender.tex \
	$(FIGURES_DIR)/counts_turnover_md.csv \
	$(FIGURES_DIR)/counts_turnover_md.tex \
	$(FIGURES_DIR)/forest_turnover_raw_gender.png \
	$(FIGURES_DIR)/forest_turnover_norm_gender.png \
	$(FIGURES_DIR)/forest_turnover_raw_md.png \
	$(FIGURES_DIR)/forest_turnover_norm_md.png

ROLES_OUTCOMES := business1_sh_active business2_sh_active clinical1_sh_active clinical2_sh_active itlegalhr1_sh_active itlegalhr2_sh_active business1_sh_vacant business2_sh_vacant clinical1_sh_vacant clinical2_sh_vacant itlegalhr1_sh_vacant itlegalhr2_sh_vacant business1_sh_dne business2_sh_dne clinical1_sh_dne clinical2_sh_dne itlegalhr1_sh_dne itlegalhr2_sh_dne business1_people_per_role business2_people_per_role clinical1_people_per_role clinical2_people_per_role itlegalhr1_people_per_role itlegalhr2_people_per_role
ROLES_BY_TIER_OUTPUTS := \
	$(TABLES_DIR)/pre_treatment_means.tex \
	$(foreach o,$(ROLES_OUTCOMES),$(FIGURES_DIR)/file'_$(o).pdf) \
	$(FIGURES_DIR)/forest_sh_active.pdf \
	$(FIGURES_DIR)/forest_sh_vacant.pdf \
	$(FIGURES_DIR)/forest_sh_dne.pdf \
	$(FIGURES_DIR)/wgt_forest_sh_active.pdf \
	$(FIGURES_DIR)/wgt_forest_sh_vacant.pdf \
	$(FIGURES_DIR)/wgt_forest_sh_dne.pdf

TG_CODES := t1b t1c t1i t2b t2c t2i
OUTCOME_TYPES := active vacant dne
TIER_DECOMP_OUTPUTS := $(foreach tg,$(TG_CODES),$(foreach out,$(OUTCOME_TYPES),$(FIGURES_DIR)/forest_$(tg)_$(out).pdf) $(FIGURES_DIR)/forest_$(tg)_combined.pdf)
WGT_TIER_DECOMP_OUTPUTS := $(foreach tg,$(TG_CODES),$(foreach out,$(OUTCOME_TYPES),$(FIGURES_DIR)/wgt_forest_$(tg)_$(out).pdf))

TURNOVER_STAMP := temp/turnover.done
TURNOVER_HET_STAMP := temp/turnover_heterogeneity.done
INDIV_HET_STAMP := temp/indiv_heterogeneity.done
ROLES_STAMP := temp/roles_by_tier.done
TIER_DECOMP_STAMP := temp/tier_decomposition.done
WGT_TIER_DECOMP_STAMP := temp/wgt_tier_decomposition.done
DNE_STAMP := temp/dne.done

all: dne

turnover: $(TURNOVER_STAMP)
turnover_heterogeneity: $(TURNOVER_HET_STAMP)
indiv_heterogeneity: $(INDIV_HET_STAMP)
roles_by_tier: $(ROLES_STAMP)
tier_decomposition: $(TIER_DECOMP_STAMP)
tier_descomposition: $(TIER_DECOMP_STAMP)
wgt_tier_decomposition: $(WGT_TIER_DECOMP_STAMP)
dne: $(DNE_STAMP)

$(TURNOVER_STAMP): $(SETUP_DEPS) turnover.do $(TURNOVER_INPUTS)
	$(STATA) -b do $(RUNNER) turnover.do
	@touch $@

$(TURNOVER_HET_STAMP): $(TURNOVER_STAMP) $(SETUP_DEPS) turnover_heterogeneity.do $(TURNOVER_INPUTS)
	$(STATA) -b do $(RUNNER) turnover_heterogeneity.do
	@touch $@

$(INDIV_HET_STAMP): $(TURNOVER_HET_STAMP) $(SETUP_DEPS) indiv_heterogeneity.do $(TURNOVER_INPUTS)
	$(STATA) -b do $(RUNNER) indiv_heterogeneity.do
	@touch $@

$(ROLES_STAMP): $(INDIV_HET_STAMP) $(SETUP_DEPS) roles_by_tier.do $(POSITIONS_INPUTS)
	$(STATA) -b do $(RUNNER) roles_by_tier.do
	@touch $@

$(TIER_DECOMP_STAMP): $(ROLES_STAMP) $(SETUP_DEPS) tier_decomposition.do $(POSITIONS_INPUTS)
	$(STATA) -b do $(RUNNER) tier_decomposition.do
	@touch $@

$(WGT_TIER_DECOMP_STAMP): $(TIER_DECOMP_STAMP) $(SETUP_DEPS) wgt_tier_decomposition.do $(POSITIONS_INPUTS)
	$(STATA) -b do $(RUNNER) wgt_tier_decomposition.do
	@touch $@

$(DNE_STAMP): $(WGT_TIER_DECOMP_STAMP) $(SETUP_DEPS) dne.do $(POSITIONS_INPUTS) $(DERIVED_DIR)/individuals_final.feather.dta
	$(STATA) -b do $(RUNNER) dne.do
	@touch $@

.PHONY: all turnover turnover_heterogeneity indiv_heterogeneity roles_by_tier tier_decomposition tier_descomposition wgt_tier_decomposition dne
