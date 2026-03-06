*----------------------------------------------------------
* Sample size report: treated & control entities
* by outcome variable and binary split
*----------------------------------------------------------

* check setup
	check_setup

*----------------------------------------------------------
* Load data and restrict to correct sample
*----------------------------------------------------------
	use "${dbdata}/derived/temp/merged_ma_sysid_xwalk.dta", clear
	
* merge in type 
preserve
	use "${dbdata}/derived/temp/merged_ma_nonharmonized.dta", clear
	keep entity_uniqueid year type
	
	tempfile himss_type_xwalk
	save `himss_type_xwalk'
restore

merge 1:1 entity_uniqueid year using `himss_type_xwalk', assert(master match) keep(match) nogen

* restrict to hospital sample
restrict_hosp_sample

* will need to revisit for the cases where we're dropping observations still
merge 1:1 entity_uniqueid year using "${dbdata}/derived/hospitals_with_turnover.dta", keep(match) nogen

* ── Outcomes ──────────────────────────────────────────────
local turnover_outcomes "turnover_ceo turnover_cfo turnover_coo turnover_cmo turnover_cno turnover_cco turnover_cio"
local vacancy_outcomes  "vacant_ceo vacant_cfo vacant_coo vacant_cmo vacant_cno vacant_cco vacant_cio"
local all_outcomes "`turnover_outcomes' `vacancy_outcomes'"

* ── Outcome nice labels ────────────────────────────────────
local outnice_turnover_ceo "CEO Turnover"
local outnice_turnover_cfo "CFO Turnover"
local outnice_turnover_coo "COO Turnover"
local outnice_turnover_cmo "CMO Turnover"
local outnice_turnover_cno "CNO Turnover"
local outnice_turnover_cco "CCO Turnover"
local outnice_turnover_cio "CIO Turnover"
local outnice_vacant_ceo   "CEO Vacancy"
local outnice_vacant_cfo   "CFO Vacancy"
local outnice_vacant_coo   "COO Vacancy"
local outnice_vacant_cmo   "CMO Vacancy"
local outnice_vacant_cno   "CNO Vacancy"
local outnice_vacant_cco   "CCO Vacancy"
local outnice_vacant_cio   "CIO Vacancy"

* ── Splits ────────────────────────────────────────────────
local vars aha_bdtot_orig aha_mcddc aha_mcrdc aha_fte

foreach v of local vars {
    local suffix : subinstr local v "aha_" "", all

    * Compute each entity's median value of v across years
    bys aha_id: egen entity_med_`suffix' = median(`v')

    * Compute the overall median of the entity-level medians
    quietly summarize entity_med_`suffix', detail
    local med = r(p50)

    * Classify entity as above/below based on their entity-level median
    gen above_median_`suffix' = (entity_med_`suffix' > `med')

    drop entity_med_`suffix'
}

// Prespecify your binary variables and labels
gen fpstatus = .
replace fpstatus = 1 if aha_own_fp == 1 & aha_own_np == 0
replace fpstatus = 0 if aha_own_fp == 0 & aha_own_np == 1


local binvar1 "fpstatus"
local binvar2 "above_median_bdtot_orig"
local binvar3 "above_median_mcddc"
local binvar4 "above_median_mcrdc"
local binvar5 "above_median_fte"
local binvar6 "aha_teaching"
local binvar7 "aha_cah"

local binname1 "fp"
local binname2 "bdtot"
local binname3 "mcddc"
local binname4 "mcr"
local binname5 "fte"
local binname6 "teaching"
local binname7 "cah"

local label1_0 "NFP"             
local label1_1 "FP"
local label2_0 "Below Median Bed Count" 
local label2_1 "Above Median Bed Count"
local label3_0 "Below Median Medicaid"  
local label3_1 "Above Median Medicaid"
local label4_0 "Below Median Medicare"  
local label4_1 "Above Median Medicare"
local label5_0 "Below Median FTE"       
local label5_1 "Above Median FTE"
local label6_0 "Not Teaching"           
local label6_1 "Teaching"
local label7_0 "Not CAH"                
local label7_1 "CAH"

local splitnice1 "Ownership Type"
local splitnice2 "Bed Count"
local splitnice3 "Medicaid"
local splitnice4 "Medicare"
local splitnice5 "FTE"
local splitnice6 "Teaching Status"
local splitnice7 "CAH Status"

local n_splits 7

* ── Postfile setup ────────────────────────────────────────
tempname memhold
tempfile results

postfile `memhold' ///
    str40 outcome ///
    str40 split_name ///
    str40 group_label ///
    int   n_treated ///
    int   n_control ///
    using `results', replace

* ── Main loop ─────────────────────────────────────────────
foreach outcome of local all_outcomes {

    display _newline(2) as result "Outcome: `outcome'"
    display as result "{hline 50}"

    local outnice "`outnice_`outcome''"

    * ── Aggregate (no split) ──────────────────────────────
    preserve
    keep if !missing(`outcome')
    make_target_sample

    * Treated entities
    quietly levelsof entity_uniqueid if balanced_2_year_sample == 1, local(tmp)
    local n_t : word count `tmp'

    * Control entities
    quietly levelsof entity_uniqueid if never_tar == 1, local(tmp)
    local n_c : word count `tmp'

    display as text "  [Aggregate] Treated: `n_t'  |  Control: `n_c'"
    post `memhold' ("`outnice'") ("Aggregate") ("All") (`n_t') (`n_c')

    restore

    * ── By split ──────────────────────────────────────────
    forvalues s = 1/`n_splits' {

        local splitvar  "`binvar`s''"
        local splitnice "`splitnice`s''"

        forvalues g = 0/1 {

            local grp_label "`label`s'_`g''"

            preserve
            keep if !missing(`outcome') & `splitvar' == `g'
            make_target_sample

            quietly levelsof entity_uniqueid if balanced_2_year_sample == 1, local(tmp)
            local n_t : word count `tmp'

            quietly levelsof entity_uniqueid if never_tar == 1, local(tmp)
            local n_c : word count `tmp'

            display as text "  [`splitnice' = `grp_label'] Treated: `n_t'  |  Control: `n_c'"
            post `memhold' ("`outnice'") ("`splitnice'") ("`grp_label'") (`n_t') (`n_c')

            restore
        }
    }
}

postclose `memhold'

* ── Format and export ─────────────────────────────────────

local sname1 "Ownership Type"
local sname2 "Bed Count"
local sname3 "Medicaid"
local sname4 "Medicare"
local sname5 "FTE"
local sname6 "Teaching Status"
local sname7 "CAH Status"

local sfile1 "ownership_type"
local sfile2 "bed_count"
local sfile3 "medicaid"
local sfile4 "medicare"
local sfile5 "fte"
local sfile6 "teaching_status"
local sfile7 "cah_status"

* ── Display full table in Stata ───────────────────────────
use `results', clear
list outcome split_name group_label n_treated n_control, ///
    separator(0) noobs abbreviate(20)

* ── Aggregate table ───────────────────────────────────────
use `results', clear
keep if split_name == "Aggregate"
drop split_name group_label

export delimited using "${overleaf}/notes/Non CEO Event Study/tables/sample_counts_aggregate.csv", ///
    replace

listtab outcome n_treated n_control ///
    using "${overleaf}/notes/Non CEO Event Study/tables/sample_counts_aggregate.tex", ///
    rstyle(tabular) ///
    head("\begin{table}[H]" ///
         "\centering" ///
         "\caption{Sample Counts -- Aggregate}" ///
         "\label{tab:sample_counts_aggregate}" ///
         "\begin{tabular}{lrr}" ///
         "\hline\hline" ///
         "Outcome & N Treated & N Control \\\\") ///
    foot("\hline\hline" ///
         "\end{tabular}" ///
         "\end{table}") ///
    replace

* ── One table per split ───────────────────────────────────
forvalues s = 1/7 {

    use `results', clear
    keep if split_name == "`sname`s''"
    drop split_name

    export delimited using "${overleaf}/notes/Non CEO Event Study/tables/sample_counts_`sfile`s''.csv", ///
        replace

    listtab outcome group_label n_treated n_control ///
        using "${overleaf}/notes/Non CEO Event Study/tables/sample_counts_`sfile`s''.tex", ///
        rstyle(tabular) ///
        head("\begin{table}[H]" ///
             "\centering" ///
             "\caption{Sample Counts -- `sname`s''}" ///
             "\label{tab:sample_counts_`sfile`s''}" ///
             "\begin{tabular}{llrr}" ///
             "\hline\hline" ///
             "Outcome & Group & N Treated & N Control \\\\") ///
        foot("\hline\hline" ///
             "\end{tabular}" ///
             "\end{table}") ///
        replace
}

display _newline(2) as result "Sample count report complete."
