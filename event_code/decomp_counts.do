
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
merge 1:1 entity_uniqueid year using "${dbdata}/derived/positions_by_tier.dta", keep(match) nogen


********************************************************************************
* FOREST PLOTS BY TIER AND GROUP 
********************************************************************************


*-------------------------------------------------------------------------------
* STEP 1: Define role mappings with clean labels
*-------------------------------------------------------------------------------

// Define as paired lists: variable prefix | display label
// Tier 1 Business
local t1b_vars    "ceo cfo coo"
local t1b_n = 3
local t1b_lab1 "CEO"
local t1b_lab2 "CFO"  
local t1b_lab3 "COO"
local t1b_title "Tier 1: Business"

// Tier 1 Clinical
local t1c_vars    "medical_staff_chief chief_nursing_head"
local t1c_n = 2
local t1c_lab1 "Medical Staff Chief"
local t1c_lab2 "Chief Nursing Head"
local t1c_title "Tier 1: Clinical"

// Tier 1 IT/Legal/HR
local t1i_vars    "cio chief_compliance_officer csio_it_security_officer"
local t1i_n = 3
local t1i_lab1 "CIO"
local t1i_lab2 "Chief Compliance Officer"
local t1i_lab3 "CSIO/IT Security Officer"
local t1i_title "Tier 1: IT/Legal/HR"

// Tier 2 Business
local t2b_vars    "business_office_head marketing_head purchasing_head patient_accounting"
local t2b_n = 4
local t2b_lab1 "Business Office Head"
local t2b_lab2 "Marketing Head"
local t2b_lab3 "Purchasing Head"
local t2b_lab4 "Patient Accounting"
local t2b_title "Tier 2: Business"

// Tier 2 Clinical
local t2c_vars    "quality_head ob_head cardiology_head er_director or_head ambulatory_care_head patient_safety_head pathology_chief laboratory_director pharmacy_head radiology_med_dir"
local t2c_n = 11
local t2c_lab1  "Quality Head"
local t2c_lab2  "OB Head"
local t2c_lab3  "Cardiology Head"
local t2c_lab4  "ER Director"
local t2c_lab5  "OR Head"
local t2c_lab6  "Ambulatory Care Head"
local t2c_lab7  "Patient Safety Head"
local t2c_lab8  "Pathology Chief"
local t2c_lab9  "Laboratory Director"
local t2c_lab10 "Pharmacy Head"
local t2c_lab11 "Radiology Med Dir"
local t2c_title "Tier 2: Clinical"

// Tier 2 IT/Legal/HR
local t2i_vars    "it_director hr_head him_director facility_management_head director_of_technology clinical_systems_director"
local t2i_n = 6
local t2i_lab1 "IT Director"
local t2i_lab2 "HR Head"
local t2i_lab3 "HIM Director"
local t2i_lab4 "Facility Management Head"
local t2i_lab5 "Director of Technology"
local t2i_lab6 "Clinical Systems Director"
local t2i_title "Tier 2: IT/Legal/HR"

// All tier-group codes
local all_tg "t1b t1c t1i t2b t2c t2i"

// Outcome types
local outcome_types "active vacant dne"
local outcome_lab_active  "Active Position"
local outcome_lab_vacant  "Vacant Position"
local outcome_lab_dne     "Position Does Not Exist"

*-------------------------------------------------------------------------------
* Sample counts by tier-group, role, and outcome type
*-------------------------------------------------------------------------------

tempname cnt_hold
tempfile cnt_results

postfile `cnt_hold' ///
    str10  tg_code       ///
    str50  tg_title      ///
    str50  role_var      ///
    str50  role_label    ///
    str10  outcome_type  ///
    int    n_treated     ///
    int    n_control     ///
    using `cnt_results', replace

foreach tg of local all_tg {

    local roles    ``tg'_vars'
    local n_roles  ``tg'_n'
    local tg_title "``tg'_title'"

    local i = 0
    foreach role of local roles {
        local ++i
        local role_label "``tg'_lab`i''"

        foreach outcome_type of local outcome_types {

            local outcome_var "`role'_`outcome_type'"

            * Check variable exists
            capture confirm variable `outcome_var'
            if _rc != 0 {
                display as text "  Skipping `outcome_var' (not found)"
                continue
            }

            preserve
            keep if !missing(`outcome_var')
            make_target_sample

            quietly levelsof entity_uniqueid if balanced_2_year_sample == 1, local(tmp)
            local n_t : word count `tmp'

            quietly levelsof entity_uniqueid if never_tar == 1, local(tmp)
            local n_c : word count `tmp'

            display as text "  `tg_title' | `role_label' | `outcome_type': Treated=`n_t'  Control=`n_c'"

            post `cnt_hold' ///
                ("`tg'") ("`tg_title'") ("`role'") ("`role_label'") ///
                ("`outcome_type'") (`n_t') (`n_c')

            restore
        }
    }
}

postclose `cnt_hold'

*-------------------------------------------------------------------------------
* Export
*-------------------------------------------------------------------------------

use `cnt_results', clear

* Display in Stata
list tg_title role_label outcome_type n_treated n_control, ///
    separator(0) noobs abbreviate(25)

* CSV
export delimited using "${overleaf}/notes/Non CEO Event Study/tables/sample_counts_by_tier.csv", ///
    replace

* One LaTeX table per tier-group
foreach tg of local all_tg {

    use `cnt_results', clear
    keep if tg_code == "`tg'"
    drop tg_code tg_title role_var

    local tg_title "``tg'_title'"

    listtab role_label outcome_type n_treated n_control ///
        using "${overleaf}/notes/Non CEO Event Study/tables/sample_counts_`tg'.tex", ///
        rstyle(tabular) ///
        head("\begin{table}[H]" ///
             "\centering" ///
             "\caption{Sample Counts -- `tg_title'}" ///
             "\label{tab:sample_counts_`tg'}" ///
             "\begin{tabular}{llrr}" ///
             "\hline\hline" ///
             "Role & Outcome & N Treated & N Control \\\\") ///
        foot("\hline\hline" ///
             "\end{tabular}" ///
             "\end{table}") ///
        replace
}

display _newline(2) as result "Tier sample count report complete."