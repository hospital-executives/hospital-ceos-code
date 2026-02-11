/* EVENT_STUDY_STATS *************************************************************

Program name: 	non_ceo_turnover.do
Programmer: 	Katherine Papen

Goal: 			Generate event study plots for non-CEO turnover measures

*******************************************************************************/

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

make_target_sample


*----------------------------------------------------------
* Define relative time indicators
*----------------------------------------------------------

sum tar_reltime, meanonly
local rmin = r(min)
local rmax = r(max)

* Create Relative Time Indicators
forvalues h = 0/`rmax' {
    gen byte ev_lag`h' = (tar_reltime == `h')
}
forvalues h = 1/`=abs(`rmin')' {
    gen byte ev_lead`h' = (tar_reltime == -`h')
}
replace ev_lead1 = 0

********************************************************************************
* FOREST PLOTS BY TIER AND GROUP - IMPROVED VERSION
********************************************************************************

// Install labmask if needed
// ssc install labmask

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
* STEP 2: Run event studies and store results
*-------------------------------------------------------------------------------
local spec1_treated "balanced_2_year_sample == 1"
local spec1_control "never_tar == 1"
local spec1_cohort "never_tar"
local spec1_name "2yr Balanced, Never Treated"
local spec1_file "2yrbalanced_never_tar"

local nspecs = 1

// Create results file
tempfile results
postfile handle str10 tg_code str50 role_var str30 role_label str10 outcome_type ///
         double avg_effect double avg_se int spec_num double pre_mean ///
         using `results', replace

forvalues s = 1/`nspecs' {
    
    foreach tg of local all_tg {
        
        local roles ``tg'_vars'
        local n_roles ``tg'_n'
        
        foreach outcome_type of local outcome_types {
            
            local i = 0
            foreach role of local roles {
                local ++i
                
                local outcome_var "`role'_`outcome_type'"
                local role_label "``tg'_lab`i''"
                
                // Check variable exists
                capture confirm variable `outcome_var'
                if _rc != 0 {
                    display as text "  Skipping `outcome_var' (not found)"
                    continue
                }
                
                display as result _newline "Spec `s' | `tg' | `role_label' | `outcome_type'"
                
                // Run event study
                capture noisily {
                    eventstudyinteract `outcome_var' ev_lead* ev_lag* ///
                        if (`spec`s'_treated' | `spec`s'_control'), ///
                        vce(cluster entity_uniqueid) ///
                        absorb(entity_uniqueid year) ///
                        cohort(tar_event_year) ///
                        control_cohort(`spec`s'_cohort')
                    
                    matrix b = e(b_iw)
                    matrix V = e(V_iw)
                    ereturn post b V
                    
                    lincom (ev_lag0 + ev_lag1 + ev_lag2)/3
                    
					local avg_eff = r(estimate)
                    local avg_se_val = r(se)
					
                    summarize `outcome_var' if (`spec`s'_control') & year < tar_event_year, meanonly
                    local pre_mean = r(mean)

                    // Scale
                    local scaled_eff = `avg_eff' / `pre_mean'
                    local scaled_se  = `avg_se_val' / `pre_mean'

                    post handle ("`tg'") ("`role'") ("`role_label'") ("`outcome_type'") ///
						(`scaled_eff') (`scaled_se') (`s') (`pre_mean')
                }
                
                if _rc != 0 {
                    display as error "  Error in estimation, skipping..."
                }
            }
        }
    }
}

postclose handle

*-------------------------------------------------------------------------------
* STEP 3: Create forest plots
*-------------------------------------------------------------------------------

use `results', clear

// Generate CIs
gen ci_lo = avg_effect - 1.96 * avg_se
gen ci_hi = avg_effect + 1.96 * avg_se

// Add tier-group titles
gen tg_title = ""
foreach tg of local all_tg {
    replace tg_title = "``tg'_title'" if tg_code == "`tg'"
}

// Save full results

// Create forest plots for each tier-group x outcome combination
// (Assuming spec 1 for now - adjust as needed)
local spec_to_plot = 1

foreach tg of local all_tg {
    
    local plot_title "``tg'_title'"
    
    foreach outcome_type of local outcome_types {
        
        local outcome_title "`outcome_lab_`outcome_type''"
        
        preserve
        
        keep if tg_code == "`tg'" & outcome_type == "`outcome_type'" & spec_num == `spec_to_plot'
        
        qui count
        if r(N) == 0 {
            display as text "No data for `tg' - `outcome_type'"
            restore
            continue
        }
        
        // Sort by effect size for visual clarity
        gsort -avg_effect
        gen plot_order = _n
        local n_roles = _N
        
        // Create value labels for y-axis
        forvalues j = 1/`n_roles' {
            local lab_`j' = role_label[`j']
        }
        
        // Build ylabel option dynamically
        local ylab_opt ""
        forvalues j = 1/`n_roles' {
            local ylab_opt `"`ylab_opt' `j' "`lab_`j''""'
        }
        
        // Create the forest plot
        twoway (rcap ci_lo ci_hi plot_order, horizontal lcolor(navy%70) lwidth(medium)) ///
               (scatter plot_order avg_effect, mcolor(navy) msymbol(D) msize(medium)), ///
               xline(0, lcolor(cranberry) lpattern(dash) lwidth(thin)) ///
               ylabel(`ylab_opt', angle(0) labsize(vsmall) nogrid) ///
               ytitle("") ///
               xtitle("Average Treatment Effect (Periods 0-2)", size(small)) ///
               title("`plot_title'", size(medsmall) position(11)) ///
               subtitle("`outcome_title'", size(small) position(11)) ///
               legend(off) ///
               graphregion(color(white) margin(small)) ///
               plotregion(margin(l=0)) ///
               name(forest_`tg'_`outcome_type', replace)
        
        graph export "${overleaf}/notes/Non CEO Event Study/figures/wgt_forest_`tg'_`outcome_type'.pdf", ///
                     as(pdf) replace
        
        restore
    }
}

display _newline as result "Forest plots complete!"
