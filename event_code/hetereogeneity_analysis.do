/* CUMULATIVE_TURNOVER *************************************************************

Program name: 	cumulative_turnover.do
Programmer: 	Katherine Papen

Goal: 			Generate event study plots where outcome variable is whether or
				not the CEO has changed in the previous 2 years or 3 years. 

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

merge 1:1 entity_uniqueid year using `himss_type_xwalk', nogen

* restrict to hospital sample
restrict_hosp_sample
make_outcome_vars 
make_target_sample

*----------------------------------------------------------
* Hospital-level splits (ie not men vs women, MD CEO)
*----------------------------------------------------------
frame copy default hospital_splits
frame change hospital_splits

local vars aha_bdtot_orig aha_mcddc aha_mcrdc aha_fte

foreach v of local vars {
    // compute overall median
    quietly summarize `v', detail
    local med = r(p50)

    // make short name suffix (e.g. beds, medicaid, medicare)
    local suffix : subinstr local v "aha_" "", all

    // create above-median indicator
    gen high_`suffix' = (`v' > `med')

    // flag entities where any observation is above the median
    bys aha_id: egen above_median_`suffix' = max(high_`suffix')
}

// Prespecify outcomes
local outcome1 "ceo_turnover1"
local outname1 "turn"

local outcome2 "contact_changed_prev2yrs"
local outname2 "any_2"

local outcome3 "contact_changed_prev3yrs"
local outname3 "any_3"

// Prespecify treated samples
local sample1 "balanced_2_year_sample"
local sampname1 "bal2y"

local sample2 "balanced_3_year_sample"
local sampname2 "bal3y"

local sample3 "full_treated_sample"
local sampname3 "full"

// Number of outcomes and samples
local n_outcomes = 3
local n_samples = 3

// Prespecify your binary variables and labels
local binvar1 "forprofit"
local binname1 "fp"
local label1_0 "NFP"
local label1_1 "FP"

local binvar2 "above_median_bdtot_orig"
local binname2 "bdtot"
local label2_0 "Below Median Bed Count"
local label2_1 "Above Median Bed Count"

local binvar3 "above_median_mcddc"
local binname3 "mcddc"
local label3_0 "Below Median Medicaid"
local label3_1 "Above Median Medicaid"

local binvar4 "above_median_mcrdc"
local binname4 "mcr"
local label4_0 "Below Median Medicare"
local label4_1 "Above Median Medicare"

local binvar5 "above_median_fte"
local binname5 "fte"
local label5_0 "Below Median FTE"
local label5_1 "Above Median FTE"

local binvar6 "aha_teaching"
local binname6 "teaching"
local label6_0 "Not Teaching"
local label6_1 "Teaching"

local binvar7 "aha_cah"
local binname7 "cah"
local label7_0 "Not CAH"
local label7_1 "CAH"

// Number of comparisons to loop through
local n_comparisons = 7

// Get relative time range
sum tar_reltime, meanonly
local rmin = r(min)
local rmax = r(max)

// Create Relative Time Indicators (only once)
forvalues h = 0/`rmax' {
    cap gen byte ev_lag`h' = (tar_reltime == `h')
}
forvalues h = 1/`=abs(`rmin')' {
    cap gen byte ev_lead`h' = (tar_reltime == -`h')
}
replace ev_lead1 = 0

// Loop through outcomes
forvalues o = 1/`n_outcomes' {
    local outcome = "`outcome`o''"
    local outname = "`outname`o''"
    
    // Loop through samples
    forvalues s = 1/`n_samples' {
        local sample = "`sample`s''"
        local sampname = "`sampname`s''"
        
        // Loop through each binary variable comparison
        forvalues i = 1/`n_comparisons' {
            local binvar = "`binvar`i''"
            local binname = "`binname`i''"
            
            // Loop through both estimation methods
            foreach estimation_method in "separate" "together" {
                
                if "`estimation_method'" == "separate" {
                    // SEPARATE ESTIMATION: Run regressions for both conditions separately
                    forvalues c = 0/1 {
                        eventstudyinteract `outcome' ev_lag* ev_lead* ///
                            if (`sample'|never_m_and_a) & `binvar' == `c', ///
                            vce(cluster entity_uniqueid) absorb(entity_uniqueid year) ///
                            cohort(tar_event_year) control_cohort(never_m_and_a)
                        
                        // Store the matrices
                        matrix b_`c' = e(b_iw)
                        matrix V_`c' = e(V_iw)
                    }
                    
                    // Plot both together
                    coefplot (matrix(b_1), v(V_1) label(`label`i'_1') mcolor(maroon) ciopts(lcolor(maroon) recast(rcap)) msymbol(D)) ///
                             (matrix(b_0), v(V_0) label(`label`i'_0') mcolor(navy) ciopts(lcolor(navy) recast(rcap)) msymbol(O)), ///
                        keep(ev_lag3 ev_lag2 ev_lag1 ev_lag0 ev_lead1 ev_lead2 ev_lead3) ///
                        vertical ///
                        yline(0, lcolor(gs8)) ///
                        xline(4, lcolor(gs8) lpattern(dash)) ///	
                        order(ev_lead3 ev_lead2 ev_lead1 ev_lag0 ev_lag1 ev_lag2 ev_lag3) ///
                        coeflabels(ev_lag3="3" ev_lag2="2" ev_lag1="1" ev_lag0="0" ///
                                   ev_lead1="-1" ev_lead2="-2" ev_lead3="-3") ///
                        xtitle("Periods since the event") ytitle("Average effect") ///
                        graphregion(color(white)) plotregion(color(white))
                        
                        graph export "${overleaf}/notes/Event Study Setup/figures/`outname'_`sampname'_`binname'_sep.pdf", as(pdf) name("Graph") replace
                }
                else {
                    // TOGETHER ESTIMATION: Create interaction variables and run single regression
                    forvalues h = 0/`rmax' {
                        forvalues c = 0/1 {
                            cap gen byte ev`c'_lag`h' = ev_lag`h' * (`binvar' == `c')
                        }
                    }
                    forvalues h = 1/`=abs(`rmin')' {
                        forvalues c = 0/1 {
                            cap gen byte ev`c'_lead`h' = ev_lead`h' * (`binvar' == `c')
                        }
                    }
                    // Set baseline
                    forvalues c = 0/1 {
                        replace ev`c'_lead1 = 0
                    }
                    
                    // Single regression with both groups
                    eventstudyinteract `outcome' ev0_lead* ev0_lag* ev1_lead* ev1_lag* ///
                        if (`sample'|never_m_and_a), ///
                        vce(cluster entity_uniqueid) absorb(entity_uniqueid year) ///
                        cohort(tar_event_year) control_cohort(never_m_and_a)
                    
                    // Plot directly from regression results
                    coefplot (., keep(ev0_lead3 ev0_lead2 ev0_lead1 ev0_lag0 ev0_lag1 ev0_lag2 ev0_lag3) ///
                                 b(b_iw) v(V_iw) label(`label`i'_0') mcolor(navy) ciopts(lcolor(navy) recast(rcap)) ///
                                 rename(ev0_lead3=a2 ev0_lead2=a3 ev0_lead1=a4 ev0_lag0=a5 ///
                                        ev0_lag1=a6 ev0_lag2=a7 ev0_lag3=a8)) ///
                             (., keep(ev1_lead3 ev1_lead2 ev1_lead1 ev1_lag0 ev1_lag1 ev1_lag2 ev1_lag3) ///
                                 b(b_iw) v(V_iw) label(`label`i'_1') mcolor(maroon) ciopts(lcolor(maroon) recast(rcap)) msymbol(D) ///
                                 rename(ev1_lead3=a2 ev1_lead2=a3 ev1_lead1=a4 ev1_lag0=a5 ///
                                        ev1_lag1=a6 ev1_lag2=a7 ev1_lag3=a8)), ///
                        vertical ///
                        yline(0, lcolor(gs8)) ///
                        xline(4, lcolor(gs8) lpattern(dash)) ///
                        order(a2 a3 a4 a5 a6 a7 a8) ///
                        coeflabels(a2="-3" a3="-2" a4="-1" a5="0" a6="1" a7="2" a8="3") ///
                        xtitle("Periods since the event") ytitle("Average effect") ///
                        graphregion(color(white)) plotregion(color(white))
                    
                    graph export "${overleaf}/notes/Event Study Setup/figures/`outname'_`sampname'_`binname'_tog.pdf", as(pdf) name("Graph") replace
                    
                    // Drop interaction variables for next iteration
                    drop ev0_* ev1_*
                }
            }
        }
    }
}

frame change default

*----------------------------------------------------------
* Get individual characteristics
*----------------------------------------------------------

frame create prev_tar
frame change prev_tar

	use "${dbdata}/derived/temp/merged_ma_sysid_xwalk.dta", clear
	keep tar year aha_id
	bys aha_id year: keep if _n == 1
	tempfile tar_data
	save `tar_data'

	use "${dbdata}/derived/individuals_final.dta", clear
	keep id aha_leader_flag all_leader_flag ceo_himss_title_exact ceo_himss_title_fuzzy
	tempfile ceo_flags
	save `ceo_flags'

	use "${dbdata}/derived/temp/indiv_file_contextual.dta", clear
	merge 1:1 id using `ceo_flags', gen(ceo_merge)
	merge m:1 aha_id year using `tar_data', gen(tar_merge)

	
	keep if ceo_merge == 3 & tar_merge == 3 & aha_id != ""
	keep contact_uniqueid aha_id year all_leader_flag ceo_himss_title_exact ceo_himss_title_fuzzy char_ceo title_standardized tar
	
	bysort contact_uniqueid aha_id year: egen n_tar = total(tar == 1)
	bysort contact_uniqueid aha_id year: egen n_tar_ceo = total(tar == 1 & all_leader_flag == 1)
	
	keep if all_leader_flag
	bysort contact_uniqueid: egen serial_tar = max(n_tar_ceo > 1)
	sort aha_id year
	by aha_id: gen lag_serial_tar = serial_tar[_n-1]

	keep aha_id year lag_serial_tar serial_tar 
	bys aha_id year: keep if _n == 1
	destring aha_id, replace

	tempfile serial_temp
	save `serial_temp'
	
frame change default
frame drop prev_tar
	
merge m:1 aha_id year using `serial_temp'
keep if _merge == 3
drop _merge

*----------------------------------------------------------
* Individual-level splits (men vs women, MD CEO)
*----------------------------------------------------------
frame copy default indiv_splits
frame change indiv_splits


// Prespecify outcomes
local outcome1 "ceo_turnover1"
local outname1 "turn"

local outcome2 "contact_changed_prev2yrs"
local outname2 "any_2"

local outcome3 "contact_changed_prev3yrs"
local outname3 "any_3"

// Prespecify treated samples
local sample1 "balanced_2_year_sample"
local sampname1 "bal2y"

local sample2 "balanced_3_year_sample"
local sampname2 "bal3y"

local sample3 "full_treated_sample"
local sampname3 "full"

// Number of outcomes and samples
local n_outcomes = 3
local n_samples = 3

// Prespecify your binary variables and labels
local binvar1 "char_female_lag_1"
local binname1 "fem1"
local label1_0 "Male CEO Before Acquisition"
local label1_1 "Female CEO Before Acquisition"

local binvar2 "char_female_lag_2"
local binname2 "fem2"
local label2_0 "Male CEO 2 Years Before Acquisition"
local label2_1 "Female CEO 2 Years Before Acquisition"

local binvar3 "char_md_lag_1"
local binname3 "md1"
local label3_0 "Non-MD CEO Before Acquisition"
local label3_1 "MD CEO Before Acquisition"

local binvar4 "char_md_lag_2"
local binname4 "md2"
local label4_0 "Non-MD CEO 2 Years Before Acquisition"
local label4_1 "MD CEO 2 Years Before Acquisition"

local binvar5 "lag_serial_tar"
local binname5 "serial"
local label5_0 "Serial CEO 1 Year Before Acquisition"
local label5_1 "Serial CEO 1 Year Before Acquisition"

// Number of comparisons to loop through
local n_comparisons = 5

// Get relative time range
sum tar_reltime, meanonly
local rmin = r(min)
local rmax = r(max)

// Create Relative Time Indicators (only once)
forvalues h = 0/`rmax' {
    cap gen byte ev_lag`h' = (tar_reltime == `h')
}
forvalues h = 1/`=abs(`rmin')' {
    cap gen byte ev_lead`h' = (tar_reltime == -`h')
}
replace ev_lead1 = 0

// Loop through outcomes
forvalues o = 1/`n_outcomes' {
    local outcome = "`outcome`o''"
    local outname = "`outname`o''"
    
    // Loop through samples
    forvalues s = 1/`n_samples' {
        local sample = "`sample`s''"
        local sampname = "`sampname`s''"
        
        // Loop through each binary variable comparison
        forvalues i = 1/`n_comparisons' {
            local binvar = "`binvar`i''"
            local binname = "`binname`i''"
            
            // Loop through both estimation methods
            foreach estimation_method in "separate" "together" {
                
                if "`estimation_method'" == "separate" {
                    // SEPARATE ESTIMATION: Run regressions for both conditions separately
                    forvalues c = 0/1 {
                        eventstudyinteract `outcome' ev_lag* ev_lead* ///
                            if (`sample'|never_m_and_a) & `binvar' == `c', ///
                            vce(cluster entity_uniqueid) absorb(entity_uniqueid year) ///
                            cohort(tar_event_year) control_cohort(never_m_and_a)
                        
                        // Store the matrices
                        matrix b_`c' = e(b_iw)
                        matrix V_`c' = e(V_iw)
                    }
                    
                    // Plot both together
                    coefplot (matrix(b_1), v(V_1) label(`label`i'_1') mcolor(maroon) ciopts(lcolor(maroon) recast(rcap)) msymbol(D)) ///
                             (matrix(b_0), v(V_0) label(`label`i'_0') mcolor(navy) ciopts(lcolor(navy) recast(rcap)) msymbol(O)), ///
                        keep(ev_lag3 ev_lag2 ev_lag1 ev_lag0 ev_lead1 ev_lead2 ev_lead3) ///
                        vertical ///
                        yline(0, lcolor(gs8)) ///
                        xline(4, lcolor(gs8) lpattern(dash)) ///	
                        order(ev_lead3 ev_lead2 ev_lead1 ev_lag0 ev_lag1 ev_lag2 ev_lag3) ///
                        coeflabels(ev_lag3="3" ev_lag2="2" ev_lag1="1" ev_lag0="0" ///
                                   ev_lead1="-1" ev_lead2="-2" ev_lead3="-3") ///
                        xtitle("Periods since the event") ytitle("Average effect") ///
                        graphregion(color(white)) plotregion(color(white))
                        
                        graph export "${overleaf}/notes/Event Study Setup/figures/`outname'_`sampname'_`binname'_sep.pdf", as(pdf) name("Graph") replace
                }
                else {
                    // TOGETHER ESTIMATION: Create interaction variables and run single regression
                    forvalues h = 0/`rmax' {
                        forvalues c = 0/1 {
                            cap gen byte ev`c'_lag`h' = ev_lag`h' * (`binvar' == `c')
                        }
                    }
                    forvalues h = 1/`=abs(`rmin')' {
                        forvalues c = 0/1 {
                            cap gen byte ev`c'_lead`h' = ev_lead`h' * (`binvar' == `c')
                        }
                    }
                    // Set baseline
                    forvalues c = 0/1 {
                        replace ev`c'_lead1 = 0
                    }
                    
                    // Single regression with both groups
                    eventstudyinteract `outcome' ev0_lead* ev0_lag* ev1_lead* ev1_lag* ///
                        if (`sample'|never_m_and_a), ///
                        vce(cluster entity_uniqueid) absorb(entity_uniqueid year) ///
                        cohort(tar_event_year) control_cohort(never_m_and_a)
                    
                    // Plot directly from regression results
                    coefplot (., keep(ev0_lead3 ev0_lead2 ev0_lead1 ev0_lag0 ev0_lag1 ev0_lag2 ev0_lag3) ///
                                 b(b_iw) v(V_iw) label(`label`i'_0') mcolor(navy) ciopts(lcolor(navy) recast(rcap)) ///
                                 rename(ev0_lead3=a2 ev0_lead2=a3 ev0_lead1=a4 ev0_lag0=a5 ///
                                        ev0_lag1=a6 ev0_lag2=a7 ev0_lag3=a8)) ///
                             (., keep(ev1_lead3 ev1_lead2 ev1_lead1 ev1_lag0 ev1_lag1 ev1_lag2 ev1_lag3) ///
                                 b(b_iw) v(V_iw) label(`label`i'_1') mcolor(maroon) ciopts(lcolor(maroon) recast(rcap)) msymbol(D) ///
                                 rename(ev1_lead3=a2 ev1_lead2=a3 ev1_lead1=a4 ev1_lag0=a5 ///
                                        ev1_lag1=a6 ev1_lag2=a7 ev1_lag3=a8)), ///
                        vertical ///
                        yline(0, lcolor(gs8)) ///
                        xline(4, lcolor(gs8) lpattern(dash)) ///
                        order(a2 a3 a4 a5 a6 a7 a8) ///
                        coeflabels(a2="-3" a3="-2" a4="-1" a5="0" a6="1" a7="2" a8="3") ///
                        xtitle("Periods since the event") ytitle("Average effect") ///
                        graphregion(color(white)) plotregion(color(white))
                    
                    graph export "${overleaf}/notes/Event Study Setup/figures/`outname'_`sampname'_`binname'_tog.pdf", as(pdf) name("Graph") replace
                    
                    // Drop interaction variables for next iteration
                    drop ev0_* ev1_*
                }
            }
        }
    }
}

frame change default
frame drop indiv_splits

