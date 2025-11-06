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

*----------------------------------------------------------
* Create indicators and get relative times 
*----------------------------------------------------------
make_outcome_vars 
make_target_sample

*----------------------------------------------------------
* Hospital-level splits (ie not men vs women, MD CEO)
*----------------------------------------------------------
frame copy default hospital_splits
frame change hospital_splits

local vars aha_bdtot_orig aha_mcddc aha_mcrdc

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

sum tar_reltime, meanonly
local rmin = r(min)
local rmax = r(max)

* Create Relative Time Indicators
forvalues h = 0/`rmax' {
    gen byte ev_lag`h' = (tar_reltime == `h')
	gen byte nfp_ev_lag`h' = (tar_reltime == `h' & forprofit == 0)
	gen byte fp_ev_lag`h' = (tar_reltime == `h' & forprofit == 1)
}
forvalues h = 1/`=abs(`rmin')' {
    gen byte ev_lead`h' = (tar_reltime == -`h')
	gen byte nfp_ev_lead`h' = (tar_reltime == -`h' & forprofit == 0)
    gen byte fp_ev_lead`h' = (tar_reltime == -`h' & forprofit == 1)

}
replace ev_lead1 = 0
replace fp_ev_lead1 = 0
replace nfp_ev_lead1 = 0

***** 2 Year Balanced Sample x CEO Turnover ******
// plot combined estimates
eventstudyinteract ceo_turnover1 nfp_ev_lead* nfp_ev_lag* fp_ev_lead* fp_ev_lag*  if (balanced_2_year_sample|never_m_and_a), vce(cluster entity_uniqueid) absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

coefplot (., keep( nfp_ev_lead3 nfp_ev_lead2 nfp_ev_lead1 nfp_ev_lag0 nfp_ev_lag1 nfp_ev_lag2 nfp_ev_lag3 ) ///
             b(b_iw) v(V_iw) label(NFP) mcolor(navy) ciopts(lcolor(navy) recast(rcap)) ///
             rename( nfp_ev_lead3=a2 nfp_ev_lead2=a3 nfp_ev_lead1=a4 nfp_ev_lag0=a5 ///
                    nfp_ev_lag1=a6 nfp_ev_lag2=a7 nfp_ev_lag3=a8 )) ///
         (., keep( fp_ev_lead3 fp_ev_lead2 fp_ev_lead1 fp_ev_lag0 fp_ev_lag1 fp_ev_lag2 fp_ev_lag3 ) ///
             b(b_iw) v(V_iw) label(FP) mcolor(maroon) ciopts(lcolor(maroon) recast(rcap)) msymbol(D) ///
             rename( fp_ev_lead3=a2 fp_ev_lead2=a3 fp_ev_lead1=a4 fp_ev_lag0=a5 ///
                    fp_ev_lag1=a6 fp_ev_lag2=a7 fp_ev_lag3=a8)), ///
    vertical ///
    yline(0, lcolor(gs8)) ///
    xline(4, lcolor(gs8) lpattern(dash)) ///
	order(a2 a3 a4 a5 a6 a7 a8) ///
    coeflabels(a2="-3" a3="-2" a4="-1" a5="0" a6="1" a7="2" a8="3") ///
    xtitle("Periods since the event") ytitle("Average effect") ///
	graphregion(color(white)) plotregion(color(white))
	
graph export "${overleaf}/notes/Event Study Setup/figures/joint_fp_nfp_turnover_2.pdf", as(pdf) replace

// estimate effects separately 

// for profit 
eventstudyinteract ceo_turnover1 ev_lag* ev_lead* if (balanced_2_year_sample|never_m_and_a) & forprofit == 1, vce(cluster entity_uniqueid) absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

matrix b = e(b_iw)
matrix V = e(V_iw)
ereturn post b V
lincom (ev_lag0 + ev_lag1 + ev_lag2)/3

// not for profit 
eventstudyinteract ceo_turnover1 ev_lag* ev_lead* if (balanced_2_year_sample|never_m_and_a) & forprofit == 0, vce(cluster entity_uniqueid) absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

matrix b = e(b_iw)
matrix V = e(V_iw)
ereturn post b V
lincom (ev_lag0 + ev_lag1 + ev_lag2)/3

// combined graph
// For-profit
eventstudyinteract ceo_turnover1 ev_lag* ev_lead* ///
    if (balanced_2_year_sample|never_m_and_a) & forprofit == 1, ///
    vce(cluster entity_uniqueid) absorb(entity_uniqueid year) ///
    cohort(tar_event_year) control_cohort(never_m_and_a)

// Store the matrices
matrix b_fp = e(b_iw)
matrix V_fp = e(V_iw)

// Not-for-profit
eventstudyinteract ceo_turnover1 ev_lag* ev_lead* ///
    if (balanced_2_year_sample|never_m_and_a) & forprofit == 0, ///
    vce(cluster entity_uniqueid) absorb(entity_uniqueid year) ///
    cohort(tar_event_year) control_cohort(never_m_and_a)

// Store the matrices
matrix b_nfp = e(b_iw)
matrix V_nfp = e(V_iw)

// Plot both together
coefplot (matrix(b_fp), v(V_fp) label(For-Profit) mcolor(maroon) ciopts(lcolor(maroon) recast(rcap)) msymbol(D)) ///
         (matrix(b_nfp), v(V_nfp) label(Not-For-Profit) mcolor(navy) ciopts(lcolor(navy) recast(rcap)) msymbol(O)), ///
    keep(ev_lag3 ev_lag2 ev_lag1 ev_lag0 ev_lead1 ev_lead2 ev_lead3) ///
    vertical ///
    yline(0, lcolor(gs8)) ///
    xline(4, lcolor(gs8) lpattern(dash)) ///	
	order( ev_lead3 ev_lead2 ev_lead1 ev_lag0 ev_lag1 ev_lag2 ev_lag3) ///
    coeflabels( ev_lag3="3" ev_lag2="2" ev_lag1="1" ev_lag0="0" ///
               ev_lead1="-1" ev_lead2="-2" ev_lead3="-3" ) ///
    xtitle("Periods since the event") ytitle("Average effect") ///
	graphregion(color(white)) plotregion(color(white))

graph export "${overleaf}/notes/Event Study Setup/figures/fp_nfp_turnover_2.pdf", as(pdf) name("Graph") replace

***** 2 Year Balanced Sample x CEO change in prev 2 years ******
// plot combined estimates
eventstudyinteract contact_changed_prev2yrs nfp_ev_lead* nfp_ev_lag* fp_ev_lead* fp_ev_lag*  if (balanced_2_year_sample|never_m_and_a), vce(cluster entity_uniqueid) absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

coefplot (., keep( nfp_ev_lead3 nfp_ev_lead2 nfp_ev_lead1 nfp_ev_lag0 nfp_ev_lag1 nfp_ev_lag2 nfp_ev_lag3 ) ///
             b(b_iw) v(V_iw) label(NFP) mcolor(navy) ciopts(lcolor(navy) recast(rcap)) ///
             rename( nfp_ev_lead3=a2 nfp_ev_lead2=a3 nfp_ev_lead1=a4 nfp_ev_lag0=a5 ///
                    nfp_ev_lag1=a6 nfp_ev_lag2=a7 nfp_ev_lag3=a8 )) ///
         (., keep( fp_ev_lead3 fp_ev_lead2 fp_ev_lead1 fp_ev_lag0 fp_ev_lag1 fp_ev_lag2 fp_ev_lag3 ) ///
             b(b_iw) v(V_iw) label(FP) mcolor(maroon) ciopts(lcolor(maroon) recast(rcap)) msymbol(D) ///
             rename( fp_ev_lead3=a2 fp_ev_lead2=a3 fp_ev_lead1=a4 fp_ev_lag0=a5 ///
                    fp_ev_lag1=a6 fp_ev_lag2=a7 fp_ev_lag3=a8)), ///
    vertical ///
    yline(0, lcolor(gs8)) ///
    xline(4, lcolor(gs8) lpattern(dash)) ///
	order(a2 a3 a4 a5 a6 a7 a8) ///
    coeflabels(a2="-3" a3="-2" a4="-1" a5="0" a6="1" a7="2" a8="3") ///
    xtitle("Periods since the event") ytitle("Average effect") ///
	graphregion(color(white)) plotregion(color(white))
	
graph export "${overleaf}/notes/Event Study Setup/figures/joint_fp_nfp_any_turnover_2.pdf", as(pdf) replace

// estimate effects separately 

// for profit 
eventstudyinteract contact_changed_prev2yrs ev_lag* ev_lead* if (balanced_2_year_sample|never_m_and_a) & forprofit == 1, vce(cluster entity_uniqueid) absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

matrix b = e(b_iw)
matrix V = e(V_iw)
ereturn post b V
lincom (ev_lag0 + ev_lag1 + ev_lag2)/3

// not for profit 
eventstudyinteract contact_changed_prev2yrs ev_lag* ev_lead* if (balanced_2_year_sample|never_m_and_a) & forprofit == 0, vce(cluster entity_uniqueid) absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

matrix b = e(b_iw)
matrix V = e(V_iw)
ereturn post b V
lincom (ev_lag0 + ev_lag1 + ev_lag2)/3

// combined graph
// For-profit
eventstudyinteract contact_changed_prev2yrs ev_lag* ev_lead* ///
    if (balanced_2_year_sample|never_m_and_a) & forprofit == 1, ///
    vce(cluster entity_uniqueid) absorb(entity_uniqueid year) ///
    cohort(tar_event_year) control_cohort(never_m_and_a)

// Store the matrices
matrix b_fp = e(b_iw)
matrix V_fp = e(V_iw)

// Not-for-profit
eventstudyinteract contact_changed_prev2yrs ev_lag* ev_lead* ///
    if (balanced_2_year_sample|never_m_and_a) & forprofit == 0, ///
    vce(cluster entity_uniqueid) absorb(entity_uniqueid year) ///
    cohort(tar_event_year) control_cohort(never_m_and_a)

// Store the matrices
matrix b_nfp = e(b_iw)
matrix V_nfp = e(V_iw)

// Plot both together
coefplot (matrix(b_fp), v(V_fp) label(For-Profit) mcolor(maroon) ciopts(lcolor(maroon) recast(rcap)) msymbol(D)) ///
         (matrix(b_nfp), v(V_nfp) label(Not-For-Profit) mcolor(navy) ciopts(lcolor(navy) recast(rcap)) msymbol(O)), ///
    keep(ev_lag3 ev_lag2 ev_lag1 ev_lag0 ev_lead1 ev_lead2 ev_lead3) ///
    vertical ///
    yline(0, lcolor(gs8)) ///
    xline(4, lcolor(gs8) lpattern(dash)) ///	
	order( ev_lead3 ev_lead2 ev_lead1 ev_lag0 ev_lag1 ev_lag2 ev_lag3) ///
    coeflabels( ev_lag3="3" ev_lag2="2" ev_lag1="1" ev_lag0="0" ///
               ev_lead1="-1" ev_lead2="-2" ev_lead3="-3" ) ///
    xtitle("Periods since the event") ytitle("Average effect") ///
	graphregion(color(white)) plotregion(color(white))

graph export "${overleaf}/notes/Event Study Setup/figures/fp_nfp_any_turnover_2.pdf", as(pdf) name("Graph") replace

frame change default

*----------------------------------------------------------
* Get individual characteristics
*----------------------------------------------------------
frame create individual_char
frame change individual_char

	use "${dbdata}/derived/individuals_final.dta", clear
	keep id aha_leader_flag all_leader_flag ceo_himss_title_exact ceo_himss_title_fuzzy
	tempfile ceo_flags
	save `ceo_flags'

	use "${dbdata}/derived/temp/indiv_file_contextual.dta", clear
	merge m:1 id using `ceo_flags'
	
	keep if _merge == 3 & aha_id != ""
	keep contact_uniqueid char_female char_md year aha_id aha_leader_flag all_leader_flag ceo_himss_title_exact ceo_himss_title_fuzzy char_ceo title_standardized
	keep if all_leader_flag

	// confirm that ceo flag is unique
	bys aha_id year: egen n_unique_contacts = nvals(contact_uniqueid)
	gen multi_contact = n_unique_contacts > 1
	count if multi_contact == 1
	assert r(N) == 0
	drop multi_contact
	
	// get lags
	sort aha_id year
	by aha_id: gen char_female_lag_1 = char_female[_n-1]
	by aha_id: gen char_female_lag_2 = char_female[_n-2]
	by aha_id: gen char_md_lag_1 = char_md[_n-1]
	by aha_id: gen char_md_lag_2 = char_md[_n-2]
	by aha_id: gen year_lag_1 = year[_n-1]
	by aha_id: gen year_lag_2 = year[_n-2]
	
	keep aha_id year* char_female* char_md* 
	bys aha_id year: keep if _n == 1
	destring aha_id, replace

	tempfile individual_characteristics
	save `individual_characteristics'

frame change default
frame drop individual_char

merge m:1 aha_id year using `individual_characteristics'
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
local label3_0 "MD CEO Before Acquisition"
local label3_1 "Non-MD CEO Before Acquisition"

local binvar4 "char_md_lag_2"
local binname4 "md2"
local label4_0 "MD CEO 2 Years Before Acquisition"
local label4_1 "Non-MD CEO 2 Years Before Acquisition"

// Number of comparisons to loop through
local n_comparisons = 4

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
