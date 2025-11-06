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
