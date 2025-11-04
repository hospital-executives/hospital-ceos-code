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

// try combining estimates
eventstudyinteract contact_changed_prev2yrs nfp_ev_lead* nfp_ev_lag* fp_ev_lead* fp_ev_lag*  if (restricted_treated_sample|never_m_and_a), vce(cluster entity_uniqueid) absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

matrix b = e(b_iw)
matrix V = e(V_iw)
ereturn post b V
lincom nfp_ev_lag0 - fp_ev_lag0

// estimate effects separately 

// for profit 
eventstudyinteract contact_changed_prev2yrs ev_lag* ev_lead* if (restricted_treated_sample|never_m_and_a) & forprofit == 1, vce(cluster entity_uniqueid) absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

matrix b = e(b_iw)
matrix V = e(V_iw)
ereturn post b V
lincom (ev_lag0 + ev_lag1 + ev_lag2)/3

eventstudyinteract contact_changed_prev2yrs ev_lag* ev_lead* if (restricted_treated_sample|never_m_and_a) & forprofit == 1, vce(cluster entity_uniqueid) absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

event_plot e(b_iw)#e(V_iw), ///
        default_look ///
        graph_opt(xtitle("Periods since the event") ///
                  ytitle("Average effect") ///
                  xlabel(-4(1)4)) ///
        stub_lag(ev_lag#) ///
        stub_lead(ev_lead#) ///
        trimlag(4) ///
        trimlead(4) ///
        plottype(scatter) ///
        ciplottype(rcap)
		
    graph export "${overleaf}/notes/Event Study Setup/figures/fp_event_any_two_years.pdf", as(pdf) name("Graph") replace

// not for profit 
eventstudyinteract contact_changed_prev2yrs ev_lag* ev_lead* if (restricted_treated_sample|never_m_and_a) & forprofit == 0, vce(cluster entity_uniqueid) absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

matrix b = e(b_iw)
matrix V = e(V_iw)
ereturn post b V
lincom (ev_lag0 + ev_lag1 + ev_lag2)/3

eventstudyinteract contact_changed_prev2yrs ev_lag* ev_lead* if (restricted_treated_sample|never_m_and_a) & forprofit == 0, vce(cluster entity_uniqueid) absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

event_plot e(b_iw)#e(V_iw), ///
        default_look ///
        graph_opt(xtitle("Periods since the event") ///
                  ytitle("Average effect") ///
                  xlabel(-4(1)4)) ///
        stub_lag(ev_lag#) ///
        stub_lead(ev_lead#) ///
        trimlag(4) ///
        trimlead(4) ///
        plottype(scatter) ///
        ciplottype(rcap)
		
graph export "${overleaf}/notes/Event Study Setup/figures/nfp_event_any_two_years.pdf", as(pdf) name("Graph") replace

frame change default
