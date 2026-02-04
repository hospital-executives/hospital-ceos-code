
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
* Make event time indicators
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

*----------------------------------------------------------
* Run CCO event study
*----------------------------------------------------------

eventstudyinteract cco_switched_to_active  ev_lead* ev_lag* ///
            if (balanced_2_year_sample == 1 | never_tar == 1), ///
            vce(cluster entity_uniqueid) ///
            absorb(entity_uniqueid year) ///
            cohort(tar_event_year) ///
            control_cohort(never_tar)
        
matrix b = e(b_iw)
matrix V = e(V_iw)
ereturn post b V
        
display _newline "Average Treatment Effect (periods 0-2):"
lincom (ev_lag0 + ev_lag1 + ev_lag2)/3
        
// Store the result rounded to 3 decimal places
local avg_effect = round(r(estimate), 0.001)
local avg_se = round(r(se), 0.001)
        
// Second run: Event plot with average effect in title
eventstudyinteract cco_switched_to_active ev_lead* ev_lag* ///
            if (balanced_2_year_sample == 1 | never_tar == 1), ///
            vce(cluster entity_uniqueid) ///
            absorb(entity_uniqueid year) ///
            cohort(tar_event_year) ///
            control_cohort(never_tar)
        
event_plot e(b_iw)#e(V_iw), ///
            default_look ///
            graph_opt(xtitle("Periods since the event") ///
                      ytitle("Average effect") ///
                      xlabel(-3(1)3) ///
                      title("Effect on `outcome'" ///
                            "Avg: `avg_effect' (SE: `avg_se')", size(medium))) ///
            stub_lag(ev_lag#) ///
            stub_lead(ev_lead#) ///
            trimlag(3) ///
            trimlead(3) ///
            plottype(scatter) ///
            ciplottype(rcap)
