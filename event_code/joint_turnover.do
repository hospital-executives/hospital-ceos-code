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
merge 1:1 entity_uniqueid year using "${dbdata}/derived/hospitals_with_turnover.dta", keep(match) nogen

make_target_sample

* Define joint turnover outcomes (1 if both roles turn over)
gen turnover_ceo_x_cfo = (turnover_ceo == 1 & turnover_cfo == 1) if !missing(turnover_ceo) & !missing(turnover_cfo)
gen turnover_ceo_x_coo = (turnover_ceo == 1 & turnover_coo == 1) if !missing(turnover_ceo) & !missing(turnover_coo)
gen turnover_ceo_x_cmo = (turnover_ceo == 1 & turnover_cmo == 1) if !missing(turnover_ceo) & !missing(turnover_cmo)
gen turnover_cfo_x_coo = (turnover_cfo == 1 & turnover_coo == 1) if !missing(turnover_cfo) & !missing(turnover_coo)
gen turnover_coo_x_cco = (turnover_coo == 1 & turnover_cco == 1) if !missing(turnover_coo) & !missing(turnover_cco)
gen turnover_cmo_x_cno = (turnover_cmo == 1 & turnover_cno == 1) if !missing(turnover_cmo) & !missing(turnover_cno)

* Define outcome variables
local all_outcomes "turnover_ceo_x_cfo turnover_ceo_x_coo turnover_ceo_x_cmo turnover_cfo_x_coo turnover_coo_x_cco turnover_cmo_x_cno"

*----------------------------------------------------------
* Define samples + run event studies
*----------------------------------------------------------

* Define main specification
local spec1_treated "balanced_2_year_sample == 1"
local spec1_control "never_tar == 1"
local spec1_cohort "never_tar"
local spec1_name "2yr Balanced, Never Treated"
local spec1_file "2yrbalanced_never_tar"

local nspecs = 1

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

* Collect average effects from each regression
tempfile joint_results
postfile joint_handle str40 outcome double avg_effect double avg_se double pre_mean using `joint_results', replace

**** Loop through specifications and outcomes ****
forvalues s = 1/`nspecs' {
    
    foreach outcome of local all_outcomes {
        
        display _newline(2) "{hline 60}"
        display "Specification `s': `spec`s'_name' - `outcome'"
        display "{hline 60}"
        
        // Run regression and calculate average effect
        eventstudyinteract `outcome' ev_lead* ev_lag* ///
            if (`spec`s'_treated' | `spec`s'_control'), ///
            vce(cluster entity_uniqueid) ///
            absorb(entity_uniqueid year) ///
            cohort(tar_event_year) ///
            control_cohort(`spec`s'_cohort')
        
        matrix b = e(b_iw)
        matrix V = e(V_iw)
        ereturn post b V
        
        display _newline "Average Treatment Effect (periods 0-2):"
        lincom (ev_lag0 + ev_lag1 + ev_lag2)/3
        
        local avg_effect = r(estimate)
        local avg_se = r(se)
        local avg_effect_fmt : display %9.3f `avg_effect'
        local avg_se_fmt : display %9.3f `avg_se'
        
        quietly summarize `outcome' ///
            if (`spec`s'_treated' | `spec`s'_control') & `spec`s'_control' & tar_reltime < 0, meanonly
        local pre_mean = r(mean)

        // Fallback for sparse outcomes: use full pre-period mean if control pre-period is zero/missing
        quietly summarize `outcome' ///
            if (`spec`s'_treated' | `spec`s'_control') & tar_reltime < 0, meanonly
        local pre_mean_all = r(mean)
        if missing(`pre_mean') | `pre_mean' == 0 {
            local pre_mean = `pre_mean_all'
        }
        // Final guard so weighted plots can still be drawn
        if missing(`pre_mean') | `pre_mean' == 0 {
            local pre_mean = 1e-6
        }
        
        post joint_handle ("`outcome'") (`avg_effect') (`avg_se') (`pre_mean')

        // Event-study plot for this outcome
		eventstudyinteract `outcome' ev_lead* ev_lag* ///
            if (`spec`s'_treated' | `spec`s'_control'), ///
            vce(cluster entity_uniqueid) ///
            absorb(entity_uniqueid year) ///
            cohort(tar_event_year) ///
            control_cohort(`spec`s'_cohort')
        
        matrix b = e(b_iw)
        matrix V = e(V_iw)
		
        // Map outcome variable name to readable title
        if "`outcome'" == "turnover_ceo_x_cfo" local outcome_title "Effect on Joint CEO and CFO Turnover"
        else if "`outcome'" == "turnover_ceo_x_coo" local outcome_title "Effect on Joint CEO and COO Turnover"
        else if "`outcome'" == "turnover_ceo_x_cmo" local outcome_title "Effect on Joint CEO and CMO Turnover"
        else if "`outcome'" == "turnover_cfo_x_coo" local outcome_title "Effect on Joint CFO and COO Turnover"
        else if "`outcome'" == "turnover_coo_x_cco" local outcome_title "Effect on Joint COO and CCO Turnover"
        else if "`outcome'" == "turnover_cmo_x_cno" local outcome_title "Effect on Joint CMO and CNO Turnover"
        else local outcome_title "Effect on `outcome'"

        event_plot e(b_iw)#e(V_iw), ///
            default_look ///
            graph_opt(xtitle("Periods since the event") ///
                      ytitle("Average effect") ///
                      xlabel(-3(1)3) ///
                      title("`outcome_title'" ///
                            "`spec`s'_name' | Avg: `avg_effect_fmt' (SE: `avg_se_fmt')", size(medium))) ///
            stub_lag(ev_lag#) ///
            stub_lead(ev_lead#) ///
            trimlag(3) ///
            trimlead(3) ///
            plottype(scatter) ///
            ciplottype(rcap)

        graph export "${overleaf}/notes/Non CEO Event Study/figures/`outcome'_event.pdf", as(pdf) replace
    }
}

postclose joint_handle

* Single forest plot with all joint outcomes
preserve
use `joint_results', clear

gen ci_lo = avg_effect - 1.96 * avg_se
gen ci_hi = avg_effect + 1.96 * avg_se
gen avg_effect_wgt = .
replace avg_effect_wgt = avg_effect / pre_mean if pre_mean != 0 & !missing(pre_mean)
gen avg_se_wgt = .
replace avg_se_wgt = avg_se / abs(pre_mean) if pre_mean != 0 & !missing(pre_mean)
gen ci_lo_wgt = avg_effect_wgt - 1.96 * avg_se_wgt
gen ci_hi_wgt = avg_effect_wgt + 1.96 * avg_se_wgt

gen outcome_label = outcome
replace outcome_label = subinstr(outcome_label, "turnover_", "", .)
replace outcome_label = subinstr(outcome_label, "_x_", " x ", .)
replace outcome_label = upper(outcome_label)

gsort -avg_effect
gen plot_order = _n
local n_outcomes = _N

local ylab_opt ""
forvalues j = 1/`n_outcomes' {
    local lab = outcome_label[`j']
    local ylab_opt `"`ylab_opt' `j' "`lab'""'
}

twoway ///
    (rcap ci_lo ci_hi plot_order, horizontal lcolor(navy%70) lwidth(medium)) ///
    (scatter plot_order avg_effect, mcolor(navy) msymbol(D) msize(medium)) ///
    , ///
    xline(0, lcolor(cranberry) lpattern(dash) lwidth(thin)) ///
    ylabel(`ylab_opt', angle(0) labsize(vsmall) nogrid) ///
    ytitle("") ///
    xtitle("Average Treatment Effect (Periods 0-2)", size(small)) ///
    title("Joint Turnover Effects", size(medsmall) position(11)) ///
    subtitle("`spec1_name'", size(small) position(11)) ///
    legend(off) ///
    graphregion(color(white) margin(small)) ///
    plotregion(margin(l=0)) ///
    name(joint_turnover_forest, replace)

graph export "${overleaf}/notes/Non CEO Event Study/figures/joint_turnover_forest.pdf", as(pdf) replace

twoway ///
    (rcap ci_lo_wgt ci_hi_wgt plot_order, horizontal lcolor(navy%70) lwidth(medium)) ///
    (scatter plot_order avg_effect_wgt, mcolor(navy) msymbol(D) msize(medium)) ///
    , ///
    xline(0, lcolor(cranberry) lpattern(dash) lwidth(thin)) ///
    ylabel(`ylab_opt', angle(0) labsize(vsmall) nogrid) ///
    ytitle("") ///
    xtitle("Avg. Treatment Effect / Pre-period Mean (Periods 0-2)", size(small)) ///
    title("Joint Turnover Effects (Weighted)", size(medsmall) position(11)) ///
    subtitle("`spec1_name'", size(small) position(11)) ///
    legend(off) ///
    graphregion(color(white) margin(small)) ///
    plotregion(margin(l=0)) ///
    name(joint_turnover_forest_wgt, replace)

graph export "${overleaf}/notes/Non CEO Event Study/figures/joint_turnover_forest_wgt.pdf", as(pdf) replace
restore
