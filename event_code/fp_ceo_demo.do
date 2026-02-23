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

* merge in profit data
	* pull in parent profit info
	preserve
		use "${dbdata}/derived/temp/systems_nonharmonized_withprofit.dta", clear 
		keep entity_uniqueid year forprofit
		rename entity_uniqueid entity_uniqueid_parent 
		rename forprofit forprofit_parent
		tempfile sysprofit
		save `sysprofit'
	restore
	merge m:1 entity_uniqueid_parent year using `sysprofit', gen(_merge_profit) keep(1 3) // only unmerged observations are systems plus the 23 ambulatory 
	
* pull in contextual data for parents - specifically want profit data
	merge m:1 entity_uniqueid year using "${dbdata}/derived/temp/systems_nonharmonized_withprofit.dta", keep(1 3) gen(_merge_newprofit) keepusing(forprofit_imputed)
	replace forprofit = forprofit_imputed if _merge_newprofit == 3 & !missing(forprofit_imputed) // this only changes things for parents
	drop _merge_newprofit
	
* NEW: replacing facility forprofit info with parent info. 
	replace forprofit = forprofit_parent if missing(forprofit) & !missing(forprofit_parent)

	
make_target_sample

* Define outcome variables
local turnover_outcomes "turnover_ceo turnover_cfo turnover_coo turnover_cmo turnover_cno turnover_cco turnover_cio"
local vacancy_outcomes "vacant_ceo vacant_cfo vacant_coo vacant_cmo vacant_cno vacant_cco vacant_cio"
local all_outcomes "`turnover_outcomes' `vacancy_outcomes'"

*==============================================================
* Event Study Plot: Average Treatment Effects by C-Suite Title
*   × Binary Hospital Characteristics
*==============================================================

// ── 0. Prelims ──────────────────────────────────────────────
// Get relative time range
sum tar_reltime, meanonly
local rmin = r(min)
local rmax = r(max)

// Create Relative Time Indicators
forvalues h = 0/`rmax' {
    cap gen byte ev_lag`h' = (tar_reltime == `h')
}
forvalues h = 1/`=abs(`rmin')' {
    cap gen byte ev_lead`h' = (tar_reltime == -`h')
}
replace ev_lead1 = 0



	local splitvar  "forprofit"
	local splitname "fp"
    local lab0      "NFP"
    local lab1      "FP"

    display _newline(3) as result "=============================================="
    display as result " Split: `splitvar'  (`lab0' vs `lab1')"
    display as result "=============================================="

    // ── Create interacted event-time dummies ────────────────
    // (drop any prior versions first)
    capture drop ev0_lag* ev1_lag* ev0_lead* ev1_lead*
	
	sum tar_reltime, meanonly
	local rmin = r(min)
	local rmax = r(max)


    forvalues h = 0/`rmax' {
        forvalues c = 0/1 {
            cap gen byte ev`c'_lag`h' = ev_lag`h' * (`splitvar' == `c')
        }
    }
    forvalues h = 1/`=abs(`rmin')' {
        forvalues c = 0/1 {
            cap gen byte ev`c'_lead`h' = ev_lead`h' * (`splitvar' == `c')
        }
    }
    // Set baseline (omit lead1 for both groups)
    forvalues c = 0/1 {
        replace ev`c'_lead1 = 0
    }

    // ── Build variable lists for regression ─────────────────
    // Leads (excluding lead1) for group 0 and 1
    local evvars0 ""
    local evvars1 ""
    forvalues h = `=abs(`rmin')' (-1) 2 {
        local evvars0 "`evvars0' ev0_lead`h'"
        local evvars1 "`evvars1' ev1_lead`h'"
    }
    // Lags for group 0 and 1
    forvalues h = 0/`rmax' {
        local evvars0 "`evvars0' ev0_lag`h'"
        local evvars1 "`evvars1' ev1_lag`h'"
    }

                eventstudyinteract turnover_ceo `evvars0' `evvars1' ///
                    if (balanced_2_year_sample == 1 | never_tar == 1), ///
                    vce(cluster entity_uniqueid) ///
                    absorb(entity_uniqueid year) ///
                    cohort(tar_event_year) ///
                    control_cohort(never_tar)

                matrix b = e(b_iw)
                matrix V = e(V_iw)

	local i = 1
coefplot                                                                    ///
    (., keep(ev0_lead3 ev0_lead2 ev0_lead1 ev0_lag0 ev0_lag1 ev0_lag2 ev0_lag3) ///
        b(b_iw) v(V_iw) label(`label`i'_0')                                ///
        mcolor(navy) ciopts(lcolor(navy) recast(rcap))                      ///
        rename(ev0_lead3=a2 ev0_lead2=a3 ev0_lead1=a4                      ///
               ev0_lag0=a5 ev0_lag1=a6 ev0_lag2=a7 ev0_lag3=a8))           ///
    (., keep(ev1_lead3 ev1_lead2 ev1_lead1 ev1_lag0 ev1_lag1 ev1_lag2 ev1_lag3) ///
        b(b_iw) v(V_iw) label(`label`i'_1')                                ///
        mcolor(maroon) ciopts(lcolor(maroon) recast(rcap)) msymbol(D)      ///
        rename(ev1_lead3=a2 ev1_lead2=a3 ev1_lead1=a4                      ///
               ev1_lag0=a5 ev1_lag1=a6 ev1_lag2=a7 ev1_lag3=a8)),          ///
    vertical                                                                ///
    yline(0, lcolor(gs8))                                                   ///
    xline(3, lcolor(gs8) lpattern(dash))                                   ///
    order(a2 a3 a4 a5 a6 a7 a8)                                            ///
    coeflabels(a2="-3" a3="-2" a4="-1" a5="0" a6="1" a7="2" a8="3")       ///
    xtitle("Periods since the event") ytitle("Average effect")             ///
    graphregion(color(white)) plotregion(color(white))

	
eventstudyinteract turnover_ceo ev_lead* ev_lag* ///
                    if forprofit == 1 & (balanced_2_year_sample == 1 | never_tar == 1), ///
                    vce(cluster entity_uniqueid) ///
                    absorb(entity_uniqueid year) ///
                    cohort(tar_event_year) ///
                    control_cohort(never_tar)
					
				matrix b = e(b_iw)
                matrix V = e(V_iw)
                ereturn post b V

                lincom (ev_lag0 + ev_lag1 + ev_lag2) / 3

eventstudyinteract turnover_ceo ev_lead* ev_lag* ///
                    if forprofit == 0 &  (balanced_2_year_sample == 1 | never_tar == 1), ///
                    vce(cluster entity_uniqueid) ///
                    absorb(entity_uniqueid year) ///
                    cohort(tar_event_year) ///
                    control_cohort(never_tar)

				matrix b = e(b_iw)
                matrix V = e(V_iw)
                ereturn post b V

                // Average effect for group 0 (periods 0–2)
                lincom (ev_lag0 + ev_lag1 + ev_lag2) / 3
			
*==============================================================
* Summarize means
*==============================================================	

tempname fh
file open `fh' using `"${overleaf}/notes/Non CEO Event Study/tables/ceo_by_forprofit.tex"', write replace

file write `fh' "\begin{table}[htbp]\centering" _n
file write `fh' "\caption{CEO Turnover Means}" _n
file write `fh' "\begin{tabular}{lcc}" _n
file write `fh' "\hline\hline" _n
file write `fh' "Sample & Mean & N \\\\" _n
file write `fh' "\hline" _n

* Define the conditions and labels as parallel locals
local cond1 ""
local cond2 "balanced_2_year_sample == 1"
local cond3 "balanced_2_year_sample == 1 & forprofit == 1"
local cond4 "balanced_2_year_sample == 1 & forprofit == 1 & tar_reltime < 0"
local cond5 "balanced_2_year_sample == 1 & forprofit == 1 & tar_reltime >= 0"
local cond6 "balanced_2_year_sample == 1 & forprofit == 0"
local cond7 "balanced_2_year_sample == 1 & forprofit == 0 & tar_reltime < 0"
local cond8 "balanced_2_year_sample == 1 & forprofit == 0 & tar_reltime >= 0"

local lab1 "Full sample"
local lab2 "Balanced 2-year sample"
local lab3 "\quad FP"
local lab4 "\qquad Pre-event"
local lab5 "\qquad Post-event"
local lab6 "\quad NFP"
local lab7 "\qquad Pre-event"
local lab8 "\qquad Post-event"

forvalues r = 1/8 {

    if "`cond`r''" == "" {
        quietly mean turnover_ceo
        matrix T = r(table)
        local m = string(round(T[1,1], 0.001), "%9.3f")

        quietly count if !missing(turnover_ceo)
        local n = r(N)
    }
    else {
        quietly mean turnover_ceo if `cond`r''
        matrix T = r(table)
        local m = string(round(T[1,1], 0.001), "%9.3f")

        quietly count if `cond`r'' & !missing(turnover_ceo)
        local n = r(N)
    }

    if inlist(`r', 3, 6) {
        file write `fh' "\hline" _n
    }

    file write `fh' "`lab`r'' & `m' & `n' \\\\" _n
}

file write `fh' "\hline\hline" _n
file write `fh' "\end{tabular}" _n
file write `fh' "\end{table}" _n

file close `fh'
display `"Saved to ${overleaf}/notes/Non CEO Event Study/tables/ceo_by_forprofit.tex"'
