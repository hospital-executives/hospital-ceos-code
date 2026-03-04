/* JOINT_CSUITE_V2 *************************************************************

Program name:   joint_csuite_v2.do
Programmer:     Katherine Papen

Goal:           Decomposition event study plots for CEO joint turnover with
                other C-suite roles, allowing for missing data in other roles.
                Three components per plot:
                  (1) CEO=1 & other role(s) turn over                  [joint]
                  (2) CEO=1 & other roles fully observed & no turnover [non-joint]
                  (3) CEO=1 & some other role(s) missing & none observed turning over [missing]

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

* merge in turnover outcomes
merge 1:1 entity_uniqueid year using "${dbdata}/derived/hospitals_with_turnover.dta", keep(match) nogen

make_target_sample

*----------------------------------------------------------
* Define specification
*----------------------------------------------------------
local spec1_treated "balanced_2_year_sample == 1"
local spec1_control "never_tar == 1"
local spec1_cohort  "never_tar"
local spec1_name    "2yr Balanced, Never Treated"

*----------------------------------------------------------
* Relative time indicators
*----------------------------------------------------------
sum tar_reltime, meanonly
local rmin = r(min)
local rmax = r(max)

forvalues h = 0/`rmax' {
	gen byte ev_lag`h' = (tar_reltime == `h')
}
forvalues h = 1/`=abs(`rmin')' {
	gen byte ev_lead`h' = (tar_reltime == -`h')
}
replace ev_lead1 = 0

*----------------------------------------------------------
* Define intermediate variables for each group
*----------------------------------------------------------

* --- CFO ---
gen any_cfo  = (turnover_cfo == 1)
gen miss_cfo = missing(turnover_cfo)

gen ceo_x_cfo   = (turnover_ceo == 1 &  any_cfo == 1)                  if !missing(turnover_ceo)
gen ceo_not_cfo = (turnover_ceo == 1 & !miss_cfo  & any_cfo == 0)       if !missing(turnover_ceo)
gen ceo_mis_cfo = (turnover_ceo == 1 &  miss_cfo  & any_cfo == 0)       if !missing(turnover_ceo)

* --- COO ---
gen any_coo  = (turnover_coo == 1)
gen miss_coo = missing(turnover_coo)

gen ceo_x_coo   = (turnover_ceo == 1 &  any_coo == 1)                  if !missing(turnover_ceo)
gen ceo_not_coo = (turnover_ceo == 1 & !miss_coo  & any_coo == 0)       if !missing(turnover_ceo)
gen ceo_mis_coo = (turnover_ceo == 1 &  miss_coo  & any_coo == 0)       if !missing(turnover_ceo)

* --- Any C-Suite (CFO, CMO, COO, CCO, CNO) ---
gen any_csuite  = (turnover_cfo == 1 | turnover_cmo == 1 | turnover_coo == 1 | ///
                   turnover_cco == 1 | turnover_cno == 1)
gen miss_csuite = missing(turnover_cfo) | missing(turnover_cmo) | missing(turnover_coo) | ///
                  missing(turnover_cco) | missing(turnover_cno)

gen ceo_x_csuite   = (turnover_ceo == 1 &  any_csuite == 1)                    if !missing(turnover_ceo)
gen ceo_not_csuite = (turnover_ceo == 1 & !miss_csuite & any_csuite == 0)       if !missing(turnover_ceo)
gen ceo_mis_csuite = (turnover_ceo == 1 &  miss_csuite & any_csuite == 0)       if !missing(turnover_ceo)

* --- CFO or COO ---
gen any_cfo_coo  = (turnover_cfo == 1 | turnover_coo == 1)
gen miss_cfo_coo = missing(turnover_cfo) | missing(turnover_coo)

gen ceo_x_cfo_coo   = (turnover_ceo == 1 &  any_cfo_coo == 1)                    if !missing(turnover_ceo)
gen ceo_not_cfo_coo = (turnover_ceo == 1 & !miss_cfo_coo & any_cfo_coo == 0)     if !missing(turnover_ceo)
gen ceo_mis_cfo_coo = (turnover_ceo == 1 &  miss_cfo_coo & any_cfo_coo == 0)     if !missing(turnover_ceo)

* --- CFO, COO, or CIO ---
gen any_cfo_coo_cio  = (turnover_cfo == 1 | turnover_coo == 1 | turnover_cio == 1)
gen miss_cfo_coo_cio = missing(turnover_cfo) | missing(turnover_coo) | missing(turnover_cio)

gen ceo_x_cfo_coo_cio   = (turnover_ceo == 1 &  any_cfo_coo_cio == 1)                      if !missing(turnover_ceo)
gen ceo_not_cfo_coo_cio = (turnover_ceo == 1 & !miss_cfo_coo_cio & any_cfo_coo_cio == 0)   if !missing(turnover_ceo)
gen ceo_mis_cfo_coo_cio = (turnover_ceo == 1 &  miss_cfo_coo_cio & any_cfo_coo_cio == 0)   if !missing(turnover_ceo)

*----------------------------------------------------------
* Loop over groups: run 3 regressions, build plot, export
*----------------------------------------------------------
local ngroups = 5
local grp1 "cfo"
local lbl1 "CFO"
local file1 "cfo"

local grp2 "coo"
local lbl2 "COO"
local file2 "coo"

local grp3 "csuite"
local lbl3 "Any C-Suite"
local file3 "any_csuite"

local grp4 "cfo_coo"
local lbl4 "CFO or COO"
local file4 "cfo_coo"

local grp5 "cfo_coo_cio"
local lbl5 "CFO, COO, or CIO"
local file5 "cfo_coo_cio"

forvalues i = 1/`ngroups' {
	local grp  "`grp`i''"
	local lbl  "`lbl`i''"
	local file "`file`i''"

	display _newline(2) "{hline 60}"
	display "Group `i': CEO x `lbl'"
	display "{hline 60}"

	*----------------------------------------------------------
	* Joint regression (CEO x other turnover)
	*----------------------------------------------------------
	capture noisily eventstudyinteract ceo_x_`grp' ev_lead* ev_lag* ///
		if (`spec1_treated' | `spec1_control'), ///
		vce(cluster entity_uniqueid) ///
		absorb(entity_uniqueid year) ///
		cohort(tar_event_year) ///
		control_cohort(`spec1_cohort')
	matrix b_joint      = e(b_iw)
	matrix V_joint      = e(V_iw)
	matrix b_joint_save = b_joint
	ereturn post b_joint V_joint
	lincom (ev_lag0 + ev_lag1 + ev_lag2)/3
	local joint_avg     = r(estimate)
	local joint_se      = r(se)
	local joint_avg_fmt : display %9.3f `joint_avg'
	local joint_se_fmt  : display %9.3f `joint_se'

	*----------------------------------------------------------
	* Non-joint regression (CEO only, other roles fully observed, no turnover)
	*----------------------------------------------------------
	capture noisily eventstudyinteract ceo_not_`grp' ev_lead* ev_lag* ///
		if (`spec1_treated' | `spec1_control'), ///
		vce(cluster entity_uniqueid) ///
		absorb(entity_uniqueid year) ///
		cohort(tar_event_year) ///
		control_cohort(`spec1_cohort')
	matrix b_nonjoint      = e(b_iw)
	matrix V_nonjoint      = e(V_iw)
	matrix b_nonjoint_save = b_nonjoint
	ereturn post b_nonjoint V_nonjoint
	lincom (ev_lag0 + ev_lag1 + ev_lag2)/3
	local nonjoint_avg     = r(estimate)
	local nonjoint_se      = r(se)
	local nonjoint_avg_fmt : display %9.3f `nonjoint_avg'
	local nonjoint_se_fmt  : display %9.3f `nonjoint_se'

	*----------------------------------------------------------
	* Missing regression (CEO only, some other role data missing, none observed turning over)
	*----------------------------------------------------------
	capture noisily eventstudyinteract ceo_mis_`grp' ev_lead* ev_lag* ///
		if (`spec1_treated' | `spec1_control'), ///
		vce(cluster entity_uniqueid) ///
		absorb(entity_uniqueid year) ///
		cohort(tar_event_year) ///
		control_cohort(`spec1_cohort')
	matrix b_missing      = e(b_iw)
	matrix V_missing      = e(V_iw)
	matrix b_missing_save = b_missing
	ereturn post b_missing V_missing
	lincom (ev_lag0 + ev_lag1 + ev_lag2)/3
	local missing_avg     = r(estimate)
	local missing_se      = r(se)
	local missing_avg_fmt : display %9.3f `missing_avg'
	local missing_se_fmt  : display %9.3f `missing_se'

	*----------------------------------------------------------
	* Build plot data
	*----------------------------------------------------------
	tempfile plotdata
	tempname ploth
	postfile `ploth' int period byte comp double y0 y1 total str8 pct_lbl ///
		using `plotdata', replace

	local events  "ev_lead2 ev_lead1 ev_lag0 ev_lag1 ev_lag2"
	local periods "-2 -1 0 1 2"
	local idx = 1
	foreach ev of local events {
		local p : word `idx' of `periods'
		local b_nj = b_nonjoint_save[1, colnumb(b_nonjoint_save, "`ev'")]
		local b_j  = b_joint_save[1,    colnumb(b_joint_save,    "`ev'")]
		local b_m  = b_missing_save[1,  colnumb(b_missing_save,  "`ev'")]
		local b_t  = `b_nj' + `b_j' + `b_m'

		local pct_nj = .
		local pct_j  = .
		local pct_m  = .
		if abs(`b_t') > 1e-12 {
			local pct_nj = 100 * `b_nj' / `b_t'
			local pct_j  = 100 * `b_j'  / `b_t'
			local pct_m  = 100 * `b_m'  / `b_t'
		}

		local lbl_nj ""
		local lbl_j  ""
		local lbl_m  ""
		if `pct_nj' < . {
			local lbl_nj : display %4.1f `pct_nj'
			local lbl_nj = trim("`lbl_nj'") + "%"
		}
		if `pct_j' < . {
			local lbl_j : display %4.1f `pct_j'
			local lbl_j = trim("`lbl_j'") + "%"
		}
		if `pct_m' < . {
			local lbl_m : display %4.1f `pct_m'
			local lbl_m = trim("`lbl_m'") + "%"
		}
		if `p' == -2 {
			local lbl_j ""
			local lbl_m ""
		}

		post `ploth' (`p') (1) (0)               (`b_nj')                  (`b_t') ("`lbl_nj'")
		post `ploth' (`p') (2) (`b_nj')          (`b_nj' + `b_j')          (`b_t') ("`lbl_j'")
		post `ploth' (`p') (3) (`b_nj' + `b_j') (`b_nj' + `b_j' + `b_m')  (`b_t') ("`lbl_m'")
		local ++idx
	}
	postclose `ploth'

	*----------------------------------------------------------
	* Plot
	*----------------------------------------------------------
	preserve
	use `plotdata', clear
	twoway ///
		(rbar y0 y1 period if comp == 1, barw(0.8) color(cranberry%70) lcolor(cranberry%90)) ///
		(rbar y0 y1 period if comp == 2, barw(0.8) color(navy%70)      lcolor(navy%90))      ///
		(rbar y0 y1 period if comp == 3, barw(0.8) color(dkgreen%70)   lcolor(dkgreen%90))   ///
		(scatter y1 period if comp == 1, msymbol(none) mlabel(pct_lbl) mlabcolor(cranberry) mlabsize(vsmall) mlabposition(12)) ///
		(scatter y1 period if comp == 2, msymbol(none) mlabel(pct_lbl) mlabcolor(navy)      mlabsize(vsmall) mlabposition(12)) ///
		(scatter y1 period if comp == 3, msymbol(none) mlabel(pct_lbl) mlabcolor(dkgreen)   mlabsize(vsmall) mlabposition(12)) ///
		, ///
		yline(0, lcolor(gs8)) ///
		xline(-1, lcolor(gs8) lpattern(dash)) ///
		xlabel(-2(1)2) ///
		xtitle("Event time") ///
		ytitle("Average effect") ///
		title("CEO Turnover Decomposition: Joint vs Non-joint with `lbl'", size(medium)) ///
		subtitle("Jt: `joint_avg_fmt' (SE: `joint_se_fmt') | Non-jt: `nonjoint_avg_fmt' (SE: `nonjoint_se_fmt') | Miss: `missing_avg_fmt' (SE: `missing_se_fmt')", size(small)) ///
		legend(order(1 "Non-joint (CEO only, `lbl' observed)" 2 "Joint (CEO x `lbl')" 3 "CEO only (missing `lbl' data)") rows(3) position(6)) ///
		graphregion(color(white))
	graph export "${overleaf}/notes/Non CEO Event Study/figures/joint_csuite_ceo_`file'_decomp_event_v2.pdf", as(pdf) replace
	restore
}
