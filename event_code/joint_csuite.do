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
local spec1_cohort "never_tar"
local spec1_name "2yr Balanced, Never Treated"
local spec1_file "2yrbalanced_never_tar"

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
* Joint Turnover
*----------------------------------------------------------
local pairs "ceo_cfo"
local role1  "ceo"
local role2  "cfo"

forvalues i = 1/`=wordcount("`pairs'")' {
    local pair : word `i' of `pairs'
    local r1   : word `i' of `role1'
    local r2   : word `i' of `role2'
	
	 gen turnover_`r1'_x_`r2' = (turnover_`r1' == 1 & turnover_`r2' == 1) ///
        if !missing(turnover_`r1') & !missing(turnover_`r2')
    gen `r1'_not_`r2' = (turnover_`r1' == 1 & turnover_`r2' == 0) ///
        if !missing(turnover_`r1') & !missing(turnover_`r2')

    *----------------------------------------------------------
    * Joint regression (r1 x r2)
    *----------------------------------------------------------
    capture noisily eventstudyinteract turnover_`r1'_x_`r2' ev_lead* ev_lag* ///
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
    * Non-joint regression (r1 only)
    *----------------------------------------------------------
    capture noisily eventstudyinteract `r1'_not_`r2' ev_lead* ev_lag* ///
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
    * Build plot data
    *----------------------------------------------------------
    tempfile plotdata_`pair'
    tempname ploth
    postfile `ploth' int period byte comp double y0 y1 total str8 pct_lbl ///
        using `plotdata_`pair'', replace

    local events  "ev_lead2 ev_lead1 ev_lag0 ev_lag1 ev_lag2"
    local periods "-2 -1 0 1 2"
    local idx = 1
    foreach ev of local events {
        local p : word `idx' of `periods'
        local b_nj = b_nonjoint_save[1, colnumb(b_nonjoint_save, "`ev'")]
        local b_j  = b_joint_save[1,    colnumb(b_joint_save,    "`ev'")]
        local b_t  = `b_nj' + `b_j'

        local pct_nj = .
        local pct_j  = .
        if abs(`b_t') > 1e-12 {
            local pct_nj = 100 * `b_nj' / `b_t'
            local pct_j  = 100 * `b_j'  / `b_t'
        }

        local lbl_nj ""
        local lbl_j  ""
        if `pct_nj' < . {
            local lbl_nj : display %4.1f `pct_nj'
            local lbl_nj = trim("`lbl_nj'") + "%"
        }
        if `pct_j' < . {
            local lbl_j : display %4.1f `pct_j'
            local lbl_j = trim("`lbl_j'") + "%"
        }
        if `p' == -2 local lbl_j ""

        post `ploth' (`p') (1) (0)      (`b_nj') (`b_t') ("`lbl_nj'")
        post `ploth' (`p') (2) (`b_nj') (`b_t')  (`b_t') ("`lbl_j'")
        local ++idx
    }
    postclose `ploth'

    *----------------------------------------------------------
    * Plot
    *----------------------------------------------------------
    preserve
    use `plotdata_`pair'', clear
    local r1_upper = upper("`r1'")
    local r2_upper = upper("`r2'")
    twoway ///
        (rbar y0 y1 period if comp == 1, barw(0.8) color(cranberry%70) lcolor(cranberry%90)) ///
        (rbar y0 y1 period if comp == 2, barw(0.8) color(navy%70)      lcolor(navy%90))      ///
        (scatter y1 period if comp == 1, msymbol(none) mlabel(pct_lbl) mlabcolor(cranberry) mlabsize(vsmall) mlabposition(12)) ///
        (scatter y1 period if comp == 2, msymbol(none) mlabel(pct_lbl) mlabcolor(navy)      mlabsize(vsmall) mlabposition(12)) ///
        , ///
        yline(0, lcolor(gs8)) ///
        xline(-1, lcolor(gs8) lpattern(dash)) ///
        xlabel(-2(1)2) ///
        xtitle("Event time") ///
        ytitle("Average effect") ///
        title("`r1_upper' Turnover Decomposition: Joint vs Non-joint with `r2_upper'", size(medium)) ///
        subtitle("`spec1_name' | Joint Avg: `joint_avg_fmt' (SE: `joint_se_fmt') | Non-joint Avg: `nonjoint_avg_fmt' (SE: `nonjoint_se_fmt')", size(small)) ///
        legend(order(1 "Non-joint (`r1_upper' only)" 2 "Joint (`r1_upper' x `r2_upper')") rows(1) position(6)) ///
        graphregion(color(white))
    graph export "${overleaf}/notes/Non CEO Event Study/figures/joint_csuite_`pair'_decomp_event.pdf", as(pdf) replace
    restore
}
