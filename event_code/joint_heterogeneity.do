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


*----------------------------------------------------------
* Define specification
*----------------------------------------------------------
local spec1_treated "balanced_2_year_sample == 1"
local spec1_control "never_tar == 1"
local spec1_cohort "never_tar"
local spec1_name "2yr Balanced, Never Treated"
local spec1_file "2yrbalanced_never_tar"

gen turnover_ceo_x_cfo = (turnover_ceo == 1 & turnover_cfo == 1) if !missing(turnover_ceo) & !missing(turnover_cfo)
gen turnover_ceo_x_coo = (turnover_ceo == 1 & turnover_coo == 1) if !missing(turnover_ceo) & !missing(turnover_coo)
gen turnover_ceo_x_cmo = (turnover_ceo == 1 & turnover_cmo == 1) if !missing(turnover_ceo) & !missing(turnover_cmo)
gen turnover_cfo_x_coo = (turnover_cfo == 1 & turnover_coo == 1) if !missing(turnover_cfo) & !missing(turnover_coo)
gen turnover_coo_x_cco = (turnover_coo == 1 & turnover_cco == 1) if !missing(turnover_coo) & !missing(turnover_cco)
gen turnover_cmo_x_cno = (turnover_cmo == 1 & turnover_cno == 1) if !missing(turnover_cmo) & !missing(turnover_cno)

*==============================================================
* Heterogeneity: Joint Turnover Outcomes
*   × Binary Hospital Characteristics
*==============================================================

// ── 0. Joint outcomes and nice labels ───────────────────────
local joint_outcomes "turnover_ceo_x_cfo turnover_ceo_x_coo turnover_ceo_x_cmo turnover_cfo_x_coo turnover_coo_x_cco turnover_cmo_x_cno"

local nice_turnover_ceo_x_cfo "CEO x CFO"
local nice_turnover_ceo_x_coo "CEO x COO"
local nice_turnover_ceo_x_cmo "CEO x CMO"
local nice_turnover_cfo_x_coo "CFO x COO"
local nice_turnover_coo_x_cco "COO x CCO"
local nice_turnover_cmo_x_cno "CMO x CNO"

// ── Binary split variables ────────────────────────────────
local vars aha_bdtot_orig aha_mcddc aha_mcrdc aha_fte

foreach v of local vars {
    quietly summarize `v', detail
    local med = r(p50)
    local suffix : subinstr local v "aha_" "", all
    gen high_`suffix' = (`v' > `med')
    bys aha_id: egen above_median_`suffix' = max(high_`suffix')
}

gen fpstatus = .
replace fpstatus = 1 if aha_own_fp == 1 & aha_own_np == 0
replace fpstatus = 0 if aha_own_fp == 0 & aha_own_np == 1

local binvar1 "fpstatus"
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

local splitnice1 "Ownership Type"
local splitnice2 "Bed Count"
local splitnice3 "Medicaid"
local splitnice4 "Medicare"
local splitnice5 "FTE"
local splitnice6 "Teaching Status"
local splitnice7 "CAH Status"

local n_splits 7

// ── 1. Master loop over splits ───────────────────────────────
forvalues s = 1/`n_splits' {

    local splitvar  "`binvar`s''"
    local splitname "`binname`s''"
    local lab0      "`label`s'_0'"
    local lab1      "`label`s'_1'"
    local splitnice "`splitnice`s''"

    display _newline(3) as result "=============================================="
    display as result " Split: `splitvar'  (`lab0' vs `lab1')"
    display as result "=============================================="

    // ── Collect results for all joint outcomes ───────────────
    tempfile forest_`splitname'
    capture postclose pf_handle
    postfile pf_handle str20 outcome_label int group float(b se premean) ///
             using `forest_`splitname'', replace

    foreach yvar of local joint_outcomes {

        local nice "`nice_`yvar''"
        display _newline as text "  → `yvar' × `splitvar' (joint)"

        preserve

        * Drop missing and rebuild sample for this outcome
        keep if !missing(`yvar')
        make_target_sample

        * Recreate base event-time dummies from new tar_reltime
        sum tar_reltime, meanonly
        local rmin_inner = r(min)
        local rmax_inner = r(max)

        forvalues h = 0/`rmax_inner' {
            cap drop ev_lag`h'
            gen byte ev_lag`h' = (tar_reltime == `h')
        }
        forvalues h = 1/`=abs(`rmin_inner')' {
            cap drop ev_lead`h'
            gen byte ev_lead`h' = (tar_reltime == -`h')
        }
        replace ev_lead1 = 0

        * Recreate interacted dummies for this split
        capture drop ev0_lag* ev1_lag* ev0_lead* ev1_lead*

        forvalues h = 0/`rmax_inner' {
            forvalues c = 0/1 {
                cap gen byte ev`c'_lag`h' = ev_lag`h' * (`splitvar' == `c')
            }
        }
        forvalues h = 1/`=abs(`rmin_inner')' {
            forvalues c = 0/1 {
                cap gen byte ev`c'_lead`h' = ev_lead`h' * (`splitvar' == `c')
            }
        }
        forvalues c = 0/1 {
            replace ev`c'_lead1 = 0
        }

        * Rebuild evvars0 and evvars1 from inner dims
        local evvars0 ""
        local evvars1 ""
        forvalues h = `=abs(`rmin_inner')' (-1) 2 {
            local evvars0 "`evvars0' ev0_lead`h'"
            local evvars1 "`evvars1' ev1_lead`h'"
        }
        forvalues h = 0/`rmax_inner' {
            local evvars0 "`evvars0' ev0_lag`h'"
            local evvars1 "`evvars1' ev1_lag`h'"
        }

        capture {
            eventstudyinteract `yvar' `evvars0' `evvars1' ///
                if (balanced_2_year_sample == 1 | never_tar == 1), ///
                vce(cluster entity_uniqueid) ///
                absorb(entity_uniqueid year) ///
                cohort(tar_event_year) ///
                control_cohort(never_tar)

            matrix b = e(b_iw)
            matrix V = e(V_iw)
            ereturn post b V

            lincom (ev0_lag0 + ev0_lag1 + ev0_lag2) / 3
            local avg_b0  = r(estimate)
            local avg_se0 = r(se)

            lincom (ev1_lag0 + ev1_lag1 + ev1_lag2) / 3
            local avg_b1  = r(estimate)
            local avg_se1 = r(se)
        }
        if _rc != 0 {
            local avg_b0  = .
            local avg_se0 = .
            local avg_b1  = .
            local avg_se1 = .
        }

        quietly summarize `yvar' ///
            if (balanced_2_year_sample == 1 | never_tar == 1) ///
               & `splitvar' == 0 & tar_reltime < 0, meanonly
        local pmean0 = r(mean)

        quietly summarize `yvar' ///
            if (balanced_2_year_sample == 1 | never_tar == 1) ///
               & `splitvar' == 1 & tar_reltime < 0, meanonly
        local pmean1 = r(mean)

        restore

        post pf_handle ("`nice'") (0) (`avg_b0') (`avg_se0') (`pmean0')
        post pf_handle ("`nice'") (1) (`avg_b1') (`avg_se1') (`pmean1')
    }
    postclose pf_handle

    // ── Build forest plots ───────────────────────────────────
    preserve
    use `forest_`splitname'', clear

    encode outcome_label, gen(oid)
    quietly summarize oid
    local oidmax = r(max)
    gen ypos = `oidmax' + 1 - oid
    replace ypos = ypos + 0.15 if group == 0
    replace ypos = ypos - 0.15 if group == 1

    gen lo = b - 1.96 * se
    gen hi = b + 1.96 * se

    gen b_norm  = b / premean
    gen se_norm = se / abs(premean)
    gen lo_norm = b_norm - 1.96 * se_norm
    gen hi_norm = b_norm + 1.96 * se_norm

    levelsof oid, local(olevels)
    local ylabs ""
    foreach lev of local olevels {
        local oname : label (oid) `lev'
        local yval = `oidmax' + 1 - `lev'
        local ylabs `"`ylabs' `yval' "`oname'""'
    }

    // ── PLOT A: Raw average treatment effects ────────────────
    twoway ///
        (rcap lo hi ypos if group == 0, ///
            horizontal lcolor(navy) lwidth(medthick)) ///
        (scatter ypos b if group == 0, ///
            msymbol(O) mcolor(navy) msize(medium)) ///
        (rcap lo hi ypos if group == 1, ///
            horizontal lcolor(cranberry) lwidth(medthick)) ///
        (scatter ypos b if group == 1, ///
            msymbol(D) mcolor(cranberry) msize(medium)) ///
        , ///
        xline(0, lpattern(dash) lcolor(gs8)) ///
        ylabel(`ylabs', angle(0) labsize(medium) nogrid) ///
        ytitle("") ///
        xtitle("Avg. Treatment Effect (t=0 to t=2)", size(medium)) ///
        title("Joint Turnover by `splitnice'", size(medlarge)) ///
        legend(order(2 "`lab0'" 4 "`lab1'") ///
               rows(1) position(6) size(small)) ///
        graphregion(color(white)) plotregion(margin(l=2 r=2)) ///
        name(joint_het_raw_`splitname', replace)

    graph export "${overleaf}/notes/Non CEO Event Study/figures/joint_het_raw_`splitname'.png", ///
        width(1800) replace

    // ── PLOT B: Normalised by pre-period mean ────────────────
    twoway ///
        (rcap lo_norm hi_norm ypos if group == 0, ///
            horizontal lcolor(navy) lwidth(medthick)) ///
        (scatter ypos b_norm if group == 0, ///
            msymbol(O) mcolor(navy) msize(medium)) ///
        (rcap lo_norm hi_norm ypos if group == 1, ///
            horizontal lcolor(cranberry) lwidth(medthick)) ///
        (scatter ypos b_norm if group == 1, ///
            msymbol(D) mcolor(cranberry) msize(medium)) ///
        , ///
        xline(0, lpattern(dash) lcolor(gs8)) ///
        ylabel(`ylabs', angle(0) labsize(medium) nogrid) ///
        ytitle("") ///
        xtitle("Avg. Effect / Pre-Period Mean", size(medium)) ///
        title("Joint Turnover (Normalised) by `splitnice'", size(medlarge)) ///
        legend(order(2 "`lab0'" 4 "`lab1'") ///
               rows(1) position(6) size(small)) ///
        graphregion(color(white)) plotregion(margin(l=2 r=2)) ///
        name(joint_het_norm_`splitname', replace)

    graph export "${overleaf}/notes/Non CEO Event Study/figures/joint_het_norm_`splitname'.png", ///
        width(1800) replace

    restore
}
// end split loop

display _newline(2) as result "All forest plots saved."
