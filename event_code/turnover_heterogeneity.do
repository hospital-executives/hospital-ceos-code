
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

// make_target_sample

* Define outcome variables
local turnover_outcomes "turnover_ceo turnover_cfo turnover_coo turnover_cmo turnover_cno turnover_cco turnover_cio"
local vacancy_outcomes "vacant_ceo vacant_cfo vacant_coo vacant_cmo vacant_cno vacant_cco vacant_cio"
local all_outcomes "`turnover_outcomes' `vacancy_outcomes'"

*==============================================================
* Forest Plots: Average Treatment Effects by C-Suite Title
*   × Binary Hospital Characteristics
*==============================================================

// ── 0. Prelims ──────────────────────────────────────────────

// Get Nice Titles
local titles     "ceo cfo coo cmo cno cco cio"
local nice_ceo "CEO"
local nice_cfo "CFO"
local nice_coo "COO"
local nice_cmo "CMO"
local nice_cno "CNO"
local nice_cco "CCO"
local nice_cio "CIO"

// Make above/below median splits
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

// Prespecify your binary variables and labels
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

local turnover_outcomes "turnover_ceo turnover_cfo turnover_coo turnover_cmo turnover_cno turnover_cco turnover_cio"
local vacancy_outcomes  "vacant_ceo vacant_cfo vacant_coo vacant_cmo vacant_cno vacant_cco vacant_cio"

local n_splits 7   // number of binary split variables

// ── 1. Master loop over splits ──────────────────────────────
forvalues s = 1/`n_splits' {

    local splitvar  "`binvar`s''"
    local splitname "`binname`s''"
    local lab0      "`label`s'_0'"
    local lab1      "`label`s'_1'"
    local splitnice "`splitnice`s''"

    display _newline(3) as result "=============================================="
    display as result " Split: `splitvar'  (`lab0' vs `lab1')"
    display as result "=============================================="

    // ── Loop over outcome families ──────────────────────────
    foreach fam in turnover vacancy {

        if "`fam'" == "turnover" {
            local outcomes "`turnover_outcomes'"
            local prefix   "turnover_"
        }
        else {
            local outcomes "`vacancy_outcomes'"
            local prefix   "vacant_"
        }

        // Collect results
        tempfile forest_`fam'_`splitname'
        capture postclose pf_handle
        postfile pf_handle str10 title group float(b se premean) ///
                 using `forest_`fam'_`splitname'', replace

        foreach t of local titles {

			local yvar "`prefix'`t'"
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

			* Pre-period means by group
			quietly summarize `yvar' ///
				if (balanced_2_year_sample == 1 | never_tar == 1) ///
				   & `splitvar' == 0 ///
				   & tar_reltime < 0, meanonly
			local pmean0 = r(mean)

			quietly summarize `yvar' ///
				if (balanced_2_year_sample == 1 | never_tar == 1) ///
				   & `splitvar' == 1 ///
				   & tar_reltime < 0, meanonly
			local pmean1 = r(mean)

			restore

			post pf_handle ("`t'") (0) (`avg_b0') (`avg_se0') (`pmean0')
			post pf_handle ("`t'") (1) (`avg_b1') (`avg_se1') (`pmean1')
		}

        postclose pf_handle

        // ── Build forest plots ──────────────────────────────
        preserve
        use `forest_`fam'_`splitname'', clear

        // Numeric position for y-axis
        encode title, gen(tid)
        quietly summarize tid
        local tidmax = r(max)
        gen ypos = `tidmax' + 1 - tid
        replace ypos = ypos + 0.15 if group == 0
        replace ypos = ypos - 0.15 if group == 1

        // CIs
        gen lo = b - 1.96 * se
        gen hi = b + 1.96 * se

        // Normalised version
        gen b_norm  = b / premean
        gen se_norm = se / abs(premean)
        gen lo_norm = b_norm - 1.96 * se_norm
        gen hi_norm = b_norm + 1.96 * se_norm

        // Y-axis labels
        levelsof tid, local(tlevels)
        local ylabs ""
        foreach lev of local tlevels {
            local tname : label (tid) `lev'
            local tname = upper("`tname'")
            local yval = `tidmax' + 1 - `lev'
            local ylabs `"`ylabs' `yval' "`tname'""'
        }

        // ── PLOT A: Raw average treatment effects ───────────
        local ptitle_fam = proper("`fam'")

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
            title("`ptitle_fam' by `splitnice'", size(medlarge)) ///
            legend(order(2 "`lab0'" 4 "`lab1'") ///
                   rows(1) position(6) size(small)) ///
            graphregion(color(white)) plotregion(margin(l=2 r=2)) ///
            name(forest_`fam'_raw_`splitname', replace)

        graph export "${overleaf}/notes/Non CEO Event Study/figures/forest_`fam'_raw_`splitname'.png", ///
            width(1800) replace

        // ── PLOT B: Normalised by pre-period mean ───────────
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
            title("`ptitle_fam' (Normalised) by `splitnice'", ///
                  size(medlarge)) ///
            legend(order(2 "`lab0'" 4 "`lab1'") ///
                   rows(1) position(6) size(small)) ///
            graphregion(color(white)) plotregion(margin(l=2 r=2)) ///
            name(forest_`fam'_norm_`splitname', replace)

        graph export "${overleaf}/notes/Non CEO Event Study/figures/forest_`fam'_norm_`splitname'.png", ///
            width(1800) replace

        restore
    }
    // end outcome-family loop
}
// end split loop

display _newline(2) as result "All forest plots saved."
