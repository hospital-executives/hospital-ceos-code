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

// Get Nice Titles
local titles     "ceo cfo coo cmo cno cco cio"
local nice_ceo "CEO"
local nice_cfo "CFO"
local nice_coo "COO"
local nice_cmo "CMO"
local nice_cno "CNO"
local nice_cco "CCO"
local nice_cio "CIO"

local turnover_outcomes "turnover_ceo turnover_cfo turnover_coo turnover_cmo turnover_cno turnover_cco turnover_cio"

// ── Split definitions ───────────────────────────────────────
// Now the split variable is role-specific: prev_gender_`t', prev_md_`t'
// So we define a prefix + labels; the actual variable is `prefix'_`t'

local n_splits 2

// Recode prev_gender_* from M/F strings to 0/1 numeric
foreach t of local titles {
    cap confirm string variable prev_gender_`t'
    if _rc == 0 {
        gen byte prev_female_`t' = (prev_gender_`t' == "F") if !missing(prev_gender_`t')
    }
}

local binprefix1 "prev_female"
local binname1   "gender"
local label1_0   "Male Prev."
local label1_1   "Female Prev."

local binprefix2 "prev_md"
local binname2   "md"
local label2_0   "Non-MD Prev."
local label2_1   "MD Prev."

// ── 1. Master loop over splits ──────────────────────────────
forvalues s = 1/`n_splits' {

    local splitpfx   "`binprefix`s''"
    local splitname  "`binname`s''"
    local lab0       "`label`s'_0'"
    local lab1       "`label`s'_1'"

    display _newline(3) as result "=============================================="
    display as result " Split prefix: `splitpfx'  (`lab0' vs `lab1')"
    display as result "=============================================="

    // ── Turnover outcomes ───────────────────────────────────
    {
        local fam    "turnover"
        local prefix "turnover_"

        // Collect results
        tempfile forest_`fam'_`splitname'
        capture postclose pf_handle
        postfile pf_handle str10 title group float(b se premean) ///
                 using `forest_`fam'_`splitname'', replace

        // Collect sample counts
        tempfile counts_`fam'_`splitname'
        capture postclose pc_handle
        postfile pc_handle str10 title group float(n_total n_treated n_control) ///
                 using `counts_`fam'_`splitname'', replace

        // ── Loop over titles ────────────────────────────────
        foreach t of local titles {

            local yvar    "`prefix'`t'"
            local splitvar "`splitpfx'_`t'"    // e.g. prev_female_ceo

            display _newline as text "  → `yvar' × `splitvar'"

            // ── Preserve, drop missing, rebuild sample ──────
            preserve

            drop if missing(`splitvar')
            make_target_sample

            // Rebuild relative time range for this subsample
            sum tar_reltime, meanonly
            local rmin_t = r(min)
            local rmax_t = r(max)

            // Rebuild base event-time indicators
            capture drop ev_lag* ev_lead*
            forvalues h = 0/`rmax_t' {
                gen byte ev_lag`h' = (tar_reltime == `h')
            }
            forvalues h = 1/`=abs(`rmin_t')' {
                gen byte ev_lead`h' = (tar_reltime == -`h')
            }
            replace ev_lead1 = 0

            // ── Create interacted event-time dummies ────────
            forvalues h = 0/`rmax_t' {
                forvalues c = 0/1 {
                    gen byte ev`c'_lag`h' = ev_lag`h' * (`splitvar' == `c')
                }
            }
            forvalues h = 2/`=abs(`rmin_t')' {
                forvalues c = 0/1 {
                    gen byte ev`c'_lead`h' = ev_lead`h' * (`splitvar' == `c')
                }
            }
            // lead1 is the omitted baseline — no interaction needed

            // ── Build variable lists for regression ─────────
            local evvars0 ""
            local evvars1 ""
            forvalues h = `=abs(`rmin_t')' (-1) 2 {
                local evvars0 "`evvars0' ev0_lead`h'"
                local evvars1 "`evvars1' ev1_lead`h'"
            }
            forvalues h = 0/`rmax_t' {
                local evvars0 "`evvars0' ev0_lag`h'"
                local evvars1 "`evvars1' ev1_lag`h'"
            }

            // ── Regression ──────────────────────────────────
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

                // Average effect for group 0 (periods 0–2)
                lincom (ev0_lag0 + ev0_lag1 + ev0_lag2) / 3
                local avg_b0  = r(estimate)
                local avg_se0 = r(se)

                // Average effect for group 1 (periods 0–2)
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

            // Pre-period means by group
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

            // Sample counts by group (unique hospitals)
            forvalues c = 0/1 {
                // Total hospitals in group
                quietly distinct entity_uniqueid ///
                    if (balanced_2_year_sample == 1 | never_tar == 1) ///
                       & `splitvar' == `c'
                local n_total_`c' = r(ndistinct)

                // Treated (balanced 2-year sample)
                quietly distinct entity_uniqueid ///
                    if balanced_2_year_sample == 1 ///
                       & `splitvar' == `c'
                local n_treated_`c' = r(ndistinct)

                // Controls (never treated)
                quietly distinct entity_uniqueid ///
                    if never_tar == 1 ///
                       & `splitvar' == `c'
                local n_control_`c' = r(ndistinct)
            }

            restore

            post pf_handle ("`t'") (0) (`avg_b0') (`avg_se0') (`pmean0')
            post pf_handle ("`t'") (1) (`avg_b1') (`avg_se1') (`pmean1')

            post pc_handle ("`t'") (0) (`n_total_0') (`n_treated_0') (`n_control_0')
            post pc_handle ("`t'") (1) (`n_total_1') (`n_treated_1') (`n_control_1')
        }
        // end title loop

        postclose pf_handle
        postclose pc_handle

        // ── Export sample counts table ──────────────────────
        preserve
        use `counts_`fam'_`splitname'', clear

        gen group_label = "`lab0'" if group == 0
        replace group_label = "`lab1'" if group == 1
        replace title = upper(title)

        order title group_label n_total n_treated n_control
        label var title        "Role"
        label var group_label  "Group"
        label var n_total      "Total Hospitals"
        label var n_treated    "Treated"
        label var n_control    "Controls"

        sort title group
        export delimited using ///
            "${overleaf}/notes/Non CEO Event Study/figures/counts_`fam'_`splitname'.csv", ///
            replace

        listtab title group_label n_total n_treated n_control ///
            using "${overleaf}/notes/Non CEO Event Study/figures/counts_`fam'_`splitname'.tex", ///
            rstyle(tabular) replace ///
            head("\begin{tabular}{llccc}" "\hline" ///
                 "Role & Group & Total & Treated & Controls \\" "\hline") ///
            foot("\hline" "\end{tabular}")

        restore

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
            title("`ptitle_fam': `lab0' vs `lab1'", size(medlarge)) ///
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
            title("`ptitle_fam' (Normalised): `lab0' vs `lab1'", ///
                  size(medlarge)) ///
            legend(order(2 "`lab0'" 4 "`lab1'") ///
                   rows(1) position(6) size(small)) ///
            graphregion(color(white)) plotregion(margin(l=2 r=2)) ///
            name(forest_`fam'_norm_`splitname', replace)

        graph export "${overleaf}/notes/Non CEO Event Study/figures/forest_`fam'_norm_`splitname'.png", ///
            width(1800) replace

        restore
    }
    // end turnover outcomes
}
// end split loop

display _newline(2) as result "All forest plots saved."
