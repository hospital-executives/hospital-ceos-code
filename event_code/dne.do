/* DNE ************************************************************************

Program name: 	dne.do
Programmer: 	Katherine Papen

Goal: 			Analyse "does not exist" (DNE) outcomes for C-suite titles
				(CEO, CFO, COO, CMO, CNO, CCO, CIO) around CEO turnover events.

Outputs:
	1. Forest plots (raw and normalised) of average treatment effects by
	   heterogeneity split (ownership, bed count, Medicaid, Medicare, FTE,
	   teaching status, CAH status). Saved to:
	       ${overleaf}/notes/Non CEO Event Study/figures/forest_dne_*.png
	   Splits are skipped for a given title if any of the four cell counts
	   (splitvar x yvar) fall below 20 observations.

	2. Summary table of mean DNE rates by outcome and heterogeneity split.
	   Saved to:
	       ${overleaf}/notes/Non CEO Event Study/tables/dne_summary_table.tex

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
merge 1:1 entity_uniqueid year using "${dbdata}/derived/positions_by_tier.dta", keep(match) nogen

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

*-------------------------------------------------------------------------------
* STEP 1: Define role mappings with clean labels
*-------------------------------------------------------------------------------

// Define as paired lists: variable prefix | display label
// Tier 1 Business
local t1b_vars    "ceo cfo coo"
local t1b_n = 3
local t1b_lab1 "CEO"
local t1b_lab2 "CFO"  
local t1b_lab3 "COO"
local t1b_title "Tier 1: Business"

// Tier 1 Clinical
local t1c_vars    "medical_staff_chief chief_nursing_head"
local t1c_n = 2
local t1c_lab1 "Medical Staff Chief"
local t1c_lab2 "Chief Nursing Head"
local t1c_title "Tier 1: Clinical"

// Tier 1 IT/Legal/HR
local t1i_vars    "cio chief_compliance_officer csio_it_security_officer"
local t1i_n = 3
local t1i_lab1 "CIO"
local t1i_lab2 "Chief Compliance Officer"
local t1i_lab3 "CSIO/IT Security Officer"
local t1i_title "Tier 1: IT/Legal/HR"

// Tier 2 Business
local t2b_vars    "business_office_head marketing_head purchasing_head patient_accounting"
local t2b_n = 4
local t2b_lab1 "Business Office Head"
local t2b_lab2 "Marketing Head"
local t2b_lab3 "Purchasing Head"
local t2b_lab4 "Patient Accounting"
local t2b_title "Tier 2: Business"

// Tier 2 Clinical
local t2c_vars    "quality_head ob_head cardiology_head er_director or_head ambulatory_care_head patient_safety_head pathology_chief laboratory_director pharmacy_head radiology_med_dir"
local t2c_n = 11
local t2c_lab1  "Quality Head"
local t2c_lab2  "OB Head"
local t2c_lab3  "Cardiology Head"
local t2c_lab4  "ER Director"
local t2c_lab5  "OR Head"
local t2c_lab6  "Ambulatory Care Head"
local t2c_lab7  "Patient Safety Head"
local t2c_lab8  "Pathology Chief"
local t2c_lab9  "Laboratory Director"
local t2c_lab10 "Pharmacy Head"
local t2c_lab11 "Radiology Med Dir"
local t2c_title "Tier 2: Clinical"

// Tier 2 IT/Legal/HR
local t2i_vars    "it_director hr_head him_director facility_management_head director_of_technology clinical_systems_director"
local t2i_n = 6
local t2i_lab1 "IT Director"
local t2i_lab2 "HR Head"
local t2i_lab3 "HIM Director"
local t2i_lab4 "Facility Management Head"
local t2i_lab5 "Director of Technology"
local t2i_lab6 "Clinical Systems Director"
local t2i_title "Tier 2: IT/Legal/HR"

// All tier-group codes
local all_tg "t1b t1c t1i t2b t2c t2i"

// Outcome types
ds *dne
local dne_vars_raw `r(varlist)'

local dne_vars ""
foreach v of local dne_vars_raw {
    if !regexm("`v'", "[0-9]") {
        local dne_vars "`dne_vars' `v'"
    }
}

local dne_outcomes "ceo_dne cfo_dne coo_dne cmo_dne cno_dne cco_dne cio_dne"

*-------------------------------------------------------------------------------
* STEP 2: Run event studies and store results
*-------------------------------------------------------------------------------
local spec1_treated "balanced_2_year_sample == 1"
local spec1_control "never_tar == 1"
local spec1_cohort "never_tar"
local spec1_name "2yr Balanced, Never Treated"
local spec1_file "2yrbalanced_never_tar"

local nspecs = 1

// Get Nice Titles
rename chief_nursing_head_dne cno_dne
rename medical_staff_chief_dne cmo_dne
rename chief_compliance_officer_dne cco_dne
local titles   "ceo cfo coo cmo cno cco cio"
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
local binvar1 "forprofit"
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

local n_splits 7   // number of binary split variables

// ── 1. Identify lead/lag variables ──────────────────────────
// (assumes ev_lead* and ev_lag* already exist from your setup)
// Collect them into a local for the regression call
local evvars ""
forvalues h = 2(-1) 2 {          // leads (excl. lead1 = omitted)
    local evvars "`evvars' ev_lead`h'"
}
forvalues h = 0/3 {                        // lags
    local evvars "`evvars' ev_lag`h'"
}

// ── 2. Master loop ──────────────────────────────────────────

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

    // ── Create interacted event-time dummies ────────────────
    // (drop any prior versions first)
    capture drop ev0_lag* ev1_lag* ev0_lead* ev1_lead*

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
    forvalues h = 2 (-1) 2 {
        local evvars0 "`evvars0' ev0_lead`h'"
        local evvars1 "`evvars1' ev1_lead`h'"
    }
    // Lags for group 0 and 1
    forvalues h = 0/3 {
        local evvars0 "`evvars0' ev0_lag`h'"
        local evvars1 "`evvars1' ev1_lag`h'"
    }

    // ── Loop over outcome families ──────────────────────────
    
    local outcomes "`dne_outcomes'"
    local suffix   "_dne"
    local ptitle_fam "Does Not Exist"
	local fam "dne"

        // Collect results
        tempfile forest_`fam'_`splitname'
        capture postclose pf_handle
        postfile pf_handle str10 title group float(b se premean) ///
                 using `forest_`fam'_`splitname'', replace

        foreach t of local titles {

            local yvar "`t'`suffix'"
            display _newline as text "  → `yvar' × `splitvar' (joint)"

            // Skip if either group has fewer than 20 observations
            quietly count if (balanced_2_year_sample == 1 | never_tar == 1) & `splitvar' == 0 & `yvar' == 1
            local n0_a = r(N)
            quietly count if (balanced_2_year_sample == 1 | never_tar == 1) & `splitvar' == 0 & `yvar' == 0
            local n0_b = r(N)

            quietly count if (balanced_2_year_sample == 1 | never_tar == 1) & `splitvar' == 1 & `yvar' == 1
            local n1_a = r(N)
            quietly count if (balanced_2_year_sample == 1 | never_tar == 1) & `splitvar' == 1 & `yvar' == 0
            local n1_b = r(N)

            if `n0_a' < 20 | `n0_b' < 20 | `n1_a' < 20 | `n1_b' < 20 {
                display as text "  → Skipping: insufficient obs (n0_a=`n0_a', n0_b=`n0_b', n1_a=`n1_a', n1_b=`n1_b')"
                post pf_handle ("`t'") (0) (.) (.) (.)
                post pf_handle ("`t'") (1) (.) (.) (.)
                continue
            }

            capture {
                // Single joint regression
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
		 local ptitle_fam "Does Not Exist"

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

display _newline(2) as result "All forest plots saved."

*-------------------------------------------------------------------------------
* STEP 3: Summary table — mean DNE rate by outcome and split
*-------------------------------------------------------------------------------

local outcomes_tbl "ceo_dne cfo_dne coo_dne cmo_dne cno_dne cco_dne cio_dne"

// Column spec: label + overall + 2 cols per split
local colspec "l c"
forvalues s = 1/`n_splits' {
    local colspec "`colspec' cc"
}

// Cmidrule positions for split spans: split s occupies cols (2s+1)-(2s+2)
local cmidrules ""
forvalues s = 1/`n_splits' {
    local c1 = 2 * `s' + 1
    local c2 = 2 * `s' + 2
    local cmidrules "`cmidrules' \cmidrule(lr){`c1'-`c2'}"
}

capture file close tbl
file open tbl using "${overleaf}/notes/Non CEO Event Study/tables/dne_summary_table.tex", write replace

file write tbl "\begin{table}[htbp]" _n
file write tbl "\centering" _n
file write tbl "\caption{Fraction of Hospital-Years Where Title Does Not Exist}" _n
file write tbl "\label{tab:dne_summary}" _n
file write tbl "\resizebox{\textwidth}{!}{%" _n
file write tbl "\begin{tabular}{`colspec'}" _n
file write tbl "\hline\hline" _n

// Header row 1: Overall + split name spans
file write tbl " & Overall"
forvalues s = 1/`n_splits' {
    file write tbl " & \multicolumn{2}{c}{`splitnice`s''}"
}
file write tbl " \\" _n
file write tbl "`cmidrules'" _n

// Header row 2: group labels within each split
file write tbl " & "
forvalues s = 1/`n_splits' {
    file write tbl " & `label`s'_0' & `label`s'_1'"
}
file write tbl " \\" _n
file write tbl "\hline" _n

// Data rows
foreach outcome of local outcomes_tbl {
    local t = subinstr("`outcome'", "_dne", "", 1)
    local onice "`nice_`t''"

    quietly summarize `outcome' if (balanced_2_year_sample == 1 | never_tar == 1), meanonly
    local tot = r(mean)
    file write tbl "`onice' & " %5.3f (`tot')

    forvalues s = 1/`n_splits' {
        local sv "`binvar`s''"
        quietly summarize `outcome' if (balanced_2_year_sample == 1 | never_tar == 1) & `sv' == 0, meanonly
        local m0 = r(mean)
        quietly summarize `outcome' if (balanced_2_year_sample == 1 | never_tar == 1) & `sv' == 1, meanonly
        local m1 = r(mean)
        file write tbl " & " %5.3f (`m0') " & " %5.3f (`m1')
    }
    file write tbl " \\" _n
}

file write tbl "\hline\hline" _n
file write tbl "\end{tabular}}" _n
file write tbl "\end{table}" _n

file close tbl
display as result "DNE summary table saved."

