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
merge 1:1 entity_uniqueid year using "${dbdata}/derived/positions_by_tier.dta", keep(match) nogen

make_target_sample

* Define outcome variables
local all_outcomes "business1_sh_active business2_sh_active clinical1_sh_active clinical2_sh_active itlegalhr1_sh_active itlegalhr2_sh_active business1_sh_vacant business2_sh_vacant clinical1_sh_vacant clinical2_sh_vacant itlegalhr1_sh_vacant itlegalhr2_sh_vacant business1_sh_dne business2_sh_dne clinical1_sh_dne clinical2_sh_dne itlegalhr1_sh_dne itlegalhr2_sh_dne business1_people_per_role business2_people_per_role clinical1_people_per_role clinical2_people_per_role itlegalhr1_people_per_role itlegalhr2_people_per_role"

*----------------------------------------------------------
* Get summary stats
*----------------------------------------------------------
* Create a temporary file to store results
tempname memhold
tempfile results
postfile `memhold' str30 outcome double mean double sd double n using `results'

* Loop through outcomes and calculate pre-treatment means
foreach outcome of local all_outcomes {
    quietly count if `outcome' != . & tar_reltime < 0 & balanced_2_year_sample == 1
    local obs = r(N)
    
    if `obs' > 0 {
        quietly summarize `outcome' if tar_reltime < 0 & balanced_2_year_sample == 1
        local m = r(mean)
        local s = r(sd)
        local n = r(N)
    }
    else {
        local m = .
        local s = .
        local n = 0
    }
    
    post `memhold' ("`outcome'") (`m') (`s') (`n')
}

postclose `memhold'

* Load results and format
preserve
use `results', clear

* Round for display
gen mean_fmt = string(mean, "%9.3f")
gen sd_fmt = string(sd, "%9.3f")
gen n_fmt = string(n, "%9.0f")

* Export to LaTeX
listtab outcome mean_fmt sd_fmt n_fmt using "${overleaf}/notes/Non CEO Event Study/tables/pre_treatment_means.tex", ///
    rstyle(tabular) ///
    head("\begin{table}[H]" ///
         "\centering" ///
         "\caption{Pre-Treatment Means (2-Year Balanced Sample)}" ///
         "\label{tab:pre_treatment_means}" ///
         "\begin{tabular}{lccc}" ///
         "\hline\hline" ///
         "Outcome & Mean & SD & N \\\\") ///
    foot("\hline\hline" ///
         "\end{tabular}" ///
         "\end{table}") ///
    replace

restore

*----------------------------------------------------------
* Define samples + run event studies
*----------------------------------------------------------

* Define specifications (2 samples × 2 controls = 4 specs)
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

**** Loop through specifications and outcomes ****
forvalues s = 1/`nspecs' {
    
    foreach outcome of local all_outcomes {
        
        display _newline(2) "{hline 60}"
        
        // First run: Calculate average effect
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
        
        // Store the result rounded to 3 decimal places
        local avg_effect = round(r(estimate), 0.001)
        local avg_se = round(r(se), 0.001)
        
        // Second run: Event plot with average effect in title
        eventstudyinteract `outcome' ev_lead* ev_lag* ///
            if (`spec`s'_treated' | `spec`s'_control'), ///
            vce(cluster entity_uniqueid) ///
            absorb(entity_uniqueid year) ///
            cohort(tar_event_year) ///
            control_cohort(`spec`s'_cohort')
        
        event_plot e(b_iw)#e(V_iw), ///
            default_look ///
            graph_opt(xtitle("Periods since the event") ///
                      ytitle("Average effect") ///
                      xlabel(-3(1)3) ///
                      title("Effect on `outcome'" ///
                            "`spec`s'_name' | Avg: `avg_effect' (SE: `avg_se')", size(medium))) ///
            stub_lag(ev_lag#) ///
            stub_lead(ev_lead#) ///
            trimlag(3) ///
            trimlead(3) ///
            plottype(scatter) ///
            ciplottype(rcap)
        
        graph export "${overleaf}/notes/Non CEO Event Study/figures/`outcome'.pdf", as(pdf) name("Graph") replace
    }
}

*----------------------------------------------------------
* Forest Plots: Average Treatment Effect by Tier
*----------------------------------------------------------

* Define the outcome suffixes and tier prefixes
local suffixes "sh_active sh_vacant sh_dne"
local tiers "business1 business2 clinical1 clinical2 itlegalhr1 itlegalhr2"
local ntiers : word count `tiers'

* Nice labels for tiers
local lbl_business1    "Business Tier 1"
local lbl_business2    "Business Tier 2"
local lbl_clinical1    "Clinical Tier 1"
local lbl_clinical2    "Clinical Tier 2"
local lbl_itlegalhr1   "IT/Legal/HR Tier 1"
local lbl_itlegalhr2   "IT/Legal/HR Tier 2"

* Nice labels for suffixes (for titles)
local title_sh_active "Share Active"
local title_sh_vacant "Share Vacant"
local title_sh_dne    "Share DNE"

forvalues s = 1/`nspecs' {

    foreach suf of local suffixes {
        
        * ------ Collect point estimates and SEs for each tier ------
        local row = 1
        
        foreach tier of local tiers {
            
            local outcome "`tier'_`suf'"
            
            display _newline(2) "{hline 60}"
            display "Forest plot component: `outcome' | Spec `s'"
            
            eventstudyinteract `outcome' ev_lead* ev_lag* ///
                if (`spec`s'_treated' | `spec`s'_control'), ///
                vce(cluster entity_uniqueid) ///
                absorb(entity_uniqueid year) ///
                cohort(tar_event_year) ///
                control_cohort(`spec`s'_cohort')
            
            matrix b = e(b_iw)
            matrix V = e(V_iw)
            ereturn post b V
            
            lincom (ev_lag0 + ev_lag1 + ev_lag2)/3
            
            local est_`row' = r(estimate)
            local se_`row'  = r(se)
            local lb_`row'  = r(estimate) - 1.96 * r(se)
            local ub_`row'  = r(estimate) + 1.96 * r(se)
            local lab_`row' "`lbl_`tier''"
            
            local row = `row' + 1
        }
        
        * ------ Build a small dataset for the forest plot ------
        preserve
        clear
        set obs `ntiers'
        
        gen tier_id   = _n
        gen estimate  = .
        gen se        = .
        gen ci_lo     = .
        gen ci_hi     = .
        gen str30 tier_label = ""
        
        forvalues i = 1/`ntiers' {
            replace estimate   = `est_`i''  in `i'
            replace se         = `se_`i''   in `i'
            replace ci_lo      = `lb_`i''   in `i'
            replace ci_hi      = `ub_`i''   in `i'
            replace tier_label = "`lab_`i''" in `i'
        }
        
        * Reverse tier_id so first tier appears at top
        gen plot_pos = `ntiers' + 1 - tier_id
        
        * Create labeled axis
        labmask plot_pos, values(tier_label)
        
        * ------ Draw the forest plot ------
        twoway ///
            (rcap ci_lo ci_hi plot_pos, horizontal lcolor(navy) lwidth(medium)) ///
            (scatter plot_pos estimate, msymbol(diamond) mcolor(navy) msize(medlarge)) ///
            , ///
            xline(0, lcolor(cranberry) lpattern(dash)) ///
            ylabel(1/`ntiers', valuelabel angle(0) labsize(small) nogrid) ///
            ytitle("") ///
            xtitle("Average Treatment Effect (Periods 0–2)") ///
            title("Forest Plot: `title_`suf''" ///
                  "`spec`s'_name'", size(medium)) ///
            legend(off) ///
            graphregion(color(white)) ///
            plotregion(margin(l=2))
        
        graph export "${overleaf}/notes/Non CEO Event Study/figures/forest_`suf'.pdf", as(pdf) name("Graph") replace
        
        restore
    }
}

*----------------------------------------------------------
* Weighted Forest Plots: Average Treatment Effect by Tier
*----------------------------------------------------------
* Define the outcome suffixes and tier prefixes
local suffixes "sh_active sh_vacant sh_dne"
local tiers "business1 business2 clinical1 clinical2 itlegalhr1 itlegalhr2"
local ntiers : word count `tiers'

* Nice labels for tiers
local lbl_business1    "Business Tier 1"
local lbl_business2    "Business Tier 2"
local lbl_clinical1    "Clinical Tier 1"
local lbl_clinical2    "Clinical Tier 2"
local lbl_itlegalhr1   "IT/Legal/HR Tier 1"
local lbl_itlegalhr2   "IT/Legal/HR Tier 2"

* Nice labels for suffixes (for titles)
local title_sh_active "Share Active"
local title_sh_vacant "Share Vacant"
local title_sh_dne    "Share DNE"

forvalues s = 1/`nspecs' {
    foreach suf of local suffixes {
        
        * ------ Collect point estimates and SEs for each tier ------
        local row = 1
        
        foreach tier of local tiers {
            
            local outcome "`tier'_`suf'"
            
            display _newline(2) "{hline 60}"
            display "Forest plot component: `outcome' | Spec `s'"
            
            * Compute pretreatment mean for normalization
            summ `outcome' if tar_reltime < 0 ///
                & (`spec`s'_treated' | `spec`s'_control'), meanonly
            local premean_`row' = r(mean)
            
            eventstudyinteract `outcome' ev_lead* ev_lag* ///
                if (`spec`s'_treated' | `spec`s'_control'), ///
                vce(cluster entity_uniqueid) ///
                absorb(entity_uniqueid year) ///
                cohort(tar_event_year) ///
                control_cohort(`spec`s'_cohort')
            
            matrix b = e(b_iw)
            matrix V = e(V_iw)
            ereturn post b V
            
            lincom (ev_lag0 + ev_lag1 + ev_lag2)/3
            
            local est_`row' = r(estimate) / `premean_`row''
            local se_`row'  = r(se) / abs(`premean_`row'')
            local lb_`row'  = `est_`row'' - 1.96 * `se_`row''
            local ub_`row'  = `est_`row'' + 1.96 * `se_`row''
            local lab`row' "`lbl_`tier''"
            
            local row = `row' + 1
        }
        
        * ------ Build a small dataset for the forest plot ------
        preserve
        clear
        set obs `ntiers'
        
        gen tier_id   = _n
        gen estimate  = .
        gen se        = .
        gen ci_lo     = .
        gen ci_hi     = .
        gen str30 tier_label = ""
        
        forvalues i = 1/`ntiers' {
            replace estimate   = `est_`i''  in `i'
            replace se         = `se_`i''   in `i'
            replace ci_lo      = `lb_`i''   in `i'
            replace ci_hi      = `ub_`i''   in `i'
            replace tier_label = "`lab`i''" in `i'
        }
        
        * Reverse tier_id so first tier appears at top
        gen plot_pos = `ntiers' + 1 - tier_id
        
        * Create labeled axis
        labmask plot_pos, values(tier_label)
        
        * ------ Draw the forest plot ------
        twoway ///
            (rcap ci_lo ci_hi plot_pos, horizontal lcolor(navy) lwidth(medium)) ///
            (scatter plot_pos estimate, msymbol(diamond) mcolor(navy) msize(medlarge)) ///
            , ///
            xline(0, lcolor(cranberry) lpattern(dash)) ///
            ylabel(1/`ntiers', valuelabel angle(0) labsize(small) nogrid) ///
            ytitle("") ///
            xtitle("Avg. Treatment Effect / Pretreatment Mean (Periods 0–2)") ///
            title("Forest Plot: `title_`suf''" ///
                  "`spec`s'_name'", size(medium)) ///
            legend(off) ///
            graphregion(color(white)) ///
            plotregion(margin(l=2))
        
        graph export "${overleaf}/notes/Non CEO Event Study/figures/wgt_forest_`suf'.pdf", as(pdf) name("Graph") replace
        
        restore
    }
}
