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

* Define outcome variables
local turnover_outcomes "turnover_ceo turnover_cfo turnover_coo turnover_cmo turnover_cno turnover_cco turnover_cio"
local vacancy_outcomes "vacant_ceo vacant_cfo vacant_coo vacant_cmo vacant_cno vacant_cco vacant_cio"
local all_outcomes "`turnover_outcomes' `vacancy_outcomes'"

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

local spec2_treated "balanced_3_year_sample == 1"
local spec2_control "never_tar == 1"
local spec2_cohort "never_tar"
local spec2_name "3yr Balanced, Never Treated"
local spec2_file "3yrbalanced_never_tar"

local spec3_treated "balanced_2_year_sample == 1"
local spec3_control "never_m_and_a == 1"
local spec3_cohort "never_m_and_a"
local spec3_name "2yr Balanced, Never M&A"
local spec3_file "2yrbalanced_never_m_and_a"

local spec4_treated "balanced_3_year_sample == 1"
local spec4_control "never_m_and_a == 1"
local spec4_cohort "never_m_and_a"
local spec4_name "3yr Balanced, Never M&A"
local spec4_file "3yrbalanced_never_m_and_a"

local nspecs = 4

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
        display "Specification `s': `spec`s'_name' - `outcome'"
        display "{hline 60}"
        
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
        
        graph export "${overleaf}/notes/Non CEO Event Study/figures/`spec`s'_file'_`outcome'.pdf", as(pdf) name("Graph") replace
    }
}
