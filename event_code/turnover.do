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

* Define outcome variables
local turnover_outcomes "turnover_ceo turnover_cfo turnover_coo turnover_cmo turnover_cno turnover_cco turnover_cio"
local vacancy_outcomes "vacant_ceo vacant_cfo vacant_coo vacant_cmo vacant_cno vacant_cco vacant_cio"
local vacant_to_active_outcomes "vacant_to_active_ceo vacant_to_active_cfo vacant_to_active_coo vacant_to_active_cmo vacant_to_active_cno vacant_to_active_cco vacant_to_active_cio"
local vacancy_change_outcomes "vacancy_change_ceo vacancy_change_cfo vacancy_change_coo vacancy_change_cmo vacancy_change_cno vacancy_change_cco vacancy_change_cio"
local dne_change_outcomes "dne_change_ceo dne_change_cfo dne_change_coo dne_change_cmo dne_change_cno dne_change_cco dne_change_cio"
local dne_to_active_outcomes "dne_to_active_ceo dne_to_active_cfo dne_to_active_coo dne_to_active_cmo dne_to_active_cno dne_to_active_cco dne_to_active_cio"
local active_to_vacant_outcomes "active_to_vacant_ceo active_to_vacant_cfo active_to_vacant_coo active_to_vacant_cmo active_to_vacant_cno active_to_vacant_cco active_to_vacant_cio"
local active_to_dne_outcomes "active_to_dne_ceo active_to_dne_cfo active_to_dne_coo active_to_dne_cmo active_to_dne_cno active_to_dne_cco active_to_dne_cio"
local table_1_outcomes "`turnover_outcomes'"
local table_2_outcomes "`vacancy_outcomes' `vacant_to_active_outcomes' `vacancy_change_outcomes' `active_to_vacant_outcomes'"
local table_3_outcomes "`dne_change_outcomes' `dne_to_active_outcomes' `active_to_dne_outcomes'"
local all_outcomes "`turnover_outcomes' `vacancy_outcomes' `vacant_to_active_outcomes' `vacancy_change_outcomes' `dne_change_outcomes' `dne_to_active_outcomes' `active_to_vacant_outcomes' `active_to_dne_outcomes'"

local table_1_caption "Pre-Treatment Means: Turnover (2-Year Balanced Sample)"
local table_1_label   "pre_treatment_means_1"
local table_1_file    "pre_treatment_means_1"

local table_2_caption "Pre-Treatment Means: Vacancy (2-Year Balanced Sample)"
local table_2_label   "pre_treatment_means_2"
local table_2_file    "pre_treatment_means_2"

local table_3_caption "Pre-Treatment Means: DNE (2-Year Balanced Sample)"
local table_3_label   "pre_treatment_means_3"
local table_3_file    "pre_treatment_means_3"

*----------------------------------------------------------
* Get summary stats
*----------------------------------------------------------
forvalues t = 1/3 {

    tempname memhold
    tempfile results
    postfile `memhold' str30 outcome double mean double sd double n using `results'

    foreach outcome of local table_`t'_outcomes {
        preserve

        keep if !missing(`outcome')
        make_target_sample

        quietly count if tar_reltime < 0 & balanced_2_year_sample == 1
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

        restore
    }

    postclose `memhold'

    preserve
    use `results', clear

    gen outcome_label = ""
    foreach r in ceo cfo coo cmo cno cco cio {
        local R = upper("`r'")
        replace outcome_label = "Turnover: `R'"         if outcome == "turnover_`r'"
        replace outcome_label = "Vacancy: `R'"          if outcome == "vacant_`r'"
        replace outcome_label = "Vacant to Active: `R'" if outcome == "vacant_to_active_`r'"
        replace outcome_label = "Vacancy Change: `R'"   if outcome == "vacancy_change_`r'"
        replace outcome_label = "DNE Change: `R'"       if outcome == "dne_change_`r'"
        replace outcome_label = "DNE to Active: `R'"    if outcome == "dne_to_active_`r'"
        replace outcome_label = "Active to Vacant: `R'" if outcome == "active_to_vacant_`r'"
        replace outcome_label = "Active to DNE: `R'"    if outcome == "active_to_dne_`r'"
    }

    gen mean_fmt = string(mean, "%9.3f")
    gen sd_fmt = string(sd, "%9.3f")
    gen n_fmt = string(n, "%9.0f")

    listtab outcome_label mean_fmt sd_fmt n_fmt using "${overleaf}/notes/Non CEO Event Study/tables/`table_`t'_file'.tex", ///
        rstyle(tabular) ///
        head("\begin{table}[H]" ///
             "\centering" ///
             "\caption{`table_`t'_caption'}" ///
             "\label{tab:`table_`t'_label'}" ///
             "\begin{tabular}{lccc}" ///
             "\hline\hline" ///
             "Outcome & Mean & SD & N \\\\") ///
        foot("\hline\hline" ///
             "\end{tabular}" ///
             "\end{table}") ///
        replace

    restore
}

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

local nspecs = 1

* Outcome title labels
local lbl_turnover_ceo "CEO Turnover"
local lbl_turnover_cfo "CFO Turnover"
local lbl_turnover_coo "COO Turnover"
local lbl_turnover_cmo "CMO Turnover"
local lbl_turnover_cno "CNO Turnover"
local lbl_turnover_cco "CCO Turnover"
local lbl_turnover_cio "CIO Turnover"
local lbl_vacant_ceo   "CEO Vacancy"
local lbl_vacant_cfo   "CFO Vacancy"
local lbl_vacant_coo   "COO Vacancy"
local lbl_vacant_cmo   "CMO Vacancy"
local lbl_vacant_cno   "CNO Vacancy"
local lbl_vacant_cco   "CCO Vacancy"
local lbl_vacant_cio   "CIO Vacancy"
local lbl_vacant_to_active_ceo "CEO Vacant to Active"
local lbl_vacant_to_active_cfo "CFO Vacant to Active"
local lbl_vacant_to_active_coo "COO Vacant to Active"
local lbl_vacant_to_active_cmo "CMO Vacant to Active"
local lbl_vacant_to_active_cno "CNO Vacant to Active"
local lbl_vacant_to_active_cco "CCO Vacant to Active"
local lbl_vacant_to_active_cio "CIO Vacant to Active"
local lbl_vacancy_change_ceo "CEO Vacancy Change"
local lbl_vacancy_change_cfo "CFO Vacancy Change"
local lbl_vacancy_change_coo "COO Vacancy Change"
local lbl_vacancy_change_cmo "CMO Vacancy Change"
local lbl_vacancy_change_cno "CNO Vacancy Change"
local lbl_vacancy_change_cco "CCO Vacancy Change"
local lbl_vacancy_change_cio "CIO Vacancy Change"
local lbl_dne_change_ceo "CEO DNE Change"
local lbl_dne_change_cfo "CFO DNE Change"
local lbl_dne_change_coo "COO DNE Change"
local lbl_dne_change_cmo "CMO DNE Change"
local lbl_dne_change_cno "CNO DNE Change"
local lbl_dne_change_cco "CCO DNE Change"
local lbl_dne_change_cio "CIO DNE Change"
local lbl_dne_to_active_ceo "CEO DNE to Active"
local lbl_dne_to_active_cfo "CFO DNE to Active"
local lbl_dne_to_active_coo "COO DNE to Active"
local lbl_dne_to_active_cmo "CMO DNE to Active"
local lbl_dne_to_active_cno "CNO DNE to Active"
local lbl_dne_to_active_cco "CCO DNE to Active"
local lbl_dne_to_active_cio "CIO DNE to Active"
local lbl_active_to_vacant_ceo "CEO Active to Vacant"
local lbl_active_to_vacant_cfo "CFO Active to Vacant"
local lbl_active_to_vacant_coo "COO Active to Vacant"
local lbl_active_to_vacant_cmo "CMO Active to Vacant"
local lbl_active_to_vacant_cno "CNO Active to Vacant"
local lbl_active_to_vacant_cco "CCO Active to Vacant"
local lbl_active_to_vacant_cio "CIO Active to Vacant"
local lbl_active_to_dne_ceo "CEO Active to DNE"
local lbl_active_to_dne_cfo "CFO Active to DNE"
local lbl_active_to_dne_coo "COO Active to DNE"
local lbl_active_to_dne_cmo "CMO Active to DNE"
local lbl_active_to_dne_cno "CNO Active to DNE"
local lbl_active_to_dne_cco "CCO Active to DNE"
local lbl_active_to_dne_cio "CIO Active to DNE"

**** Loop through specifications and outcomes ****
forvalues s = 1/`nspecs' {

    foreach outcome of local all_outcomes {

        display _newline(2) "{hline 60}"
        display "Specification `s': `spec`s'_name' - `outcome'"
        display "{hline 60}"
		
		preserve 
		
		keep if !missing(`outcome')
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
			
		if `s' == 1 | `s' == 3 {
			local x_width = 2
		}
		else if `s' == 2 | `s' == 4 {
			local x_width = 3
		}

        event_plot e(b_iw)#e(V_iw), ///
            default_look ///
            graph_opt(xtitle("Periods since the event") ///
                      ytitle("Average effect") ///
                      xlabel(-`x_width'(1)`x_width') ///
                      title("Effect on `lbl_`outcome''" ///
                            "`spec`s'_name' | Avg: `avg_effect' (SE: `avg_se')", size(medium))) ///
            stub_lag(ev_lag#) ///
            stub_lead(ev_lead#) ///
            trimlag(`x_width') ///
            trimlead(`x_width') ///
            plottype(scatter) ///
            ciplottype(rcap)

        graph export "${overleaf}/notes/Non CEO Event Study/figures/`spec`s'_file'_`outcome'.pdf", as(pdf) name("Graph") replace
		restore 
    }
}
