/* CUMULATIVE_TURNOVER *************************************************************

Program name: 	cumulative_turnover.do
Programmer: 	Katherine Papen

Goal: 			Generate event study plots where outcome variable is whether or
				not the CEO has changed in the previous 2 years or 3 years. 
				Note: adjusting for vacancies does not change estimates due to
				how we designate CEOs (more broadly than title_standardized). 
				Therefore vacancies are not included. 

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

merge 1:1 entity_uniqueid year using `himss_type_xwalk', nogen

* restrict to hospital sample
restrict_hosp_sample

*----------------------------------------------------------
* Create indicator for whether the CEO changed at all in the previous 2-3 years
*----------------------------------------------------------
frame create turnover_indicators
frame change turnover_indicators

use "${dbdata}/derived/hospitals_final.dta", clear
keep entity_uniqueid year all_ceo entity_aha
drop if all_ceo != "Vacant"
gen vacancies = 1
tempfile vacancies
save `vacancies'

use "${dbdata}/derived/individuals_final.dta", clear
merge m:1 entity_uniqueid year using `vacancies'
keep if (all_leader_flag & confirmed & contact_uniqueid != .) | (vacancies == 1)
drop if entity_aha == .
keep entity_aha year contact_uniqueid firstname lastname title title_standardized vacancies ///
all_ceo aha_leader_flag all_leader_flag ceo_himss_title_exact ceo_himss_title_fuzzy
keep if title_standardized == "CEO:  Chief Executive Officer" | ceo_himss_title_exact | ceo_himss_title_fuzzy | vacancies == 1

* keep only one obs if vacant CEO position
bys entity_aha year: egen all_vacant = min(vacancies)
bys entity_aha year: gen obs_num = _n
drop if all_vacant == 1 & obs_num > 1

* verify that there aren't multiples (with one exception)
bys entity_aha year: egen n_unique_contacts = nvals(contact_uniqueid)
gen multi_contact = n_unique_contacts > 1
count if multi_contact == 1
assert r(N) == 133
drop if entity_aha == 6930101 & year == 2010
bysort entity_aha year: keep if _n == 1

sort entity_aha year
by entity_aha: gen contact_lag1 = contact_uniqueid[_n-1]
by entity_aha: gen contact_lag2 = contact_uniqueid[_n-2]
by entity_aha: gen contact_lag3 = contact_uniqueid[_n-3]
by entity_aha: gen vacancies_lag1 = vacancies[_n-1]
by entity_aha: gen vacancies_lag2 = vacancies[_n-2]
by entity_aha: gen vacancies_lag3 = vacancies[_n-3]
by entity_aha: gen year_lag1 = year[_n-1]
by entity_aha: gen year_lag2 = year[_n-2]
by entity_aha: gen year_lag3 = year[_n-3]

drop if contact_uniqueid == .

keep if contact_lag1 != . & contact_lag2 != .

gen contact_changed_prev2yrs = ///
    ((contact_lag1 != contact_uniqueid & !missing(contact_lag1) & year == year_lag1 + 1) | ///
     (contact_lag2 != contact_uniqueid & !missing(contact_lag2) & year == year_lag2 + 2))

gen contact_changed_prev3yrs = ///
    ((contact_changed_prev2yrs & !missing(year_lag3))| ///
	(contact_lag3 != contact_uniqueid & !missing(contact_lag3) & year == year_lag3 + 3))

rename entity_aha aha_id
keep aha_id year contact_changed_prev2yrs contact_changed_prev3yrs contact_lag3

bys aha_id year: keep if _n == 1

tempfile prev_ceos
save `prev_ceos'

frame change default

destring aha_id, replace
merge m:1 aha_id year using `prev_ceos', gen(_merge_turnover)
keep if _merge_turnover == 3

*----------------------------------------------------------
* Create target indicators
*----------------------------------------------------------
make_target_sample

*----------------------------------------------------------
* Create summary statistics (counts)
*----------------------------------------------------------
quietly distinct entity_uniqueid if full_treated_sample
local full_tar_final = r(ndistinct)

quietly distinct entity_uniqueid if restricted_treated_sample
local restricted_tar_final = r(ndistinct)

quietly distinct entity_uniqueid if never_tar
local count_never_tar = r(ndistinct)

quietly distinct entity_uniqueid if never_m_and_a
local clean_control = r(ndistinct)

file open out using "${overleaf}/notes/Event Study Setup/tables/any_sample_counts.tex", write replace
file write out "\begin{itemize}" _n
file write out "\item There are `full_tar_final' distinct entities that are ever targeted for an acquisition and have valid observations for turnover in the previous 2 years." _n
file write out "\item There are `restricted_tar_final' distinct entities that are ever targeted for an acquisition, have valid observations for turnover in the previous 2 years, and have the requisite pre-periods and post-periods." _n
file write out "\item There are `count_never_tar' distinct entities that are never targeted for an acquisition and have valid observations for turnover in the previous 2 years." _n
file write out "\item There are `clean_control' distinct entities that are never targeted for an acquisition or are part of a system acquiring another hospital and have valid observations for turnover in the previous 2 years." _n
file write out "\end{itemize}" _n
file close out

*----------------------------------------------------------
* Create summary statistics (means)
*----------------------------------------------------------
rename contact_changed_prev2yrs ceo_turnover_past_2_years
rename contact_changed_prev3yrs ceo_turnover_past_3_years

capture program drop summarize_turnover
program define summarize_turnover
    syntax , cond(str) prefix(str)

    quietly summ ceo_turnover_past_2_years if `cond'
    local mean = r(mean)
    local se   = r(sd)/sqrt(r(N))
    local N    = r(N)

    quietly distinct entity_uniqueid if `cond'
    local nent = r(ndistinct)

    c_local `prefix'_mean = `mean'
    c_local `prefix'_se   = `se'
    c_local `prefix'_N    = `N'
    c_local `prefix'_nent = `nent'
end

summarize_turnover , cond("never_m_and_a")          prefix(never_m_and_a)
summarize_turnover , cond("never_tar")              prefix(never_tar)
summarize_turnover , cond("full_tar_sample") 		prefix(full_tar)
summarize_turnover , cond("restricted_tar_sample") 	prefix(restricted_tar)
summarize_turnover , cond("full_tar_sample & tar_reltime < 0") 			prefix(pre_full_tar)
summarize_turnover , cond("restricted_tar_sample & tar_reltime < 0") 	prefix(pre_restricted_tar)

foreach v in never_m_and_a never_tar full_tar restricted_tar pre_full_tar pre_restricted_tar{
    local val_mean = ``v'_mean'
    local val_se   = ``v'_se'
    local val_N    = ``v'_N'
    local val_nent = ``v'_nent'

    local `v'_mean_str : display %9.3f `val_mean'
    local `v'_se_str   : display %9.3f `val_se'
    local `v'_N_str    : display %9.0f `val_N'
    local `v'_nent_str : display %9.0f `val_nent'
}

file open out using "${overleaf}/notes/Event Study Setup/tables/any_means.tex", write replace
file write out ///
"\begin{table}[!ht]\n " ///
"\centering\n" ///
"\caption{CEO turnover means, SEs, and counts}\n " ///
"\label{tab:two_year_turnover_means}\n " ///
"\begin{tabular}{lcccc}\n " ///
"\toprule\n " ///
"Group & Mean & SE & N & Distinct Entities\\\\\n" ///
"\midrule\n " ///
"Never treated (No M \& A ) & `never_m_and_a_mean_str' & `never_m_and_a_se_str' & `never_m_and_a_N_str' & `never_m_and_a_nent_str'\\\\\n " ///
"Never treated (tar) & `never_tar_mean_str' & `never_tar_se_str' & `never_tar_N_str' & `never_tar_nent_str'\\\\\n " ///
"Full treated sample (tar) & `full_tar_mean_str' & `full_tar_se_str' & `full_tar_N_str' & `full_tar_nent_str'\\\\\n " ///
"Restricted treated sample (tar) & `restricted_tar_mean_str' & `restricted_tar_se_str' & `restricted_tar_N_str' & `restricted_tar_nent_str'\\\\\n " ///
"Full treated sample : pre-period only (tar) & `pre_full_tar_mean_str' & `pre_full_tar_se_str' & `pre_full_tar_N_str' & `pre_full_tar_nent_str'\\\\\n " ///
"Restricted treated sample : pre-period only (tar)& `pre_restricted_tar_mean_str' & `pre_restricted_tar_se_str' & `pre_restricted_tar_N_str' & `restricted_tar_nent_str'\\\\\n " ///
"\bottomrule\n" ///
"\end{tabular}\n" ///
"\end{table}\n"
file close out

*----------------------------------------------------------
* Create Plot by Cohort
*----------------------------------------------------------

* CEO Turnover by Target Treatment Status
preserve

keep if restricted_tar_sample == 1

collapse (mean) ceo_turnover_past_2_years, by(year tar_event_year)

twoway ///
(line ceo_turnover_past_2_years year if tar_event_year==2011, lcolor(red) lpattern(solid)) ///
(line ceo_turnover_past_2_years year if tar_event_year==2012, lcolor(orange) lpattern(solid)) ///
(line ceo_turnover_past_2_years year if tar_event_year==2013, lcolor(green) lpattern(solid)) ///
(line ceo_turnover_past_2_years year if tar_event_year==2014, lcolor(blue) lpattern(solid)) ///
(line ceo_turnover_past_2_years year if tar_event_year==2015, lcolor(purple) lpattern(solid)), ///
legend(order(1 "2011" 2 "2012" 3 "2013" 4 "2014" 5 "2015" ) ///
       pos(11) ring(0) col(1) size(small)) ///
ytitle("Mean CEO Turnover") ///
xtitle("Calendar Year") ///
title("CEO Turnover by Year and Target Event Year (2011–2015)") ///
scheme(s1color)

  graph export "${overleaf}/notes/Event Study Setup/figures/any_two_year_turnover_cohort.pdf", as(pdf) name("Graph") replace

restore

*----------------------------------------------------------
* Create Sun & Abraham Plots
*----------------------------------------------------------

preserve

sum tar_reltime, meanonly
local rmin = r(min)
local rmax = r(max)

* Create Relative Time Indicators
* Post periods (lags): t = 0..Lpost
forvalues h = 0/`rmax' {
    gen byte ev_lag`h' = (tar_reltime == `h')
}

* Pre periods (leads): t = -1..-Lpre
forvalues h = 1/`=abs(`rmin')' {
    gen byte ev_lead`h' = (tar_reltime == -`h')
}

replace ev_lead1 = 0

* Specify conditions
local spec1_treated "full_treated_sample == 1"
local spec1_control "never_tar == 1"
local spec1_cohort "never_tar"
local spec1_name "Full Sample, Never Treated"
local spec1_file "full_tar_vs_2yr_never"

local spec2_treated "full_treated_sample == 1 & contact_lag3 != ."
local spec2_control "never_tar == 1 & contact_lag3 != ."
local spec2_cohort "never_tar"
local spec2_name "Full Sample, Never Treated"
local spec2_file "full_tar_vs_3yr_never"

local spec3_treated "restricted_treated_sample == 1"
local spec3_control "never_tar == 1"
local spec3_cohort "never_tar"
local spec3_name "Restricted Sample, Never Treated"
local spec3_file "restricted_tar_vs_2yr_never"

local spec4_treated "restricted_treated_sample == 1 & contact_lag3 != ."
local spec4_control "never_tar == 1 & contact_lag3 != ."
local spec4_cohort "never_tar"
local spec4_name "Restricted Sample, Never Treated"
local spec4_file "restricted_tar_vs_3yr_never"

local spec5_treated "full_treated_sample == 1"
local spec5_control "never_m_and_a == 1"
local spec5_cohort "never_m_and_a"
local spec5_name "Full Sample, Never M&A"
local spec5_file "full_tar_vs_2yr_never_m_and_a"

local spec6_treated "full_treated_sample == 1 & contact_lag3 != ."
local spec6_control "never_m_and_a == 1 & contact_lag3 != ."
local spec6_cohort "never_m_and_a"
local spec6_name "Full Sample, Never M&A"
local spec6_file "full_tar_vs_3yr_never_m_and_a"

local spec7_treated "restricted_treated_sample == 1"
local spec7_control "never_m_and_a == 1"
local spec7_cohort "never_m_and_a"
local spec7_name "Restricted Sample, Never M&A"
local spec7_file "restricted_tar_vs_2yr_never_m_and_a"

local spec8_treated "restricted_treated_sample == 1 & contact_lag3 != ."
local spec8_control "never_m_and_a == 1 & contact_lag3 != ."
local spec8_cohort "never_m_and_a"
local spec8_name "Restricted Sample, Never M&A"
local spec8_file "restricted_tar_vs_3yr_never_m_and_a"

* Outcome variables for each spec
local spec1_outcome "ceo_turnover_past_2_years"
local spec2_outcome "ceo_turnover_past_3_years"
local spec3_outcome "ceo_turnover_past_2_years"
local spec4_outcome "ceo_turnover_past_3_years"
local spec5_outcome "ceo_turnover_past_2_years"
local spec6_outcome "ceo_turnover_past_3_years"
local spec7_outcome "ceo_turnover_past_2_years"
local spec8_outcome "ceo_turnover_past_3_years"

local nspecs = 8

**** Loop through specifications (Never Treated & Never M&A) ****
forvalues s = 1/`nspecs' {
    
    display _newline(2) "{hline 60}"
    display "Specification `s': `spec`s'_name' - `spec`s'_outcome'"
    display "{hline 60}"
    
    // First run: Calculate average effect
    eventstudyinteract `spec`s'_outcome' ev_lead* ev_lag* ///
        if (`spec`s'_treated'|`spec`s'_control'), ///
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
    eventstudyinteract `spec`s'_outcome' ev_lead* ev_lag* ///
        if (`spec`s'_treated'|`spec`s'_control'), ///
        vce(cluster entity_uniqueid) ///
        absorb(entity_uniqueid year) ///
        cohort(tar_event_year) ///
        control_cohort(`spec`s'_cohort')
    
    event_plot e(b_iw)#e(V_iw), ///
        default_look ///
        graph_opt(xtitle("Periods since the event") ///
                  ytitle("Average effect") ///
                  xlabel(-4(1)4) ///
                  title("Effect of Targeting on `spec`s'_outcome' - `spec`s'_name'" ///
                        "Avg Effect: `avg_effect' (SE: `avg_se')", size(medium))) ///
        stub_lag(ev_lag#) ///
        stub_lead(ev_lead#) ///
        trimlag(4) ///
        trimlead(4) ///
        plottype(scatter) ///
        ciplottype(rcap)
    graph export "${overleaf}/notes/Event Study Setup/figures/any_`spec`s'_file'.pdf", as(pdf) name("Graph") replace
}

**** Last Treated Control ****

* Create last-treated indicator
gen last_treated = tar_event_year == 2017

* Drop observations after 2016 for last treated specs
drop if year > 2016

* Define last treated specifications
local spec9_treated "full_treated_sample == 1"
local spec9_control "last_treated == 1"
local spec9_cohort "last_treated"
local spec9_name "Full Sample, Last Treated"
local spec9_outcome "ceo_turnover_past_2_years"
local spec9_file "full_tar_vs_2yr_last"

local spec10_treated "full_treated_sample == 1"
local spec10_control "last_treated == 1"
local spec10_cohort "last_treated"
local spec10_name "Full Sample, Last Treated"
local spec10_outcome "ceo_turnover_past_3_years"
local spec10_file "full_tar_vs_3yr_last"

local spec11_treated "restricted_treated_sample == 1"
local spec11_control "last_treated == 1"
local spec11_cohort "last_treated"
local spec11_name "Restricted Sample, Last Treated"
local spec11_outcome "ceo_turnover_past_2_years"
local spec11_file "restricted_tar_vs_2yr_last"

local spec12_treated "restricted_treated_sample == 1"
local spec12_control "last_treated == 1"
local spec12_cohort "last_treated"
local spec12_name "Restricted Sample, Last Treated"
local spec12_outcome "ceo_turnover_past_3_years"
local spec12_file "restricted_tar_vs_3yr_last"

local nspecs_last = 4

forvalues s = 9/12 {
    
    display _newline(2) "{hline 60}"
    display "Specification `s': `spec`s'_name' - `spec`s'_outcome'"
    display "{hline 60}"
    
    // First run: Calculate average effect
    eventstudyinteract `spec`s'_outcome' ev_lead* ev_lag* ///
        if (`spec`s'_treated'|`spec`s'_control'), ///
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
    eventstudyinteract `spec`s'_outcome' ev_lead* ev_lag* ///
        if (`spec`s'_treated'|`spec`s'_control'), ///
        vce(cluster entity_uniqueid) ///
        absorb(entity_uniqueid year) ///
        cohort(tar_event_year) ///
        control_cohort(`spec`s'_cohort')
    
    event_plot e(b_iw)#e(V_iw), ///
        default_look ///
        graph_opt(xtitle("Periods since the event") ///
                  ytitle("Average effect") ///
                  xlabel(-4(1)4) ///
                  title("Effect of Targeting on `spec`s'_outcome' - `spec`s'_name'" ///
                        "Avg Effect: `avg_effect' (SE: `avg_se')", size(medium))) ///
        stub_lag(ev_lag#) ///
        stub_lead(ev_lead#) ///
        trimlag(4) ///
        trimlead(4) ///
        plottype(scatter) ///
        ciplottype(rcap)
    graph export "${overleaf}/notes/Event Study Setup/figures/any_`spec`s'_file'.pdf", as(pdf) name("Graph") replace
}

restore // Restore to main data
