/* EVENT_STUDY_STATS *************************************************************

Program name: 	event_study_stats.do
Programmer: 	Katherine Papen

Goal: 			Generate preliminary statistics to inform event study set up

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
* Create indicator for whether the CEO was the same 2 & three years before
*----------------------------------------------------------
frame create turnover_indicators
frame change turnover_indicators

use "${dbdata}/derived/individuals_final.dta", clear
keep if all_leader_flag & confirmed & entity_aha != .
keep entity_aha year contact_uniqueid firstname lastname title title_standardized

* verify that there aren't multiples (with one exception)
bys entity_aha year: egen n_unique_contacts = nvals(contact_uniqueid)
gen multi_contact = n_unique_contacts > 1
count if multi_contact == 1
assert r(N) == 2
drop if multi_contact == 1

* get 2 year vars
preserve
    keep entity_aha contact_uniqueid year
    duplicates drop
	rename year old_year
    gen year = old_year + 2
    tempfile prior2
    save `prior2'
restore

merge m:1 entity_aha contact_uniqueid year using `prior2', gen(_m2y)
gen byte same_ceo_2_years = (_m2y == 3)

* merge in to distinguish between no obs in sample and obs is different
preserve
    keep entity_aha year
    duplicates drop
    rename year old_year
    gen year = old_year + 2
    keep entity_aha year
    tempfile prior_any2
    save `prior_any2'
restore

merge m:1 entity_aha year using `prior_any2', gen(_m2y_any)
gen byte had_any_obs_2y_ago = (_m2y_any == 3)

replace same_ceo_2_years = 0 if same_ceo_2_years == 0 & had_any_obs_2y_ago == 1
replace same_ceo_2_years = . if had_any_obs_2y_ago == 0

drop if _m2y == 2 | contact_uniqueid == .
drop _m2y _m2y_any

* get 3 year vars
preserve
    keep entity_aha contact_uniqueid year
    duplicates drop
	rename year old_year
    gen year = old_year + 3
    tempfile prior3
    save `prior3'
restore

merge m:1 entity_aha contact_uniqueid year using `prior3', gen(_m3y)
gen byte same_ceo_3_years = (_m3y == 3)

preserve
    keep entity_aha year
    duplicates drop
    rename year old_year
    gen year = old_year + 3
    keep entity_aha year
    tempfile prior_any3
    save `prior_any3'
restore

merge m:1 entity_aha year using `prior_any3', gen(_m3y_any)
gen byte had_any_obs_3y_ago = (_m3y_any == 3)

replace same_ceo_3_years = 0 if same_ceo_3_years == 0 & had_any_obs_3y_ago == 1
replace same_ceo_3_years = . if had_any_obs_3y_ago == 0

drop if _m3y == 2 | contact_uniqueid == .
drop _m3y _m3y_any

rename entity_aha aha_id
keep aha_id year same_ceo_2_years same_ceo_3_years

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

file open out using "${overleaf}/notes/Event Study Setup/tables/cumulative_sample_counts.tex", write replace
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
gen ceo_turnover_past_2_years = 1 - same_ceo_2_years
gen ceo_turnover_past_3_years = 1 - same_ceo_3_years

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

file open out using "${overleaf}/notes/Event Study Setup/tables/cumulative_means.tex", write replace
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

  graph export "${overleaf}/notes/Event Study Setup/figures/two_year_turnover_cohort.pdf", as(pdf) name("Graph") replace

restore

*----------------------------------------------------------
* Create Sun & Abraham Plots
*----------------------------------------------------------
preserve

sum tar_reltime, meanonly
local rmin = r(min)
local rmax = r(max)

* Post periods (lags): t = 0..Lpost
forvalues h = 0/`rmax' {
    gen byte ev_lag`h' = (tar_reltime == `h')
}

* Pre periods (leads): t = -1..-Lpre
forvalues h = 1/`=abs(`rmin')' {
    gen byte ev_lead`h' = (tar_reltime == -`h')
}

replace ev_lead1 = 0

*** Never Treated Control ***
// full sample
eventstudyinteract ceo_turnover_past_2_years ev_lead* ev_lag* if (full_treated_sample == 1|never_tar == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 2 Year Turnover - Never Treated")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/full_tar_vs_2yr_never.pdf", as(pdf) name("Graph") replace

eventstudyinteract ceo_turnover_past_3_years ev_lead* ev_lag* if (full_treated_sample == 1|never_tar == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 3 Year Turnover - Never Treated")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/full_tar_vs_3yr_never.pdf", as(pdf) name("Graph") replace

// restricted sample
eventstudyinteract ceo_turnover_past_2_years ev_lead* ev_lag* if (restricted_treated_sample == 1|never_tar == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 2 Year Turnover - Never Treated")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/restricted_tar_vs_2yr_never.pdf", as(pdf) name("Graph") replace

eventstudyinteract ceo_turnover_past_3_years ev_lead* ev_lag* if (restricted_treated_sample == 1|never_tar == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 3 Year Turnover - Never Treated")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/restricted_tar_vs_3yr_never.pdf", as(pdf) name("Graph") replace

**** Never M & A Control ****
eventstudyinteract ceo_turnover_past_2_years ev_lead* ev_lag* if (full_treated_sample == 1|never_m_and_a == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 2 Year Turnover - Never M&A")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/full_tar_vs_2yr_never_m_and_a.pdf", as(pdf) name("Graph") replace

eventstudyinteract ceo_turnover_past_3_years ev_lead* ev_lag* if (full_treated_sample == 1|never_m_and_a == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 3 Year Turnover - Never M&A")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/full_tar_vs_3yr_never_m_and_a.pdf", as(pdf) name("Graph") replace

// restricted sample
eventstudyinteract ceo_turnover_past_2_years ev_lead* ev_lag* if (restricted_treated_sample == 1|never_m_and_a == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 2 Year Turnover - Never M&A")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/restricted_tar_vs_2yr_never_m_and_a.pdf", as(pdf) name("Graph") replace

eventstudyinteract ceo_turnover_past_3_years ev_lead* ev_lag* if (restricted_treated_sample == 1|never_m_and_a == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_m_and_a)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 3 Year Turnover - Never M&A")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/restricted_tar_vs_3yr_never_m_and_a.pdf", as(pdf) name("Graph") replace

*** Last Treated Control ***
gen last_treated = tar_event_year == 2017
drop if year > 2016
// full sample
eventstudyinteract ceo_turnover_past_2_years ev_lead* ev_lag* if (full_treated_sample == 1|never_tar == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 2 Year Turnover - Last Treated")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/full_tar_vs_2yr_last.pdf", as(pdf) name("Graph") replace

eventstudyinteract ceo_turnover_past_3_years ev_lead* ev_lag* if (full_treated_sample == 1|never_tar == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 3 Year Turnover - Last Treated")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/full_tar_vs_3yr_last.pdf", as(pdf) name("Graph") replace

// restricted sample
eventstudyinteract ceo_turnover_past_2_years ev_lead* ev_lag* if (restricted_treated_sample == 1|never_tar == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 2 Year Turnover - Last Treated")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/restricted_tar_vs_2yr_last.pdf", as(pdf) name("Graph") replace

eventstudyinteract ceo_turnover_past_2_years ev_lead* ev_lag* if (restricted_treated_sample == 1|never_tar == 1), vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-4(1)4) ///
		title("Effect of Targeting on 3 Year Turnover - Last Treated")) stub_lag(ev_lag#) stub_lead(ev_lead#) trimlag(4) trimlead(4) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/restricted_tar_vs_3yr_last.pdf", as(pdf) name("Graph") replace

restore
