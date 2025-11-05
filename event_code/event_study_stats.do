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
* Get basic summary statistics
*----------------------------------------------------------
quietly distinct entity_uniqueid if tar == 1
local n_tar1 = r(ndistinct)

quietly distinct entity_uniqueid if acq == 1
local n_cond2 = r(ndistinct)

quietly distinct entity_uniqueid if tar == 1 | acq == 1
local n_cond3 = r(ndistinct)

* save sum stats
file open out using "${overleaf}/notes/Event Study Setup/tables/prelim_sample_counts.tex", write replace
file write out "\begin{itemize}" _n
file write out "\item There are `n_tar1' distinct entities that are ever targeted for an acquisition (tar == 1)." _n
file write out "\item There are `n_cond2' distinct entities that ever conduct an acquisition (acq == 1)." _n
file write out "\item There are `n_cond3' distinct entities that ever are part of an acquisition (either as a target or acquirer)." _n
file close out

*----------------------------------------------------------
* Create sample flags
*----------------------------------------------------------
sort entity_uniqueid year

* get target vars
bys entity_uniqueid (year): egen tar_event_year = min(cond(tar == 1, year, .))
gen tar_reltime = year - tar_event_year
gen tar_treated = year >= tar_event_year

* get acq vars
bys entity_uniqueid (year): egen acq_event_year = min(cond(acq == 1, year, .))
gen acq_reltime = year - acq_event_year
gen acq_treated = year >= acq_event_year

* get never treated flags
bys entity_uniqueid: egen ever_tar_1 = max(tar == 1)
gen never_tar = (ever_tar_1 == 0)

bys entity_uniqueid: egen ever_acq_1 = max(acq == 1)
gen never_acq = (ever_acq_1 == 0)

gen never_m_and_a = never_tar & never_acq

* get flags for sample
gen full_tar_sample_temp = ever_tar_1
gen restricted_tar_sample_temp = ever_tar_1 & (tar_event_year >= 2011 & tar_event_year <= 2015)
gen restricted_tar_sample_temp_2 = ever_tar_1 & (tar_event_year >= 2012 & tar_event_year <= 2014)

gen full_acq_sample = ever_acq_1
gen restricted_acq_sample = ever_acq_1 & (acq_event_year >= 2011 & acq_event_year <= 2015)

* create flags for pre/post sample
bys entity_uniqueid: egen ever_second_tar = max(tar == 1 & tar_reltime > 0)
bys entity_uniqueid: egen ever_second_acq = max(acq == 1 & acq_reltime > 0)

gen full_treated_sample = full_tar_sample_temp & !ever_second_tar
gen restricted_treated_sample = restricted_tar_sample_temp & !ever_second_tar
gen balanced_3_year_sample = restricted_tar_sample_temp_2 & !ever_second_tar

quietly distinct entity_uniqueid if never_m_and_a
local clean_control = r(ndistinct)

quietly distinct entity_uniqueid if full_tar_sample_temp
local tar_sample_n = r(ndistinct)

quietly distinct entity_uniqueid if full_acq_sample
local acq_sample_n = r(ndistinct)

quietly distinct entity_uniqueid if ever_second_tar
local second_tar = r(ndistinct)

quietly distinct entity_uniqueid if ever_second_acq
local second_acq = r(ndistinct)

quietly distinct entity_uniqueid if full_treated_sample
local full_tar_final = r(ndistinct)

quietly distinct entity_uniqueid if restricted_treated_sample
local restricted_tar_final = r(ndistinct)

quietly distinct entity_uniqueid if balanced_3_year_sample
local num_balanced_3_year_sample = r(ndistinct)

quietly distinct entity_uniqueid if restricted_acq_sample
local restricted_acq_final = r(ndistinct)

file open out using "${overleaf}/notes/Event Study Setup/tables/prelim_sample_counts.tex", write append
// file write out "\item There are `tar_sample_n' distinct entities that are ever acquired." _n
// file write out "\item There are `acq_sample_n' distinct entities that ever acquire another hospital." _n
file write out "\item There are `clean_control' distinct entities that never are acquired or are part of a system that acquires another hospital." _n

file write out "\item There are `second_tar' distinct entities that experience a second acquisition in the two years following the initial acquisition." _n
file write out "\item There are `second_acq' distinct entities that conduct another acquisition in the two years following their initial acquisition." _n

file write out "\item This results in `full_tar_final' distinct entities that are ever acquired once within our sample." _n
file write out "\item There are `restricted_tar_final' distinct entities that are ever acquired within our sample and have at least 2 years of pre-period observations and 2 years of post-period observations." _n
file write out "\item There are `num_balanced_3_year_sample' distinct entities that are ever acquired within our sample and have at least 3 years of pre-period observations and 3 years of post-period observations." _n

file write out "\item We treat acquiring another hospital as an absorbing state. There are `restricted_acq_final' distinct entities that ever conduct an acquisition within our sample and have at least 2 years of pre-period observations and 2 years of post-period observations." _n

file write out "\end{itemize}" _n
file close out

*----------------------------------------------------------
* Merge in CEO Turnover Measure
*----------------------------------------------------------
preserve
	use "${dbdata}/derived/temp/indiv_file_contextual.dta", clear
	 
	collapse (max) ceo_turnover1, by(entity_uniqueid year)

	keep entity_uniqueid year ceo_turnover1

	tempfile ceo_turnover1_xwalk
	save `ceo_turnover1_xwalk'

restore

merge m:1 entity_uniqueid year using `ceo_turnover1_xwalk'
keep if _merge == 3
drop _merge

* GET TURNOVER AVERAGES BY TREATMENT STATUS_____________________________________

*----------------------------------------------------------
* Helper: summarize + count distinct entities for each group
*----------------------------------------------------------
capture program drop summarize_turnover
program define summarize_turnover
    syntax , cond(str) prefix(str)

    quietly summ ceo_turnover1 if `cond'
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


*----------------------------------------------------------
* Compute stats for each subgroup
*----------------------------------------------------------
summarize_turnover , cond("never_m_and_a")              prefix(clean_control)

summarize_turnover , cond("never_tar")              prefix(never_tar0)
summarize_turnover , cond("full_tar_sample") 		prefix(full_tar)
summarize_turnover , cond("restricted_treated_sample") 	prefix(restricted_tar)
summarize_turnover , cond("full_tar_sample & tar_reltime < 0") 			prefix(pre_full_tar)
summarize_turnover , cond("restricted_treated_sample & tar_reltime < 0") 	prefix(pre_restricted_tar)

summarize_turnover , cond("never_acq")              prefix(never_acq0)
summarize_turnover , cond("full_acq_sample")        prefix(full_acq)
summarize_turnover , cond("restricted_acq_sample")  prefix(restricted_acq)
summarize_turnover , cond("full_acq_sample & acq_reltime < 0")        prefix(pre_full_acq)
summarize_turnover , cond("restricted_acq_sample & acq_reltime < 0")  prefix(pre_restricted_acq)

*----------------------------------------------------------
* Format all numbers for LaTeX
*----------------------------------------------------------
foreach v in clean_control never_tar0 full_tar restricted_tar pre_full_tar pre_restricted_tar ///
	never_acq0 full_acq restricted_acq pre_full_acq pre_restricted_acq {
    local val_mean = ``v'_mean'
    local val_se   = ``v'_se'
    local val_N    = ``v'_N'
    local val_nent = ``v'_nent'

    local `v'_mean_str : display %9.3f `val_mean'
    local `v'_se_str   : display %9.3f `val_se'
    local `v'_N_str    : display %9.0f `val_N'
    local `v'_nent_str : display %9.0f `val_nent'
}

*----------------------------------------------------------
* Write to Latex 
*----------------------------------------------------------
file open out using "${overleaf}/notes/Event Study Setup/tables/sample_counts.tex", write replace
file write out ///
"\begin{table}[!ht]\n " ///
"\centering\n" ///
"\caption{CEO turnover means, SEs, and counts}\n " ///
"\label{tab:turnover_means}\n " ///
"\begin{tabular}{lcccc}\n " ///
"\toprule\n " ///
"Group & Mean & SE & N & Distinct Entities\\\\\n" ///
"\midrule\n " ///
"Never treated (tar or acq) & `clean_control_mean_str' & `clean_control_se_str' & `clean_control_N_str' & `clean_control_nent_str'\\\\\n " ///
"Never treated (tar) & `never_tar0_mean_str' & `never_tar0_se_str' & `never_tar0_N_str' & `never_tar0_nent_str'\\\\\n " ///
"Full treated sample (tar) & `full_tar_mean_str' & `full_tar_se_str' & `full_tar_N_str' & `full_tar_nent_str'\\\\\n " ///
"Restricted treated sample (tar) & `restricted_tar_mean_str' & `restricted_tar_se_str' & `restricted_tar_N_str' & `restricted_tar_nent_str'\\\\\n " ///
"Full treated sample : pre-period only (tar) & `pre_full_tar_mean_str' & `pre_full_tar_se_str' & `pre_full_tar_N_str' & `pre_full_tar_nent_str'\\\\\n " ///
"Restricted treated sample : pre-period only (tar)& `pre_restricted_tar_mean_str' & `pre_restricted_tar_se_str' & `pre_restricted_tar_N_str' & `restricted_tar_nent_str'\\\\\n " ///
"Never treated (acq) & `never_acq0_mean_str' & `never_acq0_se_str' & `never_acq0_N_str' & `never_acq0_nent_str'\\\\\n " ///
"Full treated sample (acq) & `full_acq_mean_str' & `full_acq_se_str' & `full_acq_N_str' & `full_acq_nent_str'\\\\\n " ///
"Restricted treated sample (acq) & `restricted_acq_mean_str' & `restricted_acq_se_str' & `restricted_acq_N_str' & `restricted_acq_nent_str'\\\\\n " ///
"Full treated sample : pre-period only (acq) & `pre_full_acq_mean_str' & `pre_full_acq_se_str' & `pre_full_acq_N_str' & `pre_full_acq_nent_str'\\\\\n " ///
"Restricted treated sample : pre-period only (acq) & `pre_restricted_acq_mean_str' & `pre_restricted_acq_se_str' & `pre_restricted_acq_N_str' & `restricted_acq_nent_str'\\\\\n " ///
"\bottomrule\n" ///
"\end{tabular}\n" ///
"\end{table}\n"
file close out

*----------------------------------------------------------
* Create Plots
*----------------------------------------------------------

* CEO Turnover by Target Treatment Status
preserve

gen long obsid = _n
gen byte g0 = never_m_and_a
gen byte g1 = never_tar
gen byte g2 = full_tar_sample
gen byte g3 = restricted_treated_sample

reshape long g, i(obsid) j(group)
keep if g == 1

collapse (mean) ceo_turnover1, by(year group)

label define group 0 "Never M&A" 1 "Never treated (tar)" 2 "Ever treated (tar)" 3 "Sample (tar)", replace
label values group group

twoway ///
(line ceo_turnover1 year if group==0, lcolor(green) lpattern(longdash_dot)) ///
(line ceo_turnover1 year if group==1, lcolor(black) lpattern(solid)) ///
(line ceo_turnover1 year if group==2, lcolor(blue)  lpattern(dash)) ///
(line ceo_turnover1 year if group==3, lcolor(red)   lpattern(dot)) ///
, legend(order(1 "Never M&A" 2 "Never treated" 3 "Ever treated" 4 "Sample") ring(0) pos(11)) ///
  ytitle("Mean CEO turnover") xtitle("Year") ///
  title("CEO Turnover by Treatment Group (Target)") ///
  scheme(s1color)

  graph export "${overleaf}/notes/Event Study Setup/figures/turnover_by_tar_treatment.pdf", as(pdf) name("Graph") replace

restore

* CEO Turnover by Acquisition Treatment Status
preserve

gen long obsid = _n
gen byte g0 = never_m_and_a
gen byte g1 = never_acq
gen byte g2 = full_acq_sample
gen byte g3 = restricted_acq_sample

reshape long g, i(obsid) j(group)
* Now each row is (obs, group). Keep only rows where the obs belongs to that group:
keep if g == 1

collapse (mean) ceo_turnover1, by(year group)

label define group 0 "Never M&A" 1 "Never treated (acq)" 2 "Ever treated (acq)" 3 "Sample (acq)", replace
label values group group


twoway ///
(line ceo_turnover1 year if group==0, lcolor(green) lpattern(longdash_dot)) ///
(line ceo_turnover1 year if group==1, lcolor(black) lpattern(solid)) ///
(line ceo_turnover1 year if group==2, lcolor(blue)  lpattern(dash)) ///
(line ceo_turnover1 year if group==3, lcolor(red)   lpattern(dot)) ///
, legend(order(1 "Never M&A" 2 "Never treated" 3 "Ever treated" 4 "Sample") ring(0) pos(11)) ///
  ytitle("Mean CEO turnover") xtitle("Year") ///
  title("CEO Turnover by Treatment Group (Acquiring)") ///
  scheme(s1color)


    graph export "${overleaf}/notes/Event Study Setup/figures/turnover_by_acq_treatment.pdf", as(pdf) name("Graph") replace

restore

* CEO Turnover by Acquisition Treatment Status and Cohort
preserve
keep if restricted_acq_sample == 1

collapse (mean) ceo_turnover1, by(year acq_event_year)

twoway ///
(line ceo_turnover1 year if acq_event_year==2011, lcolor(red) lpattern(solid)) ///
(line ceo_turnover1 year if acq_event_year==2012, lcolor(orange) lpattern(solid)) ///
(line ceo_turnover1 year if acq_event_year==2013, lcolor(green) lpattern(solid)) ///
(line ceo_turnover1 year if acq_event_year==2014, lcolor(blue) lpattern(solid)) ///
(line ceo_turnover1 year if acq_event_year==2015, lcolor(purple) lpattern(solid)), ///
legend(order(1 "2011" 2 "2012" 3 "2013" 4 "2014" 5 "2015" ) ///
       pos(11) ring(0) col(1) size(small)) ///
ytitle("Mean CEO Turnover") ///
xtitle("Calendar Year") ///
title("CEO Turnover by Year and Acquisition Event Year (2011–2015)") ///
scheme(s1color)

  graph export "${overleaf}/notes/Event Study Setup/figures/turnover_by_acq_cohort.pdf", as(pdf) name("Graph") replace

restore

* CEO Turnover by Target Treatment Status and Cohort
preserve
keep if restricted_treated_sample == 1

collapse (mean) ceo_turnover1, by(year tar_event_year)

twoway ///
(line ceo_turnover1 year if tar_event_year==2011, lcolor(red) lpattern(solid)) ///
(line ceo_turnover1 year if tar_event_year==2012, lcolor(orange) lpattern(solid)) ///
(line ceo_turnover1 year if tar_event_year==2013, lcolor(green) lpattern(solid)) ///
(line ceo_turnover1 year if tar_event_year==2014, lcolor(blue) lpattern(solid)) ///
(line ceo_turnover1 year if tar_event_year==2015, lcolor(purple) lpattern(solid)), ///
legend(order(1 "2011" 2 "2012" 3 "2013" 4 "2014" 5 "2015" ) ///
       pos(11) ring(0) col(1) size(small)) ///
ytitle("Mean CEO Turnover") ///
xtitle("Calendar Year") ///
title("CEO Turnover by Year and Target Event Year (2011–2015)") ///
scheme(s1color)

  graph export "${overleaf}/notes/Event Study Setup/figures/turnover_by_tar_cohort.pdf", as(pdf) name("Graph") replace

restore
 

*----------------------------------------------------------
* Preliminary Sun and Abraham Plots
*----------------------------------------------------------

* create variable for CEO turnover as absorbing state
sort entity_uniqueid year
by entity_uniqueid: gen byte ever_turnover = sum(cond(ceo_turnover1==1, 1, 0)) > 0

*----------------------------------------------------------
* Effect on Targeted Hospitals
*----------------------------------------------------------
preserve

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

* Create last-treated indicator
gen last_treated = tar_event_year == 2017

* Specify conditions
local spec1_treated "full_tar_sample == 1"
local spec1_control "never_tar == 1"
local spec1_cohort "never_tar"
local spec1_name "Full Sample, Never Treated"

local spec2_treated "restricted_treated_sample == 1"
local spec2_control "never_tar == 1"
local spec2_cohort "never_tar"
local spec2_name "Restricted Sample, Never Treated"

local spec3_treated "full_tar_sample == 1"
local spec3_control "never_m_and_a == 1"
local spec3_cohort "never_m_and_a"
local spec3_name "Full Sample, Never M&A"

local spec4_treated "restricted_treated_sample == 1"
local spec4_control "never_m_and_a == 1"
local spec4_cohort "never_m_and_a"
local spec4_name "Restricted Sample, Never M&A"

local spec5_treated "full_tar_sample == 1 & year < 2017"
local spec5_control "last_treated == 1 & year < 2017"
local spec5_cohort "last_treated"
local spec5_name "Full Sample, Last Treated"

local spec6_treated "restricted_treated_sample == 1 & year < 2017"
local spec6_control "last_treated == 1 & year < 2017" 
local spec6_cohort "last_treated"
local spec6_name "Restricted Sample, Last Treated"

local nspecs = 6

**** loop through specifications ****
forvalues s = 1/`nspecs' {
    
    display _newline(2) "{hline 60}"
    display "Specification `s': `spec`s'_name'"
    display "{hline 60}"
    
    // First run: Calculate average effect
    eventstudyinteract ceo_turnover1 ev_lead* ev_lag* ///
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
    eventstudyinteract ceo_turnover1 ev_lead* ev_lag* ///
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
                  title("Effect Being Acquired on CEO Turnover - `spec`s'_name'" ///
                        "Average Effect: `avg_effect' (SE: `avg_se')", size(medium))) ///
        stub_lag(ev_lag#) ///
        stub_lead(ev_lead#) ///
        trimlag(4) ///
        trimlead(4) ///
        plottype(scatter) ///
        ciplottype(rcap)
    graph export "${overleaf}/notes/Event Study Setup/figures/tar_spec`s'.pdf", as(pdf) name("Graph") replace
}

restore

*----------------------------------------------------------
* Effect on Acquiring Hospitals
*----------------------------------------------------------
preserve

sum acq_reltime, meanonly
local rmin = r(min)
local rmax = r(max)

* Create Relative Time Indicators
forvalues h = 0/`rmax' {
    gen byte ev_lag`h' = (acq_reltime == `h')
}
forvalues h = 1/`=abs(`rmin')' {
    gen byte ev_lead`h' = (acq_reltime == -`h')
}
replace ev_lead1 = 0

* Create last-treated indicator
gen last_treated = acq_event_year == 2017

* Specify conditions
local spec1_treated "full_acq_sample == 1"
local spec1_control "never_acq == 1"
local spec1_cohort "never_acq"
local spec1_name "Full Sample, Never Treated"

local spec2_treated "restricted_acq_sample == 1"
local spec2_control "never_acq == 1"
local spec2_cohort "never_acq"
local spec2_name "Restricted Sample, Never Treated"

local spec3_treated "full_acq_sample == 1"
local spec3_control "never_m_and_a == 1"
local spec3_cohort "never_m_and_a"
local spec3_name "Full Sample, Never M&A"

local spec4_treated "restricted_acq_sample == 1"
local spec4_control "never_m_and_a == 1"
local spec4_cohort "never_m_and_a"
local spec4_name "Restricted Sample, Never M&A"

local spec5_treated "full_acq_sample == 1 & year < 2017"
local spec5_control "last_treated == 1 & year < 2017"
local spec5_cohort "last_treated"
local spec5_name "Full Sample, Last Treated"

local spec6_treated "restricted_acq_sample == 1 & year < 2017"
local spec6_control "last_treated == 1 & year < 2017" 
local spec6_cohort "last_treated"
local spec6_name "Restricted Sample, Last Treated"

local nspecs = 6

**** loop through specifications ****
forvalues s = 1/`nspecs' {
    
    display _newline(2) "{hline 60}"
    display "Specification `s': `spec`s'_name'"
    display "{hline 60}"
    
    // First run: Calculate average effect
    eventstudyinteract ceo_turnover1 ev_lead* ev_lag* ///
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
    eventstudyinteract ceo_turnover1 ev_lead* ev_lag* ///
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
                  title("Effect of Acquiring on CEO Turnover - `spec`s'_name'" ///
                        "Average Effect: `avg_effect' (SE: `avg_se')", size(medium))) ///
        stub_lag(ev_lag#) ///
        stub_lead(ev_lead#) ///
        trimlag(4) ///
        trimlead(4) ///
        plottype(scatter) ///
        ciplottype(rcap)
    graph export "${overleaf}/notes/Event Study Setup/figures/acq_spec`s'.pdf", as(pdf) name("Graph") replace
}

restore
