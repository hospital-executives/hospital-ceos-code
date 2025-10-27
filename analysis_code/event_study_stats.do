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
file write out "\item There are `n_tar1' distinct entities that ever have tar == 1." _n
file write out "\item There are `n_cond2' distinct entities that ever have acq == 1." _n
file write out "\item There are `n_cond3' distinct entities that satisfy tar == 1 or acq == 1." _n
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

* create flags for pre/post sample
foreach k in -2 -1 0 1 2 {
    local clean = subinstr("`k'", "-", "m", .)
    bys entity_uniqueid: egen has_tar_`clean' = max(tar_reltime == `k')
	bys entity_uniqueid: egen has_acq_`clean' = max(acq_reltime == `k')
}
egen tar_sample = rowmin(has_tar_m2 has_tar_m1 has_tar_0 has_tar_1 has_tar_2)
egen acq_sample = rowmin(has_acq_m2 has_acq_m1 has_acq_0 has_acq_1 has_acq_2)

bys entity_uniqueid: egen second_tar = max( (tar == 1) & inlist(tar_reltime, 1, 2) )
bys entity_uniqueid: egen second_acq = max( (acq == 1) & inlist(acq_reltime, 1, 2) )

gen final_tar_sample = tar_sample & !second_tar
gen final_acq_sample = acq_sample & !second_acq

quietly distinct entity_uniqueid if tar_sample
local tar_sample_n = r(ndistinct)

quietly distinct entity_uniqueid if acq_sample
local acq_sample_n = r(ndistinct)

quietly distinct entity_uniqueid if second_tar
local second_tar = r(ndistinct)

quietly distinct entity_uniqueid if second_acq
local second_acq = r(ndistinct)

quietly distinct entity_uniqueid if final_tar_sample
local tar_final = r(ndistinct)

quietly distinct entity_uniqueid if final_acq_sample
local acq_final = r(ndistinct)

file open out using "${overleaf}/notes/Event Study Setup/tables/prelim_sample_counts.tex", write append
file write out "\item There are `tar_sample_n' distinct entities that are ever acquired and have the necessary number of pre-periods and post-periods." _n
file write out "\item There are `acq_sample_n' distinct entities that ever acquire another hospital and have the necessary number of pre-periods and post-periods." _n
file write out "\item There are `second_tar' distinct entities that experience a second acquisition in the two years following the initial acquistion." _n
file write out "\item There are `second_acq' distinct entities that conduct another acquisition in the two years following their initial acquisition." _n
file write out "\item This results in `tar_final' distinct entities that are ever treated by being acquired." _n
file write out "\item There are `acq_final' distinct entities that experience only one acquisition within the sample period." _n
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
summarize_turnover , cond("never_tar")              prefix(never_tar0)
summarize_turnover , cond("!never_tar")             prefix(ever_tar1)
summarize_turnover , cond("tar_sample & !second_tar") prefix(sample_tar)
summarize_turnover , cond("never_acq")              prefix(never_acq0)
summarize_turnover , cond("!never_acq")             prefix(ever_acq1)
summarize_turnover , cond("acq_sample")             prefix(sample_acq)

*----------------------------------------------------------
* Format all numbers for LaTeX
*----------------------------------------------------------
foreach v in never_tar0 ever_tar1 sample_tar never_acq0 ever_acq1 sample_acq {
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
"Never treated (tar) & `never_tar0_mean_str' & `never_tar0_se_str' & `never_tar0_N_str' & `never_tar0_nent_str'\\\\\n " ///
"Ever treated (tar)  & `ever_tar1_mean_str'  & `ever_tar1_se_str'  & `ever_tar1_N_str'  & `ever_tar1_nent_str'\\\\\n " ///
"Estimation sample (tar) & `sample_tar_mean_str' & `sample_tar_se_str' & `sample_tar_N_str' & `sample_tar_nent_str'\\\\\n " ///
"Never treated (acq) & `never_acq0_mean_str' & `never_acq0_se_str' & `never_acq0_N_str' & `never_acq0_nent_str'\\\\\n " ///
"Ever treated (acq)  & `ever_acq1_mean_str'  & `ever_acq1_se_str'  & `ever_acq1_N_str'  & `ever_acq1_nent_str'\\\\\n " ///
"Estimation sample (acq) & `sample_acq_mean_str' & `sample_acq_se_str' & `sample_acq_N_str' & `sample_acq_nent_str'\\\\\n " ///
"\bottomrule\n" ///
"\end{tabular}\n" ///
"\end{table}\n"
file close out

*----------------------------------------------------------
* Create Plots
*----------------------------------------------------------

* CEO Turnover by Target Treatment Status
preserve

gen group = .
replace group = 1 if never_tar
replace group = 2 if !never_tar
replace group = 3 if final_tar_sample & !second_tar
label define group 1 "Never treated (tar)" 2 "Ever treated (tar)" 3 "Sample (tar)"
label values group group

collapse (mean) ceo_turnover1, by(year group)

twoway ///
(line ceo_turnover1 year if group==1, lcolor(black) lpattern(solid)) ///
(line ceo_turnover1 year if group==2, lcolor(blue) lpattern(dash)) ///
(line ceo_turnover1 year if group==3, lcolor(red) lpattern(dot)) ///
, legend(order(1 "Never treated" 2 "Ever treated" 3 "Sample") ring(0) pos(11)) ///
  ytitle("Mean CEO turnover") ///
  xtitle("Year") ///
  title("CEO Turnover by Treatment Group (Target)") ///
  scheme(s1color)

  graph export "${overleaf}/notes/Event Study Setup/figures/turnover_by_tar_treatment.pdf", as(pdf) name("Graph") replace

restore

* CEO Turnover by Acquisition Treatment Status
preserve

gen group = .
replace group = 1 if never_acq
replace group = 2 if !never_acq
replace group = 3 if acq_sample 
label define group 1 "Never treated" 2 "Ever treated" 3 "Sample"
label values group group

collapse (mean) ceo_turnover1, by(year group)

twoway ///
(line ceo_turnover1 year if group==1, lcolor(black) lpattern(solid)) ///
(line ceo_turnover1 year if group==2, lcolor(blue) lpattern(dash)) ///
(line ceo_turnover1 year if group==3, lcolor(red) lpattern(dot)) ///
, legend(order(1 "Never treated" 2 "Ever treated" 3 "Sample") ring(0) pos(11)) ///
  ytitle("Mean CEO turnover") ///
  xtitle("Year") ///
  title("CEO Turnover by Treatment Group (Acquiring)") ///
  scheme(s1color)

    graph export "${overleaf}/notes/Event Study Setup/figures/turnover_by_acq_treatment.pdf", as(pdf) name("Graph") replace

restore

* CEO Turnover by Acquisition Treatment Status and Cohort
preserve
keep if acq_sample == 1

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
keep if final_tar_sample == 1

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

* SA - Never Treated Control (Targets)
preserve

keep if (final_tar_sample & tar_reltime <= 2 & tar_reltime >= -2) | never_tar

gen neg_rel_2 = tar_reltime == -2
gen neg_rel_1 = tar_reltime == -1
gen rel_0 = tar_reltime == 0
gen rel_1 = tar_reltime == 1
gen rel_2 = tar_reltime == 2

replace neg_rel_1=0

eventstudyinteract ceo_turnover1 neg_rel_* rel_*, vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-2(1)2) ///
		title("Effect Being Acquired on CEO Turnover - Never Treated")) stub_lag(rel_#) stub_lead(neg_rel_#) trimlag(-2) trimlead(2) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/sa_tar_never_treated_control.pdf", as(pdf) name("Graph") replace

eventstudyinteract ever_turnover neg_rel_* rel_*, vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-2(1)2) ///
		title("Effect Being Acquired on Ever CEO Turnover - Never Treated")) stub_lag(rel_#) stub_lead(neg_rel_#) trimlag(2) trimlead(2) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/sa_tar_never_absorb.pdf", as(pdf) name("Graph") replace

restore

* SA - Last Treated Control (Target in 2015)
preserve

gen last_treated = tar_event_year == 2015
keep if (final_tar_sample & inlist(tar_reltime, -2, -1, 0, 1, 2)) | last_treated
drop if year > 2014

gen neg_rel_2 = tar_reltime == -2
gen neg_rel_1 = tar_reltime == -1
gen rel_0 = tar_reltime == 0
gen rel_1 = tar_reltime == 1
gen rel_2 = tar_reltime == 2

replace neg_rel_1=0

eventstudyinteract ceo_turnover1 neg_rel_* rel_*, vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(last_treated)
	
event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-2(1)2) ///
		title("Effect of Being Acquired on CEO Turnover - Last Treated Control")) stub_lag(rel_#) stub_lead(neg_rel_#) ///
		trimlag(2) trimlead(2) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/sa_tar_last_treated_control.pdf", as(pdf) name("Graph") replace

eventstudyinteract ever_turnover neg_rel_* rel_*, vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(last_treated)
	
event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-2(1)2) ///
		title("Effect of Being Acquired on Ever CEO Turnover - Last Treated")) stub_lag(rel_#) stub_lead(neg_rel_#) ///
		trimlag(2) trimlead(2) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/sa_tar_last_absorb.pdf", as(pdf) name("Graph") replace


restore

* SA - Never Treated Control (Acquisitions)
preserve

keep if (acq_sample & inlist(acq_reltime, -2, -1, 0, 1, 2)) | never_acq

gen neg_rel_2 = acq_reltime == -2
gen neg_rel_1 = acq_reltime == -1
gen rel_0 = acq_reltime == 0
gen rel_1 = acq_reltime == 1
gen rel_2 = acq_reltime == 2

replace neg_rel_1=0

eventstudyinteract ceo_turnover1 neg_rel_* rel_*, vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-2(1)2) ///
		title("Effect of Acquiring on CEO Turnover - Never Treated Control")) stub_lag(rel_#) stub_lead(neg_rel_#) trimlag(2) trimlead(2) ///
		plottype(scatter) ciplottype(rcap)
graph export "${overleaf}/notes/Event Study Setup/figures/sa_acq_never_treated_control.pdf", as(pdf) name("Graph") replace

eventstudyinteract ever_turnover neg_rel_* rel_*, vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-2(1)2) ///
		title("Effect of Acquiring on Ever CEO Turnover - Never Treated")) stub_lag(rel_#) stub_lead(neg_rel_#) trimlag(2) trimlead(2) ///
		plottype(scatter) ciplottype(rcap)
graph export "${overleaf}/notes/Event Study Setup/figures/sa_acq_never_absorb.pdf", as(pdf) name("Graph") replace


restore

* SA - Last Treated Control (Target in 2015)
preserve

gen last_treated = acq_event_year == 2015
keep if (acq_sample & inlist(acq_reltime, -2, -1, 0, 1, 2)) | last_treated
drop if year > 2014

gen neg_rel_2 = acq_reltime == -2
gen neg_rel_1 = acq_reltime == -1
gen rel_0 = acq_reltime == 0
gen rel_1 = acq_reltime == 1
gen rel_2 = acq_reltime == 2

replace neg_rel_1=0

eventstudyinteract ceo_turnover1 neg_rel_* rel_*, vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(last_treated)
	
event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-2(1)2) ///
		title("Effect of Acquiring on CEO Turnover - Last Treated Control")) stub_lag(rel_#) stub_lead(neg_rel_#) ///
		trimlag(2) trimlead(2) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/sa_acq_last_treated_control.pdf", as(pdf) name("Graph") replace


eventstudyinteract ever_turnover neg_rel_* rel_*, vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(last_treated)
	
event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-2(1)2) ///
		title("Effect of Acquiring on Ever CEO Turnover - Last Treated")) stub_lag(rel_#) stub_lead(neg_rel_#) ///
		trimlag(2) trimlead(2) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/sa_acq_last_absorb.pdf", as(pdf) name("Graph") replace


restore

*----------------------------------------------------------
* Sun and Abraham Plots with 3 year pre- and post-periods
*----------------------------------------------------------
preserve

drop second_tar tar_sample final_tar_sample
foreach k in -3 3 {
    local clean = subinstr("`k'", "-", "m", .)
    bys entity_uniqueid: egen has_tar_`clean' = max(tar_reltime == `k')
	bys entity_uniqueid: egen has_acq_`clean' = max(acq_reltime == `k')
}
egen tar_sample = rowmin(has_tar_m3 has_tar_m2 has_tar_m1 has_tar_0 has_tar_1 has_tar_2 has_tar_3)
bys entity_uniqueid: egen second_tar = max( (tar == 1) & inlist(tar_reltime, 1, 2, 3) )

gen final_tar_sample = tar_sample & !second_tar
keep if (final_tar_sample & inlist(tar_reltime, -3, -2, -1, 0, 1, 2, 3)) | never_tar

gen neg_rel_3 = tar_reltime == -3
gen neg_rel_2 = tar_reltime == -2
gen neg_rel_1 = tar_reltime == -1
gen rel_0 = tar_reltime == 0
gen rel_1 = tar_reltime == 1
gen rel_2 = tar_reltime == 2
gen rel_3 = tar_reltime == 3

replace neg_rel_1=0

eventstudyinteract ceo_turnover1 neg_rel_* rel_*, vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-3(1)3) ///
		title("Effect Being Acquired on CEO Turnover - Never Treated Control")) stub_lag(rel_#) stub_lead(neg_rel_#) trimlag(3) trimlead(3) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/sa_tar_never_treated_control_3_years.pdf", as(pdf) name("Graph") replace


eventstudyinteract ever_turnover neg_rel_* rel_*, vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) cohort(tar_event_year) control_cohort(never_tar)

	event_plot e(b_iw)#e(V_iw), default_look graph_opt(xtitle("Periods since the event") ytitle("Average effect") xlabel(-3(1)3) ///
		title("Effect Being Acquired on Ever CEO Turnover - Never Treated Control")) stub_lag(rel_#) stub_lead(neg_rel_#) trimlag(3) trimlead(3) plottype(scatter) ciplottype(rcap)

graph export "${overleaf}/notes/Event Study Setup/figures/sa_tar_never_absorb_3_years.pdf", as(pdf) name("Graph") replace

restore




// callaway and santanna
preserve
staggered ceo_turnover1, i(entity_uniqueid) t(year) g(tar_event_year) estimand(simple) sa
// staggered ceo_turnover1, i(entity_uniqueid) t(year) g(tar_event_year) estimand(eventstudy)
restore
