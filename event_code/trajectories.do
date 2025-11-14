/* TRAJECTORIES *************************************************************

Program name: 	trajectories.do
Programmer: 	Katherine Papen

Goal: 			Generate event study plots with heterogeneity by where CEOs in the
				previous year go.
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
make_outcome_vars 
make_target_sample

// build data set of people who are CEOs in time t (leader_in_prev == 1)
// and where they are in time t + 1
frame create individual_char
frame change individual_char

	use "${dbdata}/derived/individuals_final.dta", clear
	keep id aha_leader_flag all_leader_flag ceo_himss_title_exact ceo_himss_title_fuzzy
	tempfile ceo_flags
	save `ceo_flags'

	use "${dbdata}/derived/temp/indiv_file_contextual.dta", clear
	merge m:1 id using `ceo_flags'
	
	keep if entity_aha != . & _merge == 3
	keep contact_uniqueid year all_leader_flag entity_aha haentitytypeid sysid entity_uniqueid aha_bdtot
	tostring contact_uniqueid, replace

	rename ///
	(contact_uniqueid entity_aha haentitytypeid all_leader_flag sysid entity_uniqueid aha_bdtot) ///
	(contact_lag1 prev_ceo_aha_id prev_ceo_haentitytypeid prev_ceo_all_leader_flag prev_ceo_sysid prev_ceo_entity_uniqueid prev_ceo_bdtot)
	
	keep year contact_lag1 prev* 
	bys contact_lag1 year: keep if _n == 1

	tempfile individual_characteristics
	save `individual_characteristics'

frame change default
frame drop individual_char

destring aha_id, replace
merge m:1 contact_lag1 year using `individual_characteristics', assert(master match using) keep (master match) gen(_prev_ceo_merge)


frame create ceo_ids
frame change ceo_ids

	use "${dbdata}/derived/individuals_final.dta", clear
	keep if entity_aha != . & all_leader_flag == 1
	tostring contact_uniqueid, replace
	
	keep contact_uniqueid year entity_aha
	
	bys entity_aha year: egen n_unique_contacts = nvals(contact_uniqueid)
	gen multi_contact = n_unique_contacts > 1	
	drop if multi_contact == 1
	bys entity_aha year: keep if _n == 1
	
	rename entity_aha aha_id
	
	tempfile current_ceo_ids
	save `current_ceo_ids'

frame change default
frame drop ceo_ids

merge m:1 aha_id year using `current_ceo_ids', assert(master match using) keep (match)

cap gen ceo_turnover2 = contact_uniqueid != contact_lag1
replace ceo_turnover2 = . if year == 2009

cap gen prev_left = _prev_ceo_merge == 1 & ceo_turnover1 == 1

// get additional ceo turnover measures
gen prev_oth_hospital = prev_ceo_aha_id != aha_id & ceo_turnover1 == 1
gen prev_oth_hospital_diff_sys = prev_ceo_sysid != sysid_ma & ceo_turnover1 == 1
gen prev_oth_hospital_same_sys = prev_ceo_sysid == sysid_ma & ceo_turnover1 == 1
gen prev_at_sys = prev_ceo_haentitytypeid == 8 & ceo_turnover1 == 1

gen non_missing = aha_bdtot != . & prev_ceo_bdtot != .
gen prev_at_larger = (prev_ceo_bdtot > aha_bdtot) & ceo_turnover1 == 1 & non_missing
gen prev_at_much_larger = (prev_ceo_bdtot > 2*aha_bdtot) & ceo_turnover1 == 1 & non_missing
gen prev_at_smaller = (prev_ceo_bdtot < aha_bdtot) & ceo_turnover1 == 1 & non_missing

gen not_prev_3_temp = (prev_oth_hospital == 0 & prev_oth_hospital_same_sys == 0 & prev_at_sys == 0)
gen prev_other = not_prev_3_temp == 1 & ceo_turnover1 == 1

gen missing_trajectory = (not_prev_3_temp == 0 & prev_left == 0) & ceo_turnover1 == 1
drop not_prev_3_temp

sort aha_id year
bys aha_id : gen prev_at_sys_lag = prev_at_sys[_n-1]
count if (prev_at_sys | prev_at_sys_lag) & ceo_turnover1 == 1 & tar_reltime == 0
count if ceo_turnover1 == 1 & tar_reltime == 0

preserve
keep if balanced_2_year_sample 

collapse (mean) prev_at_much_larger prev_at_larger prev_at_smaller, by(tar_reltime)

twoway ///
    (line prev_at_much_larger tar_reltime, lcolor(blue) lpattern(solid)) ///
    (line prev_at_larger tar_reltime, lcolor(green) lpattern(dash)) ///
    (line prev_at_smaller tar_reltime, lcolor(red) lpattern(dot)), ///
    legend(order(1 "Much larger" 2 "Larger" 3 "Smaller") ring(0) pos(11)) ///
    ytitle("Mean acquisition probability") ///
    xtitle("Relative time") ///
    title("Acquisition Patterns by Size Relationship") ///
    scheme(s1color)
restore

preserve
keep if balanced_2_year_sample 
collapse (mean) prev_at_much_larger prev_at_larger prev_at_smaller, ///
    by(tar_reltime char_female_lag_1)

twoway ///
    (line prev_at_much_larger tar_reltime if char_female_lag_1==0, lcolor(blue) lpattern(solid)) ///
    (line prev_at_much_larger tar_reltime if char_female_lag_1==1, lcolor(blue) lpattern(dash)) ///
    (line prev_at_larger tar_reltime if char_female_lag_1==0, lcolor(green) lpattern(solid)) ///
    (line prev_at_larger tar_reltime if char_female_lag_1==1, lcolor(green) lpattern(dash)) ///
    (line prev_at_smaller tar_reltime if char_female_lag_1==0, lcolor(red) lpattern(solid)) ///
    (line prev_at_smaller tar_reltime if char_female_lag_1==1, lcolor(red) lpattern(dash)), ///
    legend(order(1 "Much larger (Male)" 2 "Much larger (Female)" ///
                 3 "Larger (Male)" 4 "Larger (Female)" ///
                 5 "Smaller (Male)" 6 "Smaller (Female)") ///
           ring(0) pos(11) cols(2)) ///
    ytitle("Mean acquisition probability") ///
    xtitle("Relative time") ///
    title("Acquisition Patterns by Size and CEO Gender") ///
    scheme(s1color)
restore 


frame copy default event_studies
frame change event_studies

	local binvar "char_female_lag_1"
	local binname "fem1"
	local label1_0 "Male CEO Before Acquisition"
	local label1_1 "Female CEO Before Acquisition"

	sum tar_reltime, meanonly
	local rmin = r(min)
	local rmax = r(max)
	
	forvalues h = 0/`rmax' {
		cap gen byte ev_lag`h' = (tar_reltime == `h')
	}
	forvalues h = 1/`=abs(`rmin')' {
		cap gen byte ev_lead`h' = (tar_reltime == -`h')
	}
	replace ev_lead1 = 0

	forvalues h = 0/`rmax' {
		forvalues c = 0/1 {
			cap gen byte ev`c'_lag`h' = ev_lag`h' * (`binvar' == `c')
			}
		}
		
	forvalues h = 1/`=abs(`rmin')' {
		forvalues c = 0/1 {
			cap gen byte ev`c'_lead`h' = ev_lead`h' * (`binvar' == `c')
			}
		}

	// Set baseline
	forvalues c = 0/1 {
		replace ev`c'_lead1 = 0
		}
                    
      // Single regression with both groups
     eventstudyinteract prev_at_much_larger ev0_lead* ev0_lag* ev1_lead* ev1_lag* ///
        if (balanced_2_year_sample|never_m_and_a), ///
        vce(cluster entity_uniqueid) absorb(entity_uniqueid year) ///
        cohort(tar_event_year) control_cohort(never_m_and_a)
                    
      // Plot directly from regression results
                    coefplot (., keep(ev0_lead3 ev0_lead2 ev0_lead1 ev0_lag0 ev0_lag1 ev0_lag2 ev0_lag3) ///
                                 b(b_iw) v(V_iw) label(`label1_0') mcolor(navy) ciopts(lcolor(navy) recast(rcap)) ///
                                 rename(ev0_lead3=a2 ev0_lead2=a3 ev0_lead1=a4 ev0_lag0=a5 ///
                                        ev0_lag1=a6 ev0_lag2=a7 ev0_lag3=a8)) ///
                             (., keep(ev1_lead3 ev1_lead2 ev1_lead1 ev1_lag0 ev1_lag1 ev1_lag2 ev1_lag3) ///
                                 b(b_iw) v(V_iw) label(`label1_1') mcolor(maroon) ciopts(lcolor(maroon) recast(rcap)) msymbol(D) ///
                                 rename(ev1_lead3=a2 ev1_lead2=a3 ev1_lead1=a4 ev1_lag0=a5 ///
                                        ev1_lag1=a6 ev1_lag2=a7 ev1_lag3=a8)), ///
                        vertical ///
                        yline(0, lcolor(gs8)) ///
                        xline(4, lcolor(gs8) lpattern(dash)) ///
                        order(a2 a3 a4 a5 a6 a7 a8) ///
                        coeflabels(a2="-3" a3="-2" a4="-1" a5="0" a6="1" a7="2" a8="3") ///
                        xtitle("Periods since the event") ytitle("Average effect") ///
                        graphregion(color(white)) plotregion(color(white))
frame change default
frame drop event_studies
