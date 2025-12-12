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
use "${dbdata}/derived/temp/updated_trajectories.dta", clear

preserve 
	use "${dbdata}/derived/temp/merged_ma_sysid_xwalk.dta", clear
	duplicates tag entity_uniqueid year tar, gen(dup_tag)
	assert dup_tag == 0
	drop dup_tag
	
	duplicates drop entity_uniqueid tar year, force

	tempfile tar_file
	save `tar_file'
restore 
merge 1:1 entity_uniqueid year using `tar_file', assert(master using matched) keep(master matched) gen(_merge_1)

* merge in type 
preserve
	use "${dbdata}/derived/temp/merged_ma_nonharmonized.dta", clear
	keep entity_uniqueid year type
	
	tempfile himss_type_xwalk
	save `himss_type_xwalk'
	
	* Create entity-level crosswalk for entities with unique type
	keep entity_uniqueid type
	duplicates drop
	duplicates tag entity_uniqueid, gen(dup)
	keep if dup == 0  // keep only entities with one unique type
	drop dup
	rename type type_entity
	
	tempfile himss_type_entity
	save `himss_type_entity'
restore

* First merge on year
merge 1:1 entity_uniqueid year using `himss_type_xwalk', keep(master matched) gen(_merge_year)
merge m:1 entity_uniqueid using `himss_type_entity', keep(master matched) gen(_merge_entity)
replace type = type_entity if _merge_year == 1 & _merge_entity == 3
replace is_hospital = 1 if _merge_1 == 1

* restrict to hospital sample
restrict_hosp_sample
make_outcome_vars 
make_target_sample

preserve
use "/Users/katherinepapen/Dropbox/hospital_ceos/turnover_export.dta", clear
rename ceo_turnover1 old_ceo_turnover1
tempfile turnover_data
save `turnover_data'
restore

merge m:1 entity_uniqueid year using `turnover_data'


// get missing
gen prev_left = exists_future == 0 & ceo_turnover1 == 1 // 1,141  / 4219
gen prev_oth_hospital =  !missing(contact_lag1) & ceo_turnover1 == 1 & exists_future == 1 & future_at_same_hospital == 0 // 225
gen prev_same_hospital = !missing(contact_lag1) & ceo_turnover1 == 1 & exists_future == 1 & future_at_same_hospital == 1 // 2759

// save "${dbdata}/derived/temp/event_data.dta", replace

// get additional ceo turnover measures
gen prev_oth_hospital_diff_sys = prev_ceo_sysid != sysid_ma & ceo_turnover1 == 1 & prev_oth_hospital == 1
gen prev_oth_hospital_same_sys = prev_ceo_sysid == sysid_ma & ceo_turnover1 == 1 & prev_oth_hospital == 1 
gen prev_at_sys = prev_ceo_haentitytypeid == 8 & ceo_turnover1 == 1

destring prev_ceo_bdtot, replace
gen prev_at_larger = (prev_ceo_bdtot > aha_bdtot) & ceo_turnover1 == 1 & prev_oth_hospital == 1
gen prev_at_much_larger = (prev_ceo_bdtot > 2*aha_bdtot) & ceo_turnover1 == 1 & prev_oth_hospital == 1
gen prev_at_smaller = (prev_ceo_bdtot < aha_bdtot) & ceo_turnover1 == 1 & prev_oth_hospital == 1

// Create the table
// Create the table
preserve
keep if balanced_2_year_sample & inlist(tar_reltime, 0, 1, 2)

// Store counts in a matrix (9 rows - 8 variables + 1 total)
matrix counts = J(9, 3, .)
matrix colnames counts = reltime0 reltime1 reltime2

local row = 1
local varlist "prev_left prev_oth_hospital prev_same_hospital prev_oth_hospital_diff_sys prev_oth_hospital_same_sys prev_at_larger prev_at_smaller prev_at_sys"
local rownames ""

foreach var of local varlist {
    qui count if `var' == 1 & tar_reltime == 0
    matrix counts[`row', 1] = r(N)
    
    qui count if `var' == 1 & tar_reltime == 1
    matrix counts[`row', 2] = r(N)
    
    qui count if `var' == 1 & tar_reltime == 2
    matrix counts[`row', 3] = r(N)
    
    local rownames "`rownames' `var'"
    local row = `row' + 1
}

// Add ceo_turnover1 total row
qui count if ceo_turnover1 == 1 & tar_reltime == 0
matrix counts[9, 1] = r(N)

qui count if ceo_turnover1 == 1 & tar_reltime == 1
matrix counts[9, 2] = r(N)

qui count if ceo_turnover1 == 1 & tar_reltime == 2
matrix counts[9, 3] = r(N)

// Write LaTeX table manually
file open myfile using "${overleaf}/notes/Event Study Setup/tables/turnover_counts.tex", write replace
file write myfile "\begin{tabular}{lccc}" _n
file write myfile "\hline" _n
file write myfile "Variable & t=0 & t=1 & t=2 \\" _n
file write myfile "\hline" _n

// Write first 3 rows
forvalues i = 1/3 {
    local var : word `i' of `varlist'
    local varname = subinstr("`var'", "_", "\_", .)
    file write myfile "`varname' & " (counts[`i',1]) " & " (counts[`i',2]) " & " (counts[`i',3]) " \\" _n
}
file write myfile "\hline" _n

// Write rows 4-5
forvalues i = 4/5 {
    local var : word `i' of `varlist'
    local varname = subinstr("`var'", "_", "\_", .)
    file write myfile "`varname' & " (counts[`i',1]) " & " (counts[`i',2]) " & " (counts[`i',3]) " \\" _n
}
file write myfile "\hline" _n

// Write rows 6-7
forvalues i = 6/7 {
    local var : word `i' of `varlist'
    local varname = subinstr("`var'", "_", "\_", .)
    file write myfile "`varname' & " (counts[`i',1]) " & " (counts[`i',2]) " & " (counts[`i',3]) " \\" _n
}
file write myfile "\hline" _n

// Write row 8
local varname = subinstr("prev_at_sys", "_", "\_", .)
file write myfile "`varname' & " (counts[8,1]) " & " (counts[8,2]) " & " (counts[8,3]) " \\" _n
file write myfile "\hline" _n

// Write total row
file write myfile "Total (CEO Turnover = 1) & " (counts[9,1]) " & " (counts[9,2]) " & " (counts[9,3]) " \\" _n
file write myfile "\hline" _n
file write myfile "\end{tabular}" _n
file close myfile

restore

// get raw aggregated trends
preserve
keep if balanced_2_year_sample 

collapse (mean) prev_at_much_larger prev_at_larger prev_at_smaller prev_left, by(tar_reltime)

twoway ///
    (line prev_at_much_larger tar_reltime, lcolor(blue) lpattern(solid)) ///
    (line prev_at_larger tar_reltime, lcolor(green) lpattern(dash)) ///
    (line prev_at_smaller tar_reltime, lcolor(red) lpattern(dot)) ///
	(line prev_left tar_reltime, lcolor(black) lpattern(solid)), ///
    legend(order(1 "Much larger" 2 "Larger" 3 "Smaller" 4 "Left Sample") pos(6)) ///
    ytitle("Movement probability") ///
    xtitle("Relative time") ///
    title("Acquisition Patterns by Size Relationship") ///
    scheme(s1color)
	
	graph export "${overleaf}/notes/Event Study Setup/figures/size_rawtrends.pdf", as(pdf) name("Graph") replace

restore

// get raw aggregated trends by gender
preserve
keep if balanced_2_year_sample 
collapse (mean) prev_at_much_larger prev_at_larger prev_at_smaller prev_left, ///
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
           pos(6) cols(2)) ///
    ytitle("Share of Turnover") ///
    xtitle("Relative time") ///
    title("Acquisition Patterns by Size and CEO Gender") ///
    scheme(s1color)
	
	  graph export "${overleaf}/notes/Event Study Setup/figures/size_vs_gender_rawtrends.pdf", as(pdf) name("Graph") replace

twoway ///
    (line prev_at_much_larger tar_reltime if char_female_lag_1==0, lcolor(blue) lpattern(solid)) ///
    (line prev_at_much_larger tar_reltime if char_female_lag_1==1, lcolor(blue) lpattern(dash)) ///
    (line prev_at_larger tar_reltime if char_female_lag_1==0, lcolor(green) lpattern(solid)) ///
    (line prev_at_larger tar_reltime if char_female_lag_1==1, lcolor(green) lpattern(dash)) ///
	    (line prev_at_smaller tar_reltime if char_female_lag_1==0, lcolor(red) lpattern(solid)) ///
    (line prev_at_smaller tar_reltime if char_female_lag_1==1, lcolor(red) lpattern(dash)) ///
    (line prev_left tar_reltime if char_female_lag_1==0, lcolor(black) lpattern(solid)) ///
    (line prev_left tar_reltime if char_female_lag_1==1, lcolor(black) lpattern(dash)), ///
    legend(order(1 "Much larger (Male)" 2 "Much larger (Female)" ///
                 3 "Larger (Male)" 4 "Larger (Female)" ///
                 5 "Smaller (Male)" 6 "Smaller (Female)" ///
				 7 "Left Sample (Male)" 8 "Left Sample (Female)") ///
           pos(6) cols(2)) ///
    ytitle("Share of Turnover") ///
    xtitle("Relative time") ///
    title("Acquisition Patterns by Size and CEO Gender") ///
    scheme(s1color)
	
	  graph export "${overleaf}/notes/Event Study Setup/figures/size_vs_gender_rawtrends_leavers.pdf", as(pdf) name("Graph") replace

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
		local outcomes "prev_left prev_at_much_larger prev_at_larger prev_at_smaller"

		foreach outcome of local outcomes {
			
			// get aggregated effects 
			eventstudyinteract `outcome' ev_lead* ev_lag* ///
			if (balanced_2_year_sample|never_m_and_a), ///
			vce(cluster entity_uniqueid) ///
			absorb(entity_uniqueid year) ///
			cohort(tar_event_year) ///
			control_cohort(never_m_and_a)
		
			event_plot e(b_iw)#e(V_iw), ///
				default_look ///
				graph_opt(xtitle("Periods since the event") ///
						  ytitle("Average effect") ///
						  xlabel(-3(1)3) ///
						  title("Effect of Being Targeted", size(medium))) ///
				stub_lag(ev_lag#) ///
				stub_lead(ev_lead#) ///
				trimlag(3) ///
				trimlead(3) ///
				plottype(scatter) ///
				ciplottype(rcap)
			
			graph export "${overleaf}/notes/Event Study Setup/figures/size_`outcome'.pdf", as(pdf) replace

			
			eventstudyinteract `outcome' ev0_lead* ev0_lag* ev1_lead* ev1_lag* ///
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
			
			graph export "${overleaf}/notes/Event Study Setup/figures/gender_size_`outcome'.pdf", as(pdf) replace
		}
		
frame change default
frame drop event_studies



frame copy default outcomes_graphs
frame change outcomes_graphs

keep if balanced_2_year_sample == 1

collapse (mean) prev_oth_hospital prev_same_hospital prev_left, by(tar_reltime)

twoway ///
    (line prev_oth_hospital tar_reltime, lcolor(blue) lpattern(solid)) ///
    (line prev_same_hospital tar_reltime, lcolor(green) lpattern(dash)) ///
	(line prev_left tar_reltime, lcolor(black) lpattern(solid)), ///
    legend(order(1 "Other Hospital" 2 "Same Hospital" 3 "Left Sample") pos(6)) ///
    ytitle("Movement probability") ///
    xtitle("Relative time") ///
    title("Acquisition Patterns by Previous CEO Outcome") ///
    scheme(s1color)
	
	graph export "${overleaf}/notes/Event Study Setup/figures/movement_rawtrends.pdf", as(pdf) name("Graph") replace

frame change default
frame drop outcomes_graphs

frame copy default main_spec
frame change main_spec

	drop if missing(ceo_turnover1)

	sum tar_reltime, meanonly
	local rmin = r(min)
	local rmax = r(max)
	
	forvalues h = 0/`rmax' {
		cap gen byte ev_lag`h' = (tar_reltime == `h')
		cap gen byte left_ev_lag`h' = (tar_reltime == `h' & prev_left)
		cap gen byte oth_ev_lag`h' = (tar_reltime == `h' & prev_oth_hospital)
		cap gen byte same_ev_lag`h' = (tar_reltime == `h' & prev_same_hospital)
	}
	forvalues h = 1/`=abs(`rmin')' {
		cap gen byte ev_lead`h' = (tar_reltime == -`h')
		cap gen byte left_ev_lead`h' = (tar_reltime == -`h' & prev_left)
		cap gen byte oth_ev_lead`h' = (tar_reltime == -`h' & prev_oth_hospital)
		cap gen byte same_ev_lead`h' = (tar_reltime == -`h' & prev_same_hospital)

	}
	replace ev_lead1 = 0
	replace left_ev_lead1 = 0
	replace oth_ev_lead1 = 0
	replace same_ev_lead1 = 0

	eventstudyinteract ceo_turnover1 ev_lead* ev_lag*  ///
	if (balanced_2_year_sample|never_m_and_a), ///
	vce(cluster entity_uniqueid) ///
	absorb(entity_uniqueid year) ///
	cohort(tar_event_year) ///
	control_cohort(never_m_and_a)
	matrix b_turnover = e(b_iw)
	matrix V_turnover = e(V_iw)

	eventstudyinteract prev_left ev_lead* ev_lag*  ///
		if (balanced_2_year_sample|never_m_and_a), ///
		vce(cluster entity_uniqueid) ///
		absorb(entity_uniqueid year) ///
		cohort(tar_event_year) ///
		control_cohort(never_m_and_a)
	matrix b_left = e(b_iw)
	matrix V_left = e(V_iw)

	eventstudyinteract prev_oth_hospital ev_lead* ev_lag*  ///
		if (balanced_2_year_sample|never_m_and_a), ///
		vce(cluster entity_uniqueid) ///
		absorb(entity_uniqueid year) ///
		cohort(tar_event_year) ///
		control_cohort(never_m_and_a)
	estimates store oth_hospital
	matrix b_oth = e(b_iw)
	matrix V_oth = e(V_iw)

	eventstudyinteract prev_same_hospital ev_lead* ev_lag*  ///
		if (balanced_2_year_sample|never_m_and_a), ///
		vce(cluster entity_uniqueid) ///
		absorb(entity_uniqueid year) ///
		cohort(tar_event_year) ///
		control_cohort(never_m_and_a)
	estimates store same_hospital
	matrix b_same = e(b_iw)
	matrix V_same = e(V_iw)

	** Export to Figure **
	
	// Plot total with components
	matrix b_same_stacked = b_same + b_oth
	matrix V_same_stacked = V_same + V_oth

	// Extract SE for turnover
	mata: V = st_matrix("V_turnover"); se = sqrt(diagonal(V))'; st_matrix("se_turnover", se)
	matrix colnames se_turnover = `: colnames b_turnover'
	mata: V = st_matrix("V_oth"); se = sqrt(diagonal(V))'; st_matrix("se_oth", se)
	mata: V = st_matrix("V_same_stacked"); se = sqrt(diagonal(V))'; st_matrix("se_same_stacked", se)
	
	coefplot ///
	(matrix(b_turnover), se(se_turnover) label("Left Sample + Same Hospital + Other Hospital") mcolor(black) ciopts(lcolor(black) lwidth(medium)) msymbol(O)) ///
	(matrix(b_same_stacked), se(se_same_stacked) label("Same Hospital + Other Hospital") mcolor(green) ciopts(lcolor(green) lwidth(medium)) msymbol(S)) ///
	(matrix(b_oth), se(se_oth) label("Other Hospital") mcolor(blue) ciopts(lcolor(blue) lwidth(medium)) msymbol(D)), ///
	keep(ev_lead3 ev_lead2 ev_lead1 ev_lag0 ev_lag1 ev_lag2 ev_lag3) ///
	vertical ///
	recast(bar) ///
    yline(0, lcolor(gs8)) ///
    xline(4, lcolor(gs8) lpattern(dash)) ///
	legend(position(6) rows(3)) ///
	order(ev_lead3 ev_lead2 ev_lead1 ev_lag0 ev_lag1 ev_lag2 ev_lag3) ///
	coeflabels(ev_lead3="-3" ev_lead2="-2" ev_lead1="-1" ev_lag0="0" ev_lag1="1" ev_lag2="2" ev_lag3="3") ///
	nooffset ///
	noci ///
	graphregion(color(white))
	
	graph export "${overleaf}/notes/Event Study Setup/figures/outcome_decomposition.pdf", as(pdf) replace


	** Export to LaTeX **
	local events "ev_lead3 ev_lead2 ev_lead1 ev_lag0 ev_lag1 ev_lag2 ev_lag3"

	// Create empty matrices to store results
	matrix results = J(7, 9, .)
	matrix colnames results = event_time b_total se_total b_left b_oth b_same pct_left pct_oth pct_same
	local row = 1

	foreach event in `events' {
		// Get column position for this coefficient
		local colnum : list posof "`event'" in events
		
		// Extract coefficients
		scalar b_tot = b_turnover[1, colnumb(b_turnover, "`event'")]
		scalar se_tot = se_turnover[1, colnumb(se_turnover, "`event'")]
		scalar b_l = b_left[1, colnumb(b_left, "`event'")]
		scalar b_o = b_oth[1, colnumb(b_oth, "`event'")]
		scalar b_s = b_same[1, colnumb(b_same, "`event'")]
		
		// Calculate percentages
		scalar pct_l = (b_l / b_tot) * 100
		scalar pct_o = (b_o / b_tot) * 100
		scalar pct_s = (b_s / b_tot) * 100
		
		// Store in results matrix
		matrix results[`row', 1] = `row' - 4  // Event time (-3 to 3)
		matrix results[`row', 2] = b_tot
		matrix results[`row', 3] = se_tot
		matrix results[`row', 4] = b_l
		matrix results[`row', 5] = b_o
		matrix results[`row', 6] = b_s
		matrix results[`row', 7] = pct_l
		matrix results[`row', 8] = pct_o
		matrix results[`row', 9] = pct_s
		
		local row = `row' + 1
	}

	// Display the matrix
	matrix list results

	// Or convert to dataset and export
	clear
	svmat results, names(col)
	format b_* se_* %9.3f
	format pct_* %9.1f

	// Create nice labels
	label var event_time "Event Time"
	label var b_total "Total Turnover"
	label var se_total "SE"
	label var b_left "Left Sample"
	label var b_oth "Other Hospital"
	label var b_same "Same Hospital"
	label var pct_left "% Left"
	label var pct_oth "% Other"
	label var pct_same "% Same"

	// Display
	list, separator(0) noobs
	
	listtex using "${overleaf}/notes/Event Study Setup/tables/decomposition_table.tex", replace ///
	rstyle(tabular) ///
	head("\begin{tabular}{lcccccccc}" ///
		 "\hline" ///
		 "Event Time & Total & SE & Left & Other & Same & \% Left & \% Other & \% Same \\" ///
		 "\hline") ///
	foot("\hline" ///
		 "\end{tabular}")

frame change default
frame drop main_spec
