/* MERGERS_ACQ_DESCRIPTIVES ****************************************************

Program name: 	06_mergers_acq_descriptives.do
Programmer: 	Julia Paris

Goal: 			Look at CEO trends around M&A events

*******************************************************************************/

* setup ________________________________________________________________________ 

* check setup is complete
	check_setup
	
* load data
	use "${dbdata}/derived/temp/indiv_file_contextual.dta", clear

* describe M&A: how often are hospitals being acquired?	
	
* how many FP to NFP / vice-versa transitions are we seeing? 
	* none???
	* check original data
	
* are places with female (MD) CEOs more likely to get acquired next year?
	sum tar if char_female == 1
	sum tar if char_female == 0
// 	logit tar char_female char_md i.gov_priv_type i.year if is_hosp ==1
	* no diff by gender, but places with MD CEOs are slightly more likely to be acquired

* annual hazard of having CEO turnover
	preserve 
		keep if char_ceo ==1
		keep if is_hosp ==1 
		
		sum ceo_turnover1  
		
		bysort entity_uniqueid: egen total_tar = total(tar)
		
		* keep entities that got acquired once 
		keep if total_tar == 1
		
		* how many are there?
		codebook entity_uniqueid
		
		label define short_fp 0 "NFP" 1 "FP"
		label values forprofit short_fp
		
		* make a years relative to tar variable
		bysort entity_uniqueid (year): gen yearnum = _n
		* store value of yearnum for the observation with tar == 1 as "tarnum"
		bysort entity_uniqueid (year): gen tarnum = yearnum if tar == 1
		bysort entity_uniqueid (year): egen max_tarnum = max(tarnum)
		bysort entity_uniqueid (year): replace tarnum = max_tarnum if missing(tarnum)
		* gen years_from_tar = yearnum - tarnum
			* so years_from_tar = 0 if tar == 1
		gen years_from_tar = yearnum - tarnum
		tab years_from_tar if tar ==1 // check
		
		tab years_from_tar, sum(ceo_turnover1)
		
		tab  years_from_tar if forprofit == 1, sum(ceo_turnover1)
		tab  years_from_tar if forprofit == 0, sum(ceo_turnover1)
		
		sum ceo_turnover1 
		sum ceo_turnover1 if tar==1
		
		gen count =1 
		collapse ceo_turnover1 (rawsum) count, by(years_from_tar forprofit)
		
		keep if inrange(years_from_tar,-4,4)
		twoway line ceo_turnover1 year if forprofit == 1, lcolor(green) ///
		|| line ceo_turnover1 year if forprofit == 0, lcolor(blue) ///
		legend(order(1 "Private FP" 2 "Private NFP")) ///
		title("CEO Turnover Rate Relative to Acquisition") ///
		xtitle("Years Relative to Acquisition") ytitle("Share") ///
		xline(0) xlabel(-4(1)4)
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceoturnover_acquired.pdf", as(pdf) name("Graph") replace
		
		graph bar count, over(forprofit) over(year) ///
		title("Count of Hospitals in Sample") ///
		ytitle("Share") ///
		blabel(bar)
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceoturnover_acquired_counts.pdf", as(pdf) name("Graph") replace

	restore
	
* annual hazard of having CEO turnover
	preserve 
		keep if char_ceo ==1
		keep if is_hosp ==1 
		
		sum ceo_turnover1  
		
		bysort entity_uniqueid: egen total_tar = total(tar)
		
		* keep entities that got acquired once 
		keep if total_tar == 1
		
		* how many are there?
		codebook entity_uniqueid
		
		label define short_fp 0 "NFP" 1 "FP"
		label values forprofit short_fp
		
		* make a years relative to tar variable
		bysort entity_uniqueid (year): gen yearnum = _n
		* store value of yearnum for the observation with tar == 1 as "tarnum"
		bysort entity_uniqueid (year): gen tarnum = yearnum if tar == 1
		bysort entity_uniqueid (year): egen max_tarnum = max(tarnum)
		bysort entity_uniqueid (year): replace tarnum = max_tarnum if missing(tarnum)
		* gen years_from_tar = yearnum - tarnum
			* so years_from_tar = 0 if tar == 1
		gen years_from_tar = yearnum - tarnum
		tab years_from_tar if tar ==1 // check
		
		gen prev_female_ceo_yr = years_from_tar == -1 & char_female == 1
		replace prev_female_ceo_yr = . if missing(char_female)
		bysort entity_uniqueid: egen prev_female_ceo = max(prev_female_ceo_yr)
		
		gen count =1 
		tempfile beforecollapse
		save `beforecollapse'
		
		collapse ceo_turnover1 char_female (rawsum) count, by(years_from_tar forprofit prev_female_ceo)
		
		keep if inrange(years_from_tar,-4,4)
		twoway line ceo_turnover1 year if forprofit == 1 & prev_female_ceo == 1, lcolor("8 81 156") ///
		|| line ceo_turnover1 year if forprofit == 0 & prev_female_ceo == 1, lcolor("107 174 214") ///
		|| line ceo_turnover1 year if forprofit == 1 & prev_female_ceo == 0, lcolor("35 139 69") ///
		|| line ceo_turnover1 year if forprofit == 0 & prev_female_ceo == 0, lcolor("116 196 118") ///
		legend(order(1 "Female CEO, Private FP" 2 "Female CEO, Private NFP" 3 "Male CEO, Private FP" 4 "Male CEO, Private NFP")) ///
		title("CEO Turnover Rate Relative to Acquisition") ///
		xtitle("Years Relative to Acquisition") ytitle("Share") ///
		xline(0) xlabel(-4(1)4)
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceoturnover_acquired_byprofitfemale.pdf", as(pdf) name("Graph") replace
		
		twoway line char_female year if forprofit == 1 & prev_female_ceo == 1, lcolor("8 81 156") ///
		|| line char_female year if forprofit == 0 & prev_female_ceo == 1, lcolor("107 174 214") ///
		|| line char_female year if forprofit == 1 & prev_female_ceo == 0, lcolor("35 139 69") ///
		|| line char_female year if forprofit == 0 & prev_female_ceo == 0, lcolor("116 196 118") ///
		legend(order(1 "Female CEO, Private FP" 2 "Female CEO, Private NFP" 3 "Male CEO, Private FP" 4 "Male CEO, Private NFP")) ///
		title("Share Female CEOs Relative to Acquisition") ///
		xtitle("Years Relative to Acquisition") ytitle("Share") ///
		xline(0) xlabel(-4(1)4)
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceosharefemale_acquired_byprofitfemale.pdf", as(pdf) name("Graph") replace
		
		graph bar count if prev_female_ceo==1, over(forprofit) over(year) ///
		title("Count of Hospitals in Sample") ///
		subtitle("Female CEOs Prior to Acquisition") ///
		ytitle("Share") ///
		blabel(bar)
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceoturnover_acquired_counts_fp_prev_female.pdf", as(pdf) name("Graph") replace
		
		graph bar count if prev_female_ceo==0, over(forprofit) over(year) ///
		title("Count of Hospitals in Sample") ///
		subtitle("Male CEOs Prior to Acquisition") ///
		ytitle("Share") ///
		blabel(bar)
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceoturnover_acquired_counts_fp_prev_male.pdf", as(pdf) name("Graph") replace
		
		use `beforecollapse', clear
		
		keep if inrange(years_from_tar,-4,4)
		collapse ceo_turnover1 char_female (rawsum) count, by(years_from_tar prev_female_ceo)
		twoway line ceo_turnover1 year if prev_female_ceo == 1, lcolor("8 81 156") ///
		|| line ceo_turnover1 year if prev_female_ceo == 0, lcolor("35 139 69") ///
		legend(title("CEO Gender in Year t-1") order(1 "Female" 2 "Male")) ///
		title("CEO Turnover Rate Relative to Acquisition") ///
		xtitle("Years Relative to Acquisition") ytitle("Share") ///
		xline(0) xlabel(-4(1)4)
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceoturnover_acquired_byfemale.pdf", as(pdf) name("Graph") replace
		
		graph bar count, over(prev_female_ceo) over(year) ///
		title("Count of Hospitals in Sample") ///
		subtitle("By Female (1) or Male (0) CEO in t-1") ///
		ytitle("Share") ///
		blabel(bar)
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceoturnover_acquired_counts_prev_ceogender.pdf", as(pdf) name("Graph") replace
		

	restore
