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
