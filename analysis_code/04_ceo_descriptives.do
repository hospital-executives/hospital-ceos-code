/* CEO_DESCRIPTIVES ************************************************************

Program name: 	04_ceo_descriptives.do
Programmer: 	Julia Paris

Goal: 			Merge the facility-level M&A data onto individual-level data
				Compute descriptive stats for CEOs
				Identify CEO turnover

*******************************************************************************/

* check setup is complete
	check_setup

* merge M&A information onto individual file ___________________________________

* load data
	use "${dbdata}/derived/final_confirmed_aha_update_530.dta", clear
	
	* which observations have a missing entity_uniqueid?
	******
		* numeric AHA number
		preserve
			use "${dbdata}/supplemental/hospital_ownership.dta", clear
			destring ahaid_noletter, replace
			rename ahaid_noletter ahanumber
			bysort ahanumber year: drop if _n>1 // rare, but make unique
			* rename profit variables to make source clear
			foreach profvar in forprofit forprofit_lag forprofit_chng gov_priv_type {
				rename `profvar' `profvar'_ps
			}
			keep *_ps ahanumber year
			tempfile ellie_xwalk
			save `ellie_xwalk'
		restore
	merge m:1 ahanumber year using `ellie_xwalk', keep(1 3) nogen
	tab gov_priv_type_ps if missing(entity_uniqueid), m
	*  383,267       57.73% private NP
	*  280,624       42.27% missing gov_priv_type variable
	
	drop forprofit_ps forprofit_lag_ps forprofit_chng_ps gov_priv_type_ps
	******
	
* merge in M&A data	
	merge m:1 entity_uniqueid year using "${dbdata}/derived/temp/merged_ma_sysid_xwalk.dta", gen(_merge_ma_xwalk) keep(1 3)
	* TEMPORARY:
		keep if inrange(year,2010,2017)
		* matched to all of the hospitals with a non-missing entity_uniqueid
		keep if !missing(entity_uniqueid)
		
* create variables for descriptive statistics __________________________________		
		
* individual has an MD
	gen char_md = regexm(credentials,"MD|md")
	
* individual is female
	gen char_female = gender=="F"
		replace char_female = . if missing(gender)
		
* hospital has CEO
	* individual
	gen char_ceo = regexm(title_standardized,"CEO")
	* hospital
	bysort entity_uniqueid year: egen hosp_has_ceo = max(char_ceo)	
	
* make CEO turnover variable ___________________________________________________ 

	preserve 
	
		* keep only CEOs
		keep if char_ceo == 1
		
		* check that observations are unique by hospital-year
		bysort entity_uniqueid year: gen count = _N
		assert count == 1
		drop count
 
		* initial version: any change in the CEO from year to year
		bysort entity_uniqueid (year): gen ceo_turnover1 = contact_uniqueid != contact_uniqueid[_n-1] if _n > 1
		
		keep entity_uniqueid year ceo_turnover1
		
		tempfile ceo_turnover1_xwalk
		save `ceo_turnover1_xwalk'
		
		collapse ceo_turnover1, by(year)
		rename ceo_turnover1 ceo_turnover1_all
		tempfile ceoturnover_all
		save `ceoturnover_all'
		
	restore	
	
* merge turnover stats back into main sample
	merge m:1 entity_uniqueid year using `ceo_turnover1_xwalk', gen(_merge_turnover_xwalk)
	
* calculate CEO descriptive statistics _________________________________________

* CEO MD and female shares (ownership variable: gov_priv_type_ps)
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		
		* make a count variable
		gen count = 1
		
		* combine fed, state and local into one government category
		replace gov_priv_type_ps = 1 if gov_priv_type_ps==2
	
		* make summary stats by hospital time
		collapse char_md char_female (rawsum) count, by(year gov_priv_type_ps)
		drop if missing(gov_priv_type_ps)
		
		* make MD line graph over time		
		twoway line char_md year if gov_priv_type_ps == 1, lcolor(orange) ///
		|| line char_md year if gov_priv_type_ps == 3, lcolor(green) ///
		|| line char_md year if gov_priv_type_ps == 4, lcolor(blue) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP")) ///
		title("Share with MD CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.025)0.1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_md_gov_priv_type.pdf", as(pdf) name("Graph") replace
		
		* make gender line graph over time
		twoway line char_female year if gov_priv_type_ps == 1, lcolor(orange) ///
		|| line char_female year if gov_priv_type_ps == 3, lcolor(green) ///
		|| line char_female year if gov_priv_type_ps == 4, lcolor(blue) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP")) ///
		title("Share with Female CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_female_gov_priv_type.pdf", as(pdf) name("Graph") replace
		
	restore
	
* CEO MD and female shares (ownership variable: forprofit_ps)
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		collapse char_md char_female (rawsum) count, by(year forprofit_ps)
		drop if missing(forprofit_ps)
		
		* make MD line graph over time		
		twoway line char_md year if forprofit_ps == 0, lcolor(blue) ///
		|| line char_md year if forprofit_ps == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share with MD CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.025)0.1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_md_forprofit.pdf", as(pdf) name("Graph") replace
		
		* make gender line graph over time
		twoway line char_female year if forprofit_ps == 0, lcolor(blue) ///
		|| line char_female year if forprofit_ps == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share with Female CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_female_forprofit.pdf", as(pdf) name("Graph") replace
		
	restore
	
* turnover, share of hosps with CEO (ownership variable: gov_priv_type_ps)
	preserve
		* keep one observation per hospital-year
		bysort entity_uniqueid year: keep if _n == 1
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		collapse hosp_has_ceo ceo_turnover1 (rawsum) count, by(year gov_priv_type_ps)
		drop if missing(gov_priv_type_ps)
		
		* make "has CEO" line graph over time
		twoway line hosp_has_ceo year if gov_priv_type_ps == 1, lcolor(orange) ///
		|| line hosp_has_ceo year if gov_priv_type_ps == 3, lcolor(green) ///
		|| line hosp_has_ceo year if gov_priv_type_ps == 4, lcolor(blue) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP")) ///
		title("Share of Hospitals With CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0.5(0.1)1) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_has_ceo_gov_priv_type.pdf", as(pdf) name("Graph") replace
		
		* make CEO turnover line graph over time
		merge m:1 year using `ceoturnover_all'
		twoway line ceo_turnover1 year if gov_priv_type_ps == 1, lcolor(orange) ///
		|| line ceo_turnover1 year if gov_priv_type_ps == 3, lcolor(green) ///
		|| line ceo_turnover1 year if gov_priv_type_ps == 4, lcolor(blue) ///
		|| line ceo_turnover1_all year, lcolor(gray) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP" 4 "Aggregate")) ///
		title("Share of Hospitals With CEO Turnover") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.1)0.6) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_ceoturnover_gov_priv_type.pdf", as(pdf) name("Graph") replace
	
	restore	
	
* turnover, share of hosps with CEO (ownership variable: forprofit_ps)
	preserve
		* keep one observation per hospital-year
		bysort entity_uniqueid year: keep if _n == 1
		
		* make a count variable
		gen count = 1
		
		* combine fed, state and local into one government category
		replace gov_priv_type_ps = 1 if gov_priv_type_ps==2
	
		* make summary stats by hospital time
		collapse hosp_has_ceo ceo_turnover1 (rawsum) count, by(year forprofit_ps)
		drop if missing(forprofit_ps)
		
		* make "has CEO" line graph over time
		twoway line hosp_has_ceo year if forprofit_ps == 0, lcolor(blue) ///
		|| line hosp_has_ceo year if forprofit_ps == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share of Hospitals With CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0.5(0.1)1) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_has_ceo_forprofit.pdf", as(pdf) name("Graph") replace
		
		* make CEO turnover line graph over time
		merge m:1 year using `ceoturnover_all'
		twoway line ceo_turnover1 year if forprofit_ps == 0, lcolor(blue) ///
		|| line ceo_turnover1 year if forprofit_ps == 1, lcolor(green) ///
		|| line ceo_turnover1_all year, lcolor(gray) ///
		legend(order(1 "Non-Profit" 2 "For-Profit" 3 "Aggregate")) ///
		title("Share of Hospitals With CEO Turnover") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_ceoturnover_forprofit.pdf", as(pdf) name("Graph") replace
	
	restore
	