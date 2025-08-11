/* CEO_DESCRIPTIVES ************************************************************

Program name: 	05_ceo_descriptives.do
Programmer: 	Julia Paris

Goal: 			Compute descriptive stats for CEOs

*******************************************************************************/

* setup ________________________________________________________________________ 

* check setup is complete
	check_setup
	
* load data
	use "${dbdata}/derived/temp/indiv_file_contextual.dta", clear
	
* calculate CEO descriptive statistics _________________________________________

* CEO MD and female shares (ownership variable: gov_priv_type)
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
		
		* combine fed, state and local into one government category
		replace gov_priv_type = 1 if gov_priv_type==2
	
		* make summary stats by hospital time
		collapse char_md char_female count_ceo_roles* (rawsum) count, by(year gov_priv_type)
		drop if missing(gov_priv_type)
		
		* make MD line graph over time		
		twoway line char_md year if gov_priv_type == 1, lcolor(orange) ///
		|| line char_md year if gov_priv_type == 3, lcolor(green) ///
		|| line char_md year if gov_priv_type == 4, lcolor(blue) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP")) ///
		title("Share of Hospitals with MD CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.025)0.1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_md_gov_priv_type.pdf", as(pdf) name("Graph") replace
		
		* make gender line graph over time
		twoway line char_female year if gov_priv_type == 1, lcolor(orange) ///
		|| line char_female year if gov_priv_type == 3, lcolor(green) ///
		|| line char_female year if gov_priv_type == 4, lcolor(blue) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP")) ///
		title("Share of Hospitals with Female CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_female_gov_priv_type.pdf", as(pdf) name("Graph") replace
		
		* make line graph for CEO # roles
		twoway ///
			line count_ceo_roles_1 year if gov_priv_type == 4, lcolor("198 219 239") || ///
			line count_ceo_roles_2 year if gov_priv_type == 4, lcolor("107 174 214") || ///
			line count_ceo_roles_3 year if gov_priv_type == 4, lcolor("8 81 156") || ///
			line count_ceo_roles_1 year if gov_priv_type == 3, lcolor("199 233 192") || ///
			line count_ceo_roles_2 year if gov_priv_type == 3, lcolor("116 196 118") || ///
			line count_ceo_roles_3 year if gov_priv_type == 3, lcolor("35 139 69") || ///
			line count_ceo_roles_1 year if gov_priv_type == 1, lcolor("253 208 162") || ///
			line count_ceo_roles_2 year if gov_priv_type == 1, lcolor("253 141 60") || ///
			line count_ceo_roles_3 year if gov_priv_type == 1, lcolor("217 72 16") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles" ///
						 7 "Govt, 1 Role" ///
						 8 "Govt, 2 Roles" ///
						 9 "Govt, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_roles1_gov_priv_type.pdf", ///
			as(pdf) name("Graph") replace	
	restore

	
* CEO MD and female shares (ownership variable: forprofit)
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hospitals that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		collapse char_md char_female count_ceo_roles* (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make MD line graph over time		
		twoway line char_md year if forprofit == 0, lcolor(blue) ///
		|| line char_md year if forprofit == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share of Hospitals with MD CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.025)0.1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_md_forprofit.pdf", as(pdf) name("Graph") replace
		
		* make gender line graph over time
		twoway line char_female year if forprofit == 0, lcolor(blue) ///
		|| line char_female year if forprofit == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share of Hospitals with Female CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_female_forprofit.pdf", as(pdf) name("Graph") replace
		
		* make line graph for CEO # roles
		twoway ///
			line count_ceo_roles_1 year if forprofit == 0, lcolor("198 219 239") || ///
			line count_ceo_roles_2 year if forprofit == 0, lcolor("107 174 214") || ///
			line count_ceo_roles_3 year if forprofit == 0, lcolor("8 81 156") || ///
			line count_ceo_roles_1 year if forprofit == 1, lcolor("199 233 192") || ///
			line count_ceo_roles_2 year if forprofit == 1, lcolor("116 196 118") || ///
			line count_ceo_roles_3 year if forprofit == 1, lcolor("35 139 69") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" /// 
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_roles1_forprofit.pdf", ///
			as(pdf) name("Graph") replace
	restore
	
* female and MD stats for hosps vs systems
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		tab total_dup
// 		assert total_dup == 2 // 2 hospitals that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		collapse char_md char_female count_ceo_roles* (rawsum) count, by(year forprofit entity_type hospital)
		drop if missing(forprofit)
		
		* make MD line graph over time		
		twoway line char_md year if forprofit == 0 & hospital == 1, lcolor("8 81 156") ///
		|| line char_md year if forprofit == 0 & entity_type=="IDS/RHA", lcolor("107 174 214") ///
		|| line char_md year if forprofit == 1 & hospital==1, lcolor("35 139 69") ///
		|| line char_md year if forprofit == 1 & entity_type=="IDS/RHA", lcolor("116 196 118") ///
		legend(order(1 "Non-Profit Hospital" 2 "Non-Profit System" 3 "For-Profit Hospital" 4 "For-Profit System")) ///
		title("Share with MD CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_md_forprofit_combsys.pdf", as(pdf) name("Graph") replace
		
		* make gender line graph over time
		twoway line char_female year if forprofit == 0 & hospital == 1, lcolor("8 81 156") ///
		|| line char_female year if forprofit == 0 & entity_type=="IDS/RHA", lcolor("107 174 214") ///
		|| line char_female year if forprofit == 1 & hospital==1, lcolor("35 139 69") ///
		|| line char_female year if forprofit == 1 & entity_type=="IDS/RHA", lcolor("116 196 118") ///
		legend(order(1 "Non-Profit Hospital" 2 "Non-Profit System" 3 "For-Profit Hospital" 4 "For-Profit System")) ///
		title("Share with Female CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_female_forprofit_combsys.pdf", as(pdf) name("Graph") replace
		
	restore
	

	
* Count of CEO jobs using AHAID instead of HIMSS
	* gov_priv_type
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
		
		* make unique by person-AHAnum-year
		bysort contact_uniqueid ahanumber year: keep if _n == 1
		
		* combine fed, state and local into one government category
		replace gov_priv_type = 1 if gov_priv_type==2
	
		* make summary stats by hospital time
		collapse count_ceo_roles_aha* (rawsum) count, by(year gov_priv_type)
		drop if missing(gov_priv_type)
	
		* make line graph for CEO # roles
		twoway ///
			line count_ceo_roles_aha_1 year if gov_priv_type == 4, lcolor("198 219 239") || ///
			line count_ceo_roles_aha_2 year if gov_priv_type == 4, lcolor("107 174 214") || ///
			line count_ceo_roles_aha_3 year if gov_priv_type == 4, lcolor("8 81 156") || ///
			line count_ceo_roles_aha_1 year if gov_priv_type == 3, lcolor("199 233 192") || ///
			line count_ceo_roles_aha_2 year if gov_priv_type == 3, lcolor("116 196 118") || ///
			line count_ceo_roles_aha_3 year if gov_priv_type == 3, lcolor("35 139 69") || ///
			line count_ceo_roles_aha_1 year if gov_priv_type == 1, lcolor("253 208 162") || ///
			line count_ceo_roles_aha_2 year if gov_priv_type == 1, lcolor("253 141 60") || ///
			line count_ceo_roles_aha_3 year if gov_priv_type == 1, lcolor("217 72 16") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles" ///
						 7 "Govt, 1 Role" ///
						 8 "Govt, 2 Roles" ///
						 9 "Govt, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			subtitle("Facility ID variable: AHA Number") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_roles1_gov_priv_type_aha.pdf", ///
			as(pdf) name("Graph") replace
	restore
	* FP/NFP
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
		
		* make unique by person-AHAnum-year
		bysort contact_uniqueid ahanumber year: keep if _n == 1
	
		* make summary stats by hospital time
		collapse count_ceo_roles_aha* (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make line graph for CEO # roles
		twoway ///
			line count_ceo_roles_aha_1 year if forprofit == 0, lcolor("198 219 239") || ///
			line count_ceo_roles_aha_2 year if forprofit == 0, lcolor("107 174 214") || ///
			line count_ceo_roles_aha_3 year if forprofit == 0, lcolor("8 81 156") || ///
			line count_ceo_roles_aha_1 year if forprofit == 1, lcolor("199 233 192") || ///
			line count_ceo_roles_aha_2 year if forprofit == 1, lcolor("116 196 118") || ///
			line count_ceo_roles_aha_3 year if forprofit == 1, lcolor("35 139 69") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			subtitle("Facility ID variable: AHA Number") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_roles1_forprofit_aha.pdf", ///
			as(pdf) name("Graph") replace
	restore
	
* Count of CEOs with other c-suite jobs
	* gov_priv_type
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
		
		* combine fed, state and local into one government category
		replace gov_priv_type = 1 if gov_priv_type==2
	
		* make summary stats by hospital time
		collapse count_csuite_roles_* (rawsum) count, by(year gov_priv_type)
		drop if missing(gov_priv_type)
	
		* make line graph for CEO # roles
		twoway ///
			line count_csuite_roles_1 year if gov_priv_type == 4, lcolor("198 219 239") || ///
			line count_csuite_roles_2 year if gov_priv_type == 4, lcolor("107 174 214") || ///
			line count_csuite_roles_3 year if gov_priv_type == 4, lcolor("8 81 156") || ///
			line count_csuite_roles_1 year if gov_priv_type == 3, lcolor("199 233 192") || ///
			line count_csuite_roles_2 year if gov_priv_type == 3, lcolor("116 196 118") || ///
			line count_csuite_roles_3 year if gov_priv_type == 3, lcolor("35 139 69") || ///
			line count_csuite_roles_1 year if gov_priv_type == 1, lcolor("253 208 162") || ///
			line count_csuite_roles_2 year if gov_priv_type == 1, lcolor("253 141 60") || ///
			line count_csuite_roles_3 year if gov_priv_type == 1, lcolor("217 72 16") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles" ///
						 7 "Govt, 1 Role" ///
						 8 "Govt, 2 Roles" ///
						 9 "Govt, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			subtitle("Includes any C-Suite Role") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_csuite_roles1_gov_priv_type.pdf", ///
			as(pdf) name("Graph") replace
	restore
	* FP/NFP
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		collapse count_csuite_roles* (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make line graph for CEO # roles
		twoway ///
			line count_csuite_roles_1 year if forprofit == 0, lcolor("198 219 239") || ///
			line count_csuite_roles_2 year if forprofit == 0, lcolor("107 174 214") || ///
			line count_csuite_roles_3 year if forprofit == 0, lcolor("8 81 156") || ///
			line count_csuite_roles_1 year if forprofit == 1, lcolor("199 233 192") || ///
			line count_csuite_roles_2 year if forprofit == 1, lcolor("116 196 118") || ///
			line count_csuite_roles_3 year if forprofit == 1, lcolor("35 139 69") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			subtitle("Includes any C-Suite Role") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_csuite_roles1_forprofit.pdf", ///
			as(pdf) name("Graph") replace
	restore

	
* turnover, share of hosps with CEO (ownership variable: gov_priv_type)
	preserve
		
		* keep only hospital observations
		keep if hospital ==1 
		
		* keep one observation per hospital-year
		bysort entity_uniqueid year: keep if _n == 1
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		collapse hosp_has_ceo ceo_turnover1 (rawsum) count, by(year gov_priv_type)
		drop if missing(gov_priv_type)
		
		* make "has CEO" line graph over time
		twoway line hosp_has_ceo year if gov_priv_type == 1, lcolor(orange) ///
		|| line hosp_has_ceo year if gov_priv_type == 3, lcolor(green) ///
		|| line hosp_has_ceo year if gov_priv_type == 4, lcolor(blue) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP")) ///
		title("Share of Hospitals With CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0.5(0.1)1) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_has_ceo_gov_priv_type.pdf", as(pdf) name("Graph") replace
		
		* make CEO turnover line graph over time
		merge m:1 year using "${dbdata}/derived/temp/ceoturnover_all"
		twoway line ceo_turnover1 year if gov_priv_type == 1, lcolor(orange) ///
		|| line ceo_turnover1 year if gov_priv_type == 3, lcolor(green) ///
		|| line ceo_turnover1 year if gov_priv_type == 4, lcolor(blue) ///
		|| line ceo_turnover_govpriv year, lcolor(gray) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP" 4 "Aggregate")) ///
		title("Share of Hospitals With CEO Turnover") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.1)0.6) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_ceoturnover_gov_priv_type.pdf", as(pdf) name("Graph") replace
	
	restore	
	
* turnover, share of hosps with CEO (ownership variable: forprofit)
	preserve
	
		* keep only hospital observations
		keep if hospital ==1 
	
		* keep one observation per hospital-year
		bysort entity_uniqueid year: keep if _n == 1
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		collapse hosp_has_ceo ceo_turnover1 (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make "has CEO" line graph over time
		twoway line hosp_has_ceo year if forprofit == 0, lcolor(blue) ///
		|| line hosp_has_ceo year if forprofit == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share of Hospitals With CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0.5(0.1)1) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_has_ceo_forprofit.pdf", as(pdf) name("Graph") replace
		
		* make CEO turnover line graph over time
		merge m:1 year using "${dbdata}/derived/temp/ceoturnover_all"
		twoway line ceo_turnover1 year if forprofit == 0, lcolor(blue) ///
		|| line ceo_turnover1 year if forprofit == 1, lcolor(green) ///
		|| line ceo_turnover_forprofit year, lcolor(gray) ///
		legend(order(1 "Non-Profit" 2 "For-Profit" 3 "Aggregate")) ///
		title("Share of Hospitals With CEO Turnover") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_ceoturnover_forprofit.pdf", as(pdf) name("Graph") replace
	
	restore
	
* share of hospital SYSTEMS with CEO
	preserve
	
		* keep only hospital observations
		keep if entity_type =="IDS/RHA"
	
		* keep one observation per hospital-year
		bysort entity_uniqueid year: keep if _n == 1
		
		* make a count variable
		gen count = 1
		
		* make summary stats by hospital time
		collapse hosp_has_ceo ceo_turnover1 (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make "has CEO" line graph over time
		twoway line hosp_has_ceo year if forprofit == 0, lcolor(blue) ///
		|| line hosp_has_ceo year if forprofit == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share of Hospital Systems With CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0.5(0.1)1) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_sys_has_ceo_forprofit.pdf", as(pdf) name("Graph") replace
		
		* make CEO turnover line graph over time
		merge m:1 year using "${dbdata}/derived/temp/ceoturnover_all"
		twoway line ceo_turnover1 year if forprofit == 0, lcolor(blue) ///
		|| line ceo_turnover1 year if forprofit == 1, lcolor(green) /// || line ceo_turnover_forprofit year, lcolor(gray) ///
		legend(order(1 "Non-Profit" 2 "For-Profit" /* 3 "Aggregate"*/)) ///
		title("Share of Hospital Systems With CEO Turnover") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_sys_ceoturnover_forprofit.pdf", as(pdf) name("Graph") replace
	
	restore
	
* share with CEO at either hosp or system 
	preserve
	
		* keep only hospital observations
		keep if hospital==1
	
		* make a system CEO variable
		gen comb_ceo_prelim = 1 if !missing(contact_uniqueid_parentceo)
		
		* max by year
		bysort entity_uniqueid year: egen sys_ceo = max(comb_ceo_prelim)
	
		* keep one observation per hospital-year
		bysort entity_uniqueid year: keep if _n == 1
		
		* make combined variable
		gen comb_ceo = hosp_has_ceo
		replace comb_ceo = 1 if sys_ceo == 1
		
		* make a count variable
		gen count = 1
		
		* make summary stats by hospital time
		collapse comb_ceo ceo_turnover1 (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make "has CEO" line graph over time
		twoway line comb_ceo year if forprofit == 0, lcolor(blue) ///
		|| line comb_ceo year if forprofit == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share of Hospitals With Facility or System CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0.5(0.1)1) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_comb_has_ceo_forprofit.pdf", as(pdf) name("Graph") replace
	
	restore
	
* episode length descriptives by FP/NFP
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 

		* summarize episode_length
		bysort entity_uniqueid episode_num: keep if _n == 1
		gen count = 1
		collapse episode_length (rawsum) count, by(forprofit episode_type)
		keep if !missing(forprofit)
		
		label define profit_lbl 0 "Non-Profit" 1 "For-Profit"
		label values forprofit profit_lbl
		
		graph bar episode_length, over(episode_type) over(forprofit) ///
			ytitle("Mean Episode Length (Years)") ///
			title("Episode Length by Profit Status and Episode Type") ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_avgepisodelength_forprofit.pdf", as(pdf) name("Graph") replace
			
		graph bar count, over(episode_type) over(forprofit) ///
			ytitle("Count of Episodes") ///
			title("Count of Episodes by Profit Status and Episode Type") ///
			blabel(bar, format(%3.0f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_countepisodetype_forprofit.pdf", as(pdf) name("Graph") replace
		
	restore
	
* episode length descriptives by CEO gender
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 

		* summarize episode_length
		bysort entity_uniqueid episode_num: keep if _n == 1
		gen count = 1
		collapse episode_length (rawsum) count, by(char_female episode_type)
		keep if !missing(char_female)
				
		graph bar episode_length, over(episode_type) over(char_female) ///
			ytitle("Mean Episode Length (Years)") ///
			title("Episode Length by CEO Gender and Episode Type") ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_avgepisodelength_female.pdf", as(pdf) name("Graph") replace
			
		graph bar count, over(episode_type) over(char_female) ///
			ytitle("Count of Episodes") ///
			title("Count of Episodes by CEO Gender and Episode Type") ///
			blabel(bar, format(%3.0f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_countepisodetype_female.pdf", as(pdf) name("Graph") replace
		
	restore
	
* do career transition types vary over time?
preserve 
	tab career_transition_type if hospital ==1, gen(ct)
	collapse (sum) ct*, by(year)
	drop if year <= 2009
	
	twoway                                                         ///
    (line ct1 year, lp(solid)     lw(medthick))                ///
    (line ct2 year, lp(dash)      lw(medthick))                ///
    (line ct3 year, lp(dot)       lw(medthick))                ///
    (line ct4 year, lp(shortdash) lw(medthick))                ///
    (line ct5 year, lp(longdash)  lw(medthick)),              	///
    legend(order(1 "External move" 2 "External promotion"        ///
                 3 "Internal move" 4 "Internal promotion"        ///
                 5 "Lateral move") cols(1) position(6))         ///
    ytitle("Count of transitions")                              ///
    xtitle("Year")                                              ///
    title("Career Transition Distribution by Year")             ///
    graphregion(color(white))
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_byyear.pdf", as(pdf) name("Graph") replace	
restore

* do diff ownership types have different career transition types?
preserve
	tab career_transition_type if hospital ==1, gen(ct)
	collapse (sum) ct*, by(forprofit)
	
	graph bar ct*, asyvars stack over(forprofit) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distribution by Hospital Ownership")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_forprofit.pdf", as(pdf) name("Graph") replace
restore

* career transition types by profit AND gender
preserve
	tab career_transition_type if hospital ==1, gen(ct)
	collapse (sum) ct*, by(forprofit char_female)
	
	graph bar ct* if char_female == 0, asyvars stack over(forprofit) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distribution by Hospital Ownership: Men Only")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_forprofit_men.pdf", as(pdf) name("Graph") replace
	
	graph bar ct* if char_female == 1, asyvars stack over(forprofit) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distribution by Hospital Ownership: Women Only")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_forprofit_women.pdf", as(pdf) name("Graph")replace
restore

* do female and male CEOs have different career transition types?
preserve
	tab career_transition_type if hospital ==1, gen(ct)
	collapse (sum) ct*, by(char_female)
	
	graph bar ct*, asyvars stack over(char_female) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distribution by CEO Gender")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_bygender.pdf", as(pdf) name("Graph") replace
restore

* do MD and non-MD CEOs have different career transition types?
preserve
	tab career_transition_type if hospital ==1, gen(ct)
	collapse (sum) ct*, by(char_md)
	
	graph bar ct*, asyvars stack over(char_md) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distribution by CEO MD Status")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_bymd.pdf", as(pdf) name("Graph") replace
restore

* are men and women CEOs of different size hospitals? 
	cap destring hospbd, replace
	cap destring bdtot, replace
	* these seem pretty much the same
* bdtot
preserve
	* keep CEOs only
		keep if char_ceo == 1
		keep if hospital == 1
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 facilities that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
	* collapse 	
		collapse bdtot, by(char_female forprofit)
		graph bar bdtot, over(char_female) over(forprofit) ///
			ytitle("Count") ///
			title("Mean Bed Count by CEO Gender and Ownership") ///
			subtitle("AHA Variable") ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_bdtot_female_forprofit.pdf", as(pdf) name("Graph") replace
restore
* entity_bedsize
preserve
	* keep CEOs only
		keep if char_ceo == 1
		keep if hospital == 1
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 facilities that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
	* collapse 	
		collapse entity_bedsize, by(char_female forprofit)
		graph bar entity_bedsize, over(char_female) over(forprofit) ///
			ytitle("Count") ///
			title("Mean Bed Count by CEO Gender and Ownership") ///
			subtitle("HIMSS Variable") ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_entity_bedsize_female_forprofit.pdf", as(pdf) name("Graph") replace
restore

* TO-DO: redo first taking the sum by contact_uniqueid and then collapsing
	* take sum of each kind of ownership within a contact_uniqueid
	* graph over gender stacked bars of each 

* how many places are people CEOs at in a given year?
preserve
		* keep CEOs only
		keep if char_ceo == 1
		keep if hospital ==1
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 facilities that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* how many in any year
		gen ct_fac_yr = 1
		collapse (rawsum) ct_fac_yr (max) char_md, by(contact_uniqueid year char_female)
		
		sum ct_fac_yr
		tab char_md, sum(ct_fac_yr )
		tab char_female, sum(ct_fac_yr)
		
restore 

* ceo shares by ownership and gender and year
preserve
	* keep CEOs only
		keep if char_ceo == 1
		keep if hospital ==1
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
	* what share of CEOs at each type?
		sum forprofit 
		gen nonprofit_ps = 1- forprofit
		
	* share of CEOs of each type
	gen count =1 
	collapse forprofit (rawsum) count, by(year char_female)
	
	drop if year <= 2009
	
	* graph bar over gender/year
	twoway ///
    (line forprofit year if char_female == 0, lcolor(blue) lpattern(solid)) ///
    (line forprofit year if char_female == 1, lcolor(red) lpattern(dash)) ///
    , ///
    ytitle("Share") ///
    title("Share of CEOs at For-Profit Facilities by Gender") ///
    legend(order(1 "Male" 2 "Female"))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_forprofit_bygender.pdf", as(pdf) name("Graph") replace
	
restore
	
* different CEO ecosystems?
preserve
	* keep CEOs only
		keep if char_ceo == 1
		keep if hospital == 1
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 facilities that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
	* make CEO fp/nfp variables
	gen ceo_nfp = char_ceo == 1 & forprofit == 0
		replace ceo_nfp = . if missing(forprofit)
	gen ceo_forprofit = char_ceo == 1 & forprofit == 1
		replace ceo_forprofit = . if missing(forprofit)
	
	* make EVER variables
	bysort contact_uniqueid: egen ever_ceo_nfp = max(ceo_nfp)
	bysort contact_uniqueid: egen ever_ceo_forprofit = max(ceo_forprofit)
	
	* make years variable
	gen roles = 1
	
	collapse *ceo_nfp *ceo_forprofit (rawsum) roles (max) char_md, by(contact_uniqueid char_female)
	* TEMP: keep most common
	bysort contact_uniqueid (roles): keep if _n == _N
	
	sum ever_ceo_forprofit if ever_ceo_nfp == 1
	sum ever_ceo_nfp if ever_ceo_forprofit ==1
	
	sum ever_ceo_forprofit if ever_ceo_nfp == 1 & char_female ==1
	sum ever_ceo_forprofit if ever_ceo_nfp == 1 & char_female ==0
	sum ever_ceo_nfp if ever_ceo_forprofit ==1 & char_female ==1
	sum ever_ceo_nfp if ever_ceo_forprofit ==1 & char_female ==0
	
	* make variables for graphing
	gen only_nonprofit_ceo = ever_ceo_forprofit == 0 & ever_ceo_nfp == 1
		replace only_nonprofit_ceo = . if missing(ever_ceo_forprofit) | missing(ever_ceo_nfp) 
	gen only_forprofit_ceo = ever_ceo_forprofit == 1 & ever_ceo_nfp == 0
		replace only_forprofit_ceo = . if missing(ever_ceo_forprofit) | missing(ever_ceo_nfp) 
	gen ever_both_ceo = ever_ceo_forprofit == 1 & ever_ceo_nfp == 1
		replace ever_both_ceo = . if missing(ever_ceo_forprofit) | missing(ever_ceo_nfp) 
		
	tempfile base
	save `base'
	
	* ALL
	collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_nfp
	gen type = "NFP"
	tempfile nfp
	save `nfp'
	
	use `base', clear
	collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_forprofit
	gen type = "For-Profit"
	tempfile forprofit 
	save `forprofit'

	use `base', clear
	collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo
	gen type = "All"
	append using `nfp'
	append using `forprofit'
	
	graph bar only_nonprofit_ceo only_forprofit_ceo ever_both_ceo, over(type) stack ///
		ytitle("Share") ///
		title("Has the CEO Ever Worked in Each Ownership Type?") ///
		legend(order(1 "Only Non-Profit" 2 "Only For-Profit" 3 "Both")) ///
		blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_everceo_forprofit.pdf", as(pdf) name("Graph") replace
	
	* BY GENDER
	use `base', clear
	collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_nfp, by(char_female)
	gen type = "NFP"
	tempfile nfp_female
	save `nfp_female'
	
	use `base', clear
	collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_forprofit, by(char_female)
	gen type = "For-Profit"
	tempfile forprofit_female 
	save `forprofit_female'

	use `base', clear
	collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo, by(char_female)
	gen type = "All"
	append using `nfp_female'
	append using `forprofit_female'
	
	graph bar only_nonprofit_ceo only_forprofit_ceo ever_both_ceo, over(char_female) over(type) stack ///
		ytitle("Share") ///
		title("Has the CEO Ever Worked in Each Ownership Type, by Gender") ///
		legend(order(1 "Only Non-Profit" 2 "Only For-Profit" 3 "Both")) ///
		blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_everceo_forprofit_bygender.pdf", as(pdf) name("Graph") replace
		
	* BY MD
	use `base', clear
	collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_nfp, by(char_md)
	gen type = "NFP"
	tempfile nfp_md
	save `nfp_md'
	
	use `base', clear
	collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_forprofit, by(char_md)
	gen type = "For-Profit"
	tempfile forprofit_md 
	save `forprofit_md'

	use `base', clear
	collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo, by(char_md)
	gen type = "All"
	append using `nfp_md'
	append using `forprofit_md'
	
	cap label define degree_md 0 "No MD" 1 "Has MD"
	cap label values char_md degree_md
	
	graph bar only_nonprofit_ceo only_forprofit_ceo ever_both_ceo, over(char_md) over(type) stack ///
		ytitle("Share") ///
		title("Has the CEO Ever Worked in Each Ownership Type, by CEO MD") ///
		legend(order(1 "Only Non-Profit" 2 "Only For-Profit" 3 "Both")) ///
		blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_everceo_forprofit_bymd.pdf", as(pdf) name("Graph") replace
	
restore
	