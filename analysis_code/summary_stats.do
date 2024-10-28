
/* ============================================================================================================
	Name of file: 	summary_stats.do
	
	Purpose:	    Preliminary data analysis 
	
	Files Used:		$DERIVED_DATA/final_confirmed_new.dta
	
============================================================================================================ */

* ======================================================
		* SET UP.
* ======================================================

	capture log close
	clear
	clear matrix
	clear mata
	set maxvar 10000
	set more off

	
***Before you start:
*1: open terminal
*2: navigate to your local version of the repo
*3: run "make compile_himss"
*In doing so, this will generate a custom configuration file with shortcuts


* Include the custom configuration file
*do "config_stata.do"
do "/Users/ambarlaforgia/hospital-ceos-code/analysis_code/config_stata.do"

* Now you have the following globals to use as shortcuts to the DropBox:


* ======================================================
		* DATA READIN & SAMPLE RESTRICTIONS
* ======================================================

* DERIVED_DATA,RAW_DATA, SUPPLEMENTAL_DATA, AUXILIARY_DATA, NICKNAMES_DICTIONARIES 

use "$DERIVED_DATA/final_confirmed_new.dta", clear


*Organize
order contact_uniqueid year entity_uniqueid system_id full_name firstname lastname title title_standardized c_suite entity_name type entity_profitstatus cntrl
sort contact_uniqueid year 

*Only keep hospitals for now
keep if entity_type==("Hospital")


*Only keep if ever c_suite 
bys contact_uniqueid: egen max=max(c_suite)
keep if max==1
drop max



* ======================================================
		* CREATE/CLEAN KEY VARIABLES.
* ======================================================

*Core C-suite /// COO seems to be incomplete -- could also be head of operations or head of facility

gen CEO=(regexm(title_standardized, "CEO:"))

gen CFO=(regexm(title_standardized, "CFO:"))

gen COO=(regexm(title_standardized, "COO:"))

gen CIO=(regexm(title_standardized, "CIO:"))

*Type of hospital

preserve
	keep year entity_uniqueid entity_name entity_address entity_zip aha_entity_name aha_entity_address cntrl
	duplicates drop 
	
	sort entity_uniqueid year
	bys entity_uniqueid: egen mean=mean(cntrl)
	bys entity_uniqueid: egen sd=sd(cntrl)

	*Easy fixes 
	bys entity_uniqueid: replace cntrl=mean if sd==0
	
	*Gen types 
	gen govt=(cntrl==12 | cntrl==13 | cntrl==14 | cntrl==15 | cntrl==16 | (cntrl>40 & cntrl!=.))
	gen nfp=(cntrl==21 | cntrl==23)
	gen fp=(cntrl==31 | cntrl==32 | cntrl==33)
	
	gen entity_cntrl=3 if govt==1
	replace entity_cntrl=1 if nfp==1 
	replace entity_cntrl=2 if fp==1
	
	label define entity_cntrl_lbl 1 "Not-for-Profit" 2 "For-Profit" 3 "Government"
	label values entity_cntrl entity_cntrl_lbl
	
	*Cases with multiple cntrl types for same hosp 
	drop mean sd 
	
	bys entity_uniqueid: egen sd=sd(entity_cntrl)
	bys entity_uniqueid: egen mean=mean(entity_cntrl)
	
	bys entity_uniqueid: replace entity_cntrl=mean if (sd==.| sd==0) & entity_cntrl==.
	
	tsset entity_uniqueid year
	
	replace entity_cntrl=entity_cntrl[_n+1] if entity_cntrl[_n-1] == entity_cntrl[_n+1] & entity_cntrl==.
	
	drop sd mean 
	bys entity_uniqueid: egen sd=sd(entity_cntrl)
	bys entity_uniqueid: egen mean=mean(entity_cntrl)
	
	gen cntrl_unknown=(sd==. & mean==.)
	gen cntrl_switch=(sd>0 & sd!=.)
	
	/// 393 unknown
	unique entity_uniqueid if cntrl_unknown==1 
	
	/// 485 switchers (may be valid or the result of an error)
	unique entity_uniqueid if cntrl_switch==1  
	
	keep entity_uniqueid year entity_cntrl cntrl_unknown cntrl_switch
	duplicates drop
	
	tempfile status
	save `status'
restore	

	merge m:1 entity_uniqueid year using `status'
	assert _merge!=2
	drop _merge
	

* ======================================================
		* SUMMARY STATS.
* ======================================================


*Basic characterisitcs 

	*Share of hosp w/c-suite roles	
	
foreach aa in CEO CFO CIO COO{ 	
	
	preserve 
	
		bysort year entity_uniqueid: egen any_`aa'_hosp = max(`aa')
		
		collapse (max) any_`aa'_hosp, by(year entity_uniqueid entity_cntrl)
		
		egen total_hosp = count(entity_uniqueid), by(year entity_cntrl)
		egen total_with_`aa' = total(any_`aa'_hosp), by(year entity_cntrl)
	
		bys entity_cntrl: gen share_`aa' = total_with_`aa' / total_hosp
		
		egen total_hosp2 = count(entity_uniqueid), by(year )
		egen total_with_`aa'2 = total(any_`aa'_hosp), by(year )
	
		bys entity_cntrl: gen total_share = total_with_`aa'2 / total_hosp2
		
		
		keep year share_`aa' total_share entity_cntrl
		duplicates drop 
		
		sort entity_cntrl year

		qui twoway (line share_`aa' year if entity_cntrl == 1, lcolor(blue) lpattern(solid)) (line share_`aa' year if entity_cntrl == 2, lcolor(red) lpattern(dash)) (line share_`aa' year if entity_cntrl == 3, lcolor(green) lpattern(shortdash)) (line total_share year if entity_cntrl == 1, lcolor(black) lpattern(longdash_dot)), legend(order(1 "Not-for-Profit" 2 "For-Profit" 3 "Government" 4 "All Hospitals") ring(0)) xlabel(2009(1)2017) ylabel(0.6(0.1)1) title("`aa' Share Over Time by Hospital Ownership") ytitle("`aa' Share") xtitle("Year")  graphregion(fcolor(white)) nodraw

		graph export "$OUTPUT/share_with`aa'.jpg", replace
		
		drop if entity_cntrl==. 

		collapse (mean) share_`aa' total_share, by(entity_cntrl)
		
		
		tempfile share_`aa'
		save `share_`aa''
	restore
}	


		
	*CEO tenure
	
	*Will assume that every hospital should have a CEO in each year of the data that they are in the sample 
	preserve 
		gen n=1
		collapse (mean) n , by(year entity_uniqueid)
		
		bys entity_uniqueid: egen year_count=sum(n)
		
		keep entity_uniqueid year_count
		duplicates drop 
		
		tempfile year_count
		save `year_count'
	restore 
	
	merge m:1 entity_uniqueid using `year_count'
	assert _merge!=2
	
	drop _merge
	
	preserve 
	
		bys entity_uniqueid: egen ever_ceo=max(CEO)
		keep if ever_ceo==1
		keep year entity_uniqueid contact_uniqueid CEO year_count
		duplicates drop 
		
		sort entity_uniqueid year 
		
		
	*Avg. CEO Tenure (avg number of years a unique person is CEO at the hospital-level)
	*Ex. if from 2009-2017 a hospital has 3 CEOs, one CEO for 2 years, one CEO for five years, and one CEO for 2 years, then the average CEO tenure is 3 years (9/3)
	
	*Weighted Avg. CEO tenure (same as above but weighted by number of years the CEO was in the position)
	*Weighted<Avg if a hospital has may CEOs w/short tenures
	
		gen n=1
		
		collapse (sum) n, by(entity_uniqueid contact_uniqueid year_count CEO)
		
		bys entity_uniqueid: egen tot_ceo=sum(CEO)
		
		bys entity_uniqueid: gen avg_tenure=year_count/tot_ceo
		

		gen n2 = (n^2)/9 if CEO==1
		bys entity_uniqueid: egen avg_tenure_weight = total(n2) if CEO==1
		
		keep entity_uniqueid avg_tenure avg_tenure_weight
		keep if avg_tenure_weight!=.
		duplicates drop 
		
		tempfile tenure
		save `tenure'
	restore
	
	merge m:1 entity_uniqueid using `tenure'
	assert _merge!=2
	
	drop _merge
	
	
	preserve
		keep entity_cntrl entity_uniqueid avg_*
		duplicates drop 
		
		collapse (mean) avg_tenure avg_tenure_weight, by(entity_cntrl)
		egen tot_avg_tenure=mean(avg_tenure)
		egen tot_avg_tenure_weight=mean(avg_tenure_weight)
	
		tempfile avg_tenure
		save `avg_tenure'
	restore	
		

	*CEO turnover
	**Annual Turnover Rate**: Calculate the proportion of hospitals that experienced a change in CEO each year.
	
	preserve
		bys entity_uniqueid: egen ever_ceo=max(CEO)
		keep if ever_ceo==1
		keep year entity_uniqueid entity_cntrl contact_uniqueid CEO year_count
		duplicates drop 
		
		sort entity_uniqueid year 
		
		bys entity_uniqueid year: egen min=min(CEO)
		bys entity_uniqueid year: egen max=max(CEO)
		
		bys entity_uniqueid year: gen count=_n if min==0 & max==0
		
		keep if CEO==1 | count==1
		
		drop min max count
		
		
		* Step 1: Sort Data
		sort entity_uniqueid year

		* Step 2: Create Lagged Variables
		by entity_uniqueid (year), sort: gen ceo_lag = contact_uniqueid[_n-1]
		by entity_uniqueid (year), sort: gen CEO_lag = CEO[_n-1]

		* Step 3: Identify CEO Changes
		gen ceo_change = .

		* Case 1: Both years have CEO==1
		replace ceo_change = 0 if CEO == 1 & CEO_lag == 1 & contact_uniqueid == ceo_lag
		replace ceo_change = 1 if CEO == 1 & CEO_lag == 1 & contact_uniqueid != ceo_lag

		* Case 2: CEO leaves, position becomes vacant
		replace ceo_change = 1 if CEO == 0 & CEO_lag == 1

		* Case 3: CEO position filled after vacancy
		replace ceo_change = 1 if CEO == 1 & CEO_lag == 0

		* Case 4: Both years have no CEO
		replace ceo_change = 0 if CEO == 0 & CEO_lag == 0

		* Handle missing data (first year of each hospital)
		replace ceo_change = . if missing(CEO_lag)

		* Step 4: Calculate Annual Turnover Rate
		by year, sort: egen num_hospitals = count(ceo_change)
		by year: egen num_ceo_changes = total(ceo_change)
		gen turnover_rate = num_ceo_changes / num_hospitals
		
		****By entity
		bys year entity_cntrl: egen num_hospitals2 = count(ceo_change)
		bys year entity_cntrl: egen num_ceo_changes2 = total(ceo_change)
		gen turnover_rate_entity = num_ceo_changes2 / num_hospitals2
		
		
		keep year entity_cntrl turnover_rate_entity turnover_rate

		qui twoway (line turnover_rate_entity year if entity_cntrl == 1, lcolor(blue) lpattern(solid)) (line turnover_rate_entity year if entity_cntrl == 2, lcolor(red) lpattern(dash)) (line turnover_rate_entity year if entity_cntrl == 3, lcolor(green) lpattern(shortdash)) (line turnover_rate year if entity_cntrl == 1, lcolor(black) lpattern(longdash_dot)), legend(order(1 "Not-for-Profit" 2 "For-Profit" 3 "Government" 4 "All Hospitals") ring(0)) xlabel(2009(1)2017) ylabel(0(0.05).3) title("CEO Turnover Rate") ytitle("CEO Turnover Rate") xtitle("Year")  graphregion(fcolor(white)) nodraw

		graph export "$OUTPUT/CEO_turnover.jpg", replace
		
		drop if entity_cntrl==. 

		collapse (mean) turnover_rate_entity turnover_rate, by(entity_cntrl)
		
		
		tempfile turnover
		save `turnover'
	restore
		
		
	*View sumstats for now 	
		use `share_CEO', clear
		append using `avg_tenure'
		append using `turnover'
		
		x
	
	restore 
	x
	
	



