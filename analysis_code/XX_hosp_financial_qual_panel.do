/* HOSP_FINANCIAL_QUAL_PANEL ***************************************************

Program name: 	XX_hosp_financial_qual_panel.do
Programmer: 	Julia Paris

Goal: 			Compute descriptive stats for CEOs

*******************************************************************************/

* setup ________________________________________________________________________ 

	check_setup

	* merge in cleaned M&A data for FACILITIES
	use "${dbdata}/derived/temp/merged_ma_sysid_xwalk.dta", clear
		
	* pull in parent profit info
	preserve
		use "${dbdata}/derived/temp/systems_nonharmonized_withprofit.dta", clear 
		keep entity_uniqueid year forprofit
		rename entity_uniqueid entity_uniqueid_parent 
		rename forprofit forprofit_parent
		tempfile sysprofit
		save `sysprofit'
	restore
	merge m:1 entity_uniqueid_parent year using `sysprofit', gen(_merge_profit) keep(1 3) // only unmerged observations are systems plus the 23 ambulatory 
	
* pull in supplemental data ____________________________________________________

	preserve 
		use "${dbdata}/supplemental/hospital-cost-report/hcris_hospyear.dta", clear
		
		rename ayear year
		merge 1:1 pn year using "${dbdata}/supplemental/hospital_compare/mortreadm.dta"
		
		rename pn medicarenumber
		keep if inrange(year,2009,2017)
		
		tab _merge
		drop _merge
		
		tempfile hcris
		save `hcris'
	restore
	
	merge m:1 medicarenumber year using `hcris', gen(_merge_hcris)
	
	* unmerged from HCRIS
		tab year if !missing(medicarenumber) & _merge_hcris == 2
		* fairly even across years, going down over time
	
	* unmerged from our data
	* restrict to hospitals **
	restrict_hosp_sample
		
		* earlier years are worse
		 tab year if !missing(medicarenumber) & _merge_hcris == 1

	* categorize unmerged obs into categories: 
		* missing CCN
		* CCN merges in a diff year but not this one
		* federal 
		
		bysort medicarenumber: egen max_merge = max(_merge_hcris)
		
		gen unmerged_cat = "Missing CCN" if missing(medicarenumber) & _merge_hcris == 1
		replace unmerged_cat = "Federal" if regexm(medicarenumber,"F") & _merge_hcris == 1
		replace unmerged_cat = "Merged in Another Year" if max_merge == 3 & _merge_hcris == 1
		
		
		
		