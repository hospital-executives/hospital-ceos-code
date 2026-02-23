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
	
* pull in Sacarny HCRIS and Hospital Compare data ______________________________

	preserve 
		use "${dbdata}/supplemental/hospital-cost-report/hcris_hospyear.dta", clear
		
		rename ayear year
		merge 1:1 pn year using "${dbdata}/supplemental/hospital_compare/mortreadm.dta", gen(_merge_hospcomp)
		
		rename pn medicarenumber
		keep if inrange(year,2009,2017)
		
		tab _merge_hospcomp
		
		tempfile hcris
		save `hcris'
	restore
	
	merge m:1 medicarenumber year using `hcris', gen(_merge_hcris)
	
	* unmerged from HCRIS
	tab year if !missing(medicarenumber) & _merge_hcris == 2
		* fairly even across years, going down over time
	
	* unmerged from our data
	restrict_hosp_sample // restrict to hospitals
		
		* earlier years are worse
		 tab year if !missing(medicarenumber) & _merge_hcris == 1

	* categorize unmerged obs into categories: 
		* missing CCN
		* CCN merges in a diff year but not this one
		* federal 
		
		bysort medicarenumber: egen max_merge = max(_merge_hcris)
		
		preserve
			gen unmerged_cat = "Missing CCN" if missing(medicarenumber) & _merge_hcris == 1
			replace unmerged_cat = "Federal" if regexm(medicarenumber,"F") & _merge_hcris == 1
			replace unmerged_cat = "Merged in Another Year" if max_merge == 3 & _merge_hcris == 1
			
			gen count =1 
			
			collapse (rawsum) count, by(_merge_hcris unmerged_cat year)
		restore
		
	* make merge flags
	gen not_in_hcris_flag = _merge_hcris == 1 | (_merge_hcris == 3 & _merge_hospcomp == 2)
	gen not_in_hospcomp_flag =_merge_hcris == 1 | (_merge_hcris == 3 & _merge_hospcomp == 1)
	
	* drop observations that aren't in our data
	drop if _merge_hcris == 2
		
	* make profit margin variable
	gen hosp_profitmargin = (income - totcost)/income
	gen rev_per_inpatient = income / ipdischarges_adultped
	gen iprev_per_inpatient = iptotrev / ipdischarges_adultped
	gen cost_per_inpatient = totcost / ipdischarges_adultped
	
	* winsorize variables at 5% on each side
	foreach var in hosp_profitmargin rev_per_inpatient cost_per_inpatient {
		winsor2 `var', cuts(5 95) replace
	}
	
* add HCAHPS ________________________________________________________________________

	preserve
		use "${dbdata}/supplemental/HCAPHS/hcahps_all_cleaned.dta", clear

		tostring provider_number, gen(medicarenumber)
		gen ccn_6 = substr("000000" + medicarenumber, -6, .)
				replace ccn_6 = "" if ccn_6 == "000000"
				replace medicarenumber = ccn_6 if length(medicarenumber) < 6
				drop ccn_6	
				
		keep if inrange(year,2009,2017)
		
		tempfile hcahps
		save `hcahps'
			
	restore
	
	merge m:1 medicarenumber year using `hcahps', gen(_merge_hcahps)
	
	* make a flag for missing from HCAHPS
	gen not_in_hcahps_flag = _merge_hcahps == 1

	* drop observations that aren't in our data
	drop if _merge_hcahps == 2
		
	* make list of variables to keep 
	glob keepvars_origfile "entity_uniqueid year medicarenumber aha_fte"
	glob keepvars_sacarny "hosp_profitmargin ami_readm_rate hf_readm_rate pn_readm_rate ami_mort_rate hf_mort_rate pn_mort_rate not_in_hcris_flag not_in_hospcomp_flag"
	glob keepvars_hcahps "topbox_overall topbox_recommend not_in_hcahps_flag"
		
	keep $keepvars_origfile $keepvars_sacarny $keepvars_hcahps 
	
	* check sparseness of dataset
	egen n_nonmiss = rownonmiss(aha_fte ami_mort_rate hf_mort_rate pn_mort_rate ami_readm_rate hf_readm_rate pn_readm_rate hosp_profitmargin topbox_overall topbox_recommend)
	tab n_nonmiss // currently 34% missing 3 or more variables, while ~40% complete

* testing merge ________________________________________________________________________

exit

* looks like some weirdness with CCNs accounts for the problems with merge for 2009 and 2010:
use "${dbdata}/derived/himss_aha_hospitals_final.dta", clear
restrict_hosp_sample
bysort entity_uniqueid (year): gen switch2011 = 1 if (medicarenumber[_n] !=  medicarenumber[_n -1] & year == 2011 & _n != 1)
bysort entity_uniqueid: egen evswitch2011 = max(switch2011)
tab switch2011 // this is the ~400 obs that don't merge 
tab evswitch2011

* compare to other years: 
use "${dbdata}/derived/himss_aha_hospitals_final.dta", clear
restrict_hosp_sample
forval i = 2010/2017 {
	bysort entity_uniqueid (year): gen switch`i' = 1 if (medicarenumber[_n] !=  medicarenumber[_n -1] & year == `i' & _n != 1)
	bysort entity_uniqueid: egen evswitch`i' = max(switch`i')
	tab switch`i' // this is the ~400 obs that don't merge 
}

* see "tab year if !missing(medicarenumber) & _merge_hcris == 1" above

		