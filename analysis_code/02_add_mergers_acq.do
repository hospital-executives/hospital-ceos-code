/* ADD_MERGERS_AQC *****************************************************************

Program name: 	02_add_mergers_acq.do
Programmer: 	Julia Paris

Goal: 			Merge M&A data into hospital-level dataset
				Updated to use the most recent version of the Cooper et al data

*******************************************************************************/

* SETUP ________________________________________________________________________ 

* check setup is complete
	check_setup
	
* PREPARE FOR MERGE  ___________________________________________________________

* load data
	use "${dbdata}/derived/himss_aha_hospitals_final.dta", clear
	
	* make sure no duplicates
	duplicates report
	duplicates tag, gen(dup)
	duplicates drop

	* check merge variable: AHA
	codebook entity_aha
	tab entity_type if !missing(entity_aha)
		* currently 10 IDS/RHA with an entity_aha 
		* 3 single hosp health systems with entity_aha
	
	* keep our own sysid separate
	rename sysid sysid_orig
	
	* pull in SYSTEM information from HIMSS PARENT _____________________________ 
	preserve 
	
		* then tempfile the systems - keep only CEO ID and name, rename to parent
		glob keepvars entity_uniqueid entity_type entity_name campus_aha
		* make unique 
			bysort himss_entityid year: keep if _n == 1
		keep himss_entityid year $keepvars
		foreach var in $keepvars {
			rename `var' `var'_parent
		}
		tostring himss_entityid, gen(entity_parentid)
		drop himss_entityid 
		tempfile parent_obs
		save `parent_obs'
		
	restore
	
	* merge parents onto children
	merge m:1 entity_parentid year using `parent_obs', gen(_merge_parent) keep(1 3)	
	
	* generate system membership variable
	gen sys_member = 1 if entity_type_parent == "IDS/RHA"
	replace sys_member = 0 if entity_type_parent == "Single Hospital Health System"
	
* CHECKING DATA QUALITY 
	
	* how often is the campus_aha missing for is_hosp and single hos health system parent?
	count if is_hospital == 1 & missing(campus_aha) & entity_type_parent == "Single Hospital Health System" // never missing
	
	* how often is (campus)(entity)_aha not the same for is_hosp and single hos health system parent
	gen same_entity_aha = entity_aha == campus_aha_parent if entity_type_parent == "Single Hospital Health System"
	tab same_entity_aha if is_hospital == 1 & entity_type_parent == "Single Hospital Health System" & !missing(campus_aha_parent) // 266 cases where they are different
	drop same_entity_aha
	count if !missing(entity_aha) & !missing(campus_aha) & is_hospital == 1 & entity_type_parent == "Single Hospital Health System" & missing(campus_aha_parent)
	codebook entity_uniqueid if !missing(entity_aha) & !missing(campus_aha) & is_hospital == 1 & entity_type_parent == "Single Hospital Health System" & missing(campus_aha_parent)
	
	* missingness of campus_aha for SHHS
	codebook campus_aha if entity_type == "Single Hospital Health System"
	
* SAVE SYSTEMS FILE - pre-merge since systems shouldn't be merging	
	
	* separate out systems - SAVE AS SEPARATE FILE
	preserve
		keep if regexm(entity_type,"IDS/RHA|Single Hospital Health System")
		keep if inrange(year,2009,2017)
		drop entity_parentid
		tostring himss_entityid, gen(entity_parentid)
		save "${dbdata}/derived/temp/systems_nonharmonized.dta", replace
	restore
	drop if regexm(entity_type,"IDS/RHA|Single Hospital Health System")
	
	* prepare counts for graphing after merge
	preserve
		gen count_entity_aha_all = 1 if !missing(entity_aha)
		gen count_entity_aha_hosp = 1 if !missing(entity_aha) & is_hospital == 1
		gen count_campus_aha_all = 1 if !missing(campus_aha)
		gen count_campus_aha_hosp = 1 if !missing(campus_aha) & is_hospital == 1
		collapse (rawsum) count_entity_aha* count_campus_aha*, by(year)
		tempfile ccn_aha_counts
		save `ccn_aha_counts'
	restore	
	
* load M&A data
	foreach var in entity_aha campus_aha {
		preserve
		
			use "${dbdata}/supplemental/cooper_updated/HI_mergerbase_corr.dta", clear

			destring aha_id, gen(`var') force // 198 missing obs created, all have "A" in the ID
			
			keep if !missing(`var') // eventually want to get rid of this
			
			gen system_id_ma = sysid
			
			* remove leading zeroes from sysid 
			gen sysid_nolead = regexs(1) if regexm(sysid, "^0*(.+)")
			replace sysid = sysid_nolead if !missing(sysid_nolead)
			
			* append campus to all variables if var == campus_aha
			gen strvar = "`var'"
			if strvar == "campus_aha" {
				foreach ma_var of varlist * {
					if "`ma_var'" != "campus_aha" & "`ma_var'" != "year" & "`ma_var'" != "aha_id" {
						rename `ma_var' `ma_var'_campus
					}
				}
			}
			
			tempfile ma_data
			save `ma_data'
			
			* make a graph with counts by year
			gen count_ma = 1
			collapse (rawsum) count_ma, by(year)
			merge 1:1 year using `ccn_aha_counts'
			keep if inrange(year,2009,2017)
			graph bar count_ma count_`var'_all, over(year) ///
				legend(position(bottom) label(1 "M&A Data") label(2 "AHA/HIMSS Hospitals")) ///
				title("Count of Observations by Year: `var'") ///
				blabel(bar, size(vsmall))
			graph export "${overleaf}/notes/M&A Merge/figures/counts_`var'_ma_byyear.pdf", as(pdf) name("Graph") replace
		
		restore
	
* merge
		merge m:1 `var' year using `ma_data', gen(_merge_`var')
	}
	
* keep overlapping date ranges
	keep if inrange(year,2009,2017)
	
* what percent of observations from the M&A data never match to either?
	preserve
		keep if _merge_campus_aha == 2 // unmerged from M&A on campus
		keep aha_id year 
		tempfile unmerged_campus 
		save `unmerged_campus'
	restore
	preserve
		keep if _merge_entity_aha == 2 // unmerged from M&A on campus
		keep aha_id year 
		merge 1:1 aha_id year using `unmerged_campus', keep(3) 
		* unmerged from BOTH
		tab year if _merge == 3
		count if _merge == 3
		codebook aha_id if _merge == 3
		* unmerged on entity_aha but does merge on campus_aha: _merge == 1
	restore
	
	* format and export merge results
	label define mergelab 1 "Unmerged from AHA/HIMSS" 2 "Unmerged from M\&A" 3 "Merged"
	foreach var in entity_aha campus_aha {
		label values _merge_`var' mergelab
		export_merge, folder("M&A Merge") filename(merge1_tab_`var') target(_merge_`var')
		
		gen cleaned_merge_`var' = _merge_`var' if !missing(`var')
		label values cleaned_merge_`var' mergelab
		export_merge, folder("M&A Merge") filename(merge1_tab_`var'_cleaned) target(cleaned_merge_`var')
	} 
	
	* count of hospital observations that should probably merge
	count if _merge_entity_aha == 1 ///
		& !missing(entity_aha) ///
		& inlist(type,"General Medical & Surgical", "General Medical", "Critical Access")
	
	drop if _merge_campus_aha == 2 | _merge_entity_aha == 2 
	

	* remove trailing zeroes and sort by sysid?


* MISC DATA CLEANING ___________________________________________________________

	codebook medicarenumber
	* seems like the data have inconsistent medicarenumber formats; occasionally missing leading zeroes.
		* this will add leading zeroes to short observations
		gen ccn_6 = substr("000000" + medicarenumber, -6, .)
		replace ccn_6 = "" if ccn_6 == "000000"
		replace medicarenumber = ccn_6 if length(medicarenumber) < 6
		drop ccn_6	


* SAVE _________________________________________________________________________ 

* save merged file
	save "${dbdata}/derived/temp/merged_ma_nonharmonized.dta", replace
		

* TEST MERGE RESULTS ___________________________________________________________ 

	* what aha IDs merge on campus but not on entity?
	preserve
		keep if _merge_campus_aha == 3
		bysort campus_aha: keep if _n == 1
		keep campus_aha
		rename campus_aha aha
		tempfile campus_merged
		save `campus_merged'
	restore
	preserve
		keep if _merge_entity_aha == 3
		bysort entity_aha: keep if _n == 1
		keep entity_aha
		rename entity_aha aha
		merge 1:1 aha using `campus_merged'
		keep if _merge == 2
		drop _merge
		rename aha campus_aha
		tempfile merge_campus_only
		save `merge_campus_only'
	restore
	merge m:1 campus_aha using `merge_campus_only', gen(_merge_campus_only)
	br if _merge_campus_only == 3
	
	count if !missing(entity_aha) & entity_type == "Hospital" & type ==  "General Medical & Surgical" & !regexm(medicarenumber, "F") & _merge_entity_aha == 1
	