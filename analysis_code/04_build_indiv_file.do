/* BUILD_INDIV_FILE ************************************************************

Program name: 	04_build_indiv_file.do
Programmer: 	Julia Paris

Goal: 			Merge the facility-level M&A data onto individual-level data
				Identify CEO turnover and categorize into career transition types

*******************************************************************************/

* check setup is complete
	check_setup

* merge M&A information onto individual file ___________________________________

* load data
	use "${dbdata}/derived/final_confirmed_aha.dta", clear
	
	* is this dataset unique by unique_entityid contact_uniqueid year title?
	duplicates tag entity_uniqueid contact_uniqueid year title_standardized, gen(dup_tag)
	assert dup_tag == 0
	drop dup_tag
	
* merge in cleaned M&A data for FACILITIES
	merge m:1 entity_uniqueid year using "${dbdata}/derived/temp/merged_ma_sysid_xwalk.dta", gen(_merge_ma_xwalk) keep(1 3)
	* there are still 178,980 observations in the final_confirmed dataset that should be merging
	* DOUBLE CHECK NONE ARE MISSING ID:	
		keep if !missing(entity_uniqueid)
		
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
	
* pull in contextual data for parents - specifically want profit data
	merge m:1 entity_uniqueid year using "${dbdata}/derived/temp/systems_nonharmonized_withprofit.dta", keep(1 3) gen(_merge_newprofit) keepusing(forprofit_imputed)
	replace forprofit = forprofit_imputed if _merge_newprofit == 3
	drop _merge_newprofit
	
* MAKE A SYSTEM-LEVEL VARIABLE USING HIMSS PARENT
	gen parent_system = entity_uniqueid_parent 
	replace parent_system = entity_uniqueid if missing(entity_parentid)

* remove "CIO Reports to" as a role
 	drop if title_standardized == "CIO Reports to" 		
		
* create variables for descriptive statistics __________________________________		
		
* individual has an MD
	gen char_md = regexm(credentials,"MD|md")
	* clean some moments where one obs has the MD but another doesn't -> assume if ever MD, then always MD by year
	bysort contact_uniqueid year: egen any_md_year = max(char_md)
	replace char_md = any_md_year if !missing(any_md_year)
		label define degree_md 0 "No MD" 1 "Has MD"
		label values char_md degree_md
	
* individual is female
	* assign modal gender 
	bysort contact_uniqueid gender: gen gender_count = _N
	bysort contact_uniqueid gender_count: gen gender_modal = gender[_N]
	* make indicator based on modal gender
	gen char_female = gender_modal=="F"
		replace char_female = . if missing(gender_modal)
	* replace values with non-modal gender if modal gender is missing
	gen gender_thisyear = gender == "F"
		replace gender_thisyear = . if missing(gender)
	bysort contact_uniqueid: egen gender_ever = max(gender_thisyear)
	replace char_female = gender_ever if !missing(gender_ever) & missing(char_female)
		label define gender_female 0 "Male" 1 "Female"
		label values char_female gender_female
	drop gender_*

* identify hospitals
	gen gen_hosp = is_hospital == 1 & inlist(type,"General Medical","General Medical & Surgical","Critical Access")
	
* hospital has CEO
	* individual
	* eventually replace with Katherine's flag
	gen char_ceo = regexm(title_standardized,"CEO:")
	bysort entity_uniqueid year: egen temp_hosp_has_ceo = max(char_ceo)
		replace char_ceo = 1 if regexm(title,"CEO") & temp_hosp_has_ceo == 0
		replace char_ceo = 1 if regexm(title,"ceo") & temp_hosp_has_ceo == 0
		replace char_ceo = 1 if regexm(title,"Chief Executive Officer") & temp_hosp_has_ceo == 0
		replace char_ceo = 1 if regexm(title,"chief executive officer") & temp_hosp_has_ceo == 0
	drop temp_hosp_has_ceo
	* hospital
	bysort entity_uniqueid year: egen hosp_has_ceo = max(char_ceo)
// 		replace hosp_has_ceo = . if is_hospital == 0 // can just filter later
	
* hospital CEO variables
	gen hospital_ceo = is_hospital==1 & char_ceo==1
	bysort contact_uniqueid: egen ever_hospital_ceo = max(hospital_ceo)
	
	gen system_ceo = char_ceo ==1 & inlist(entity_type,"Single Hospital Health System","IDS/RHA") 
	bysort contact_uniqueid: egen ever_sys_ceo = max(system_ceo)
	
* label FP/NFP variable
	label define ind_fp 0 "Non-Profit" 1 "For-Profit"
	label values forprofit ind_fp
	label values forprofit_parent ind_fp
	
* add parents
	preserve 
	
	* keep only CEOs
		keep if char_ceo == 1
		* keep hospitals and IDS/RHA or Single Hospital Health Systems
		keep if regexm(entity_type,"Hospital|IDS/RHA|Single Hospital Health System")
		
		* by CEO's unique ID and year, count unique HIMSS IDs:
			* make sure unique
			bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
			egen total_dup = total(duplicates)
			assert total_dup == 6 // 6 hosps that end up with two CEOs in the same year
			drop if duplicates == 1
			drop total_dup duplicates 
		
		* then tempfile the systems - keep only CEO ID and name, rename to parent
		keep if regexm(entity_type,"IDS/RHA|Single Hospital Health System")
		glob keepvars contact_uniqueid entity_uniqueid title title_standardized entity_type firstname lastname full_name
		* make unique 
			bysort himss_entityid year: gen count = 1 if _n > 1
			egen total_dup = total(count)
			assert total_dup == 1 // there is 1 system in 2017 with co-CEOs (both female). The same two co-CEOs for both facilities. Just keeping one.
			drop if count ==1 
			drop count total_dup
		keep himss_entityid year $keepvars
		foreach var in $keepvars {
			rename `var' `var'_parentceo
		}
		rename himss_entityid entity_parentid
		tempfile parent_obs
		save `parent_obs'
		
	restore
	
	* descriptives
	preserve
		bysort entity_uniqueid year: keep if _n == 1
		tab entity_type
		gen count = 1
		collapse (rawsum) count, by(entity_parentid)
		codebook count if !missing(entity_parentid)
		codebook count if !missing(entity_parentid) & count>1
	restore
				
	* merge parents onto children
	merge m:1 entity_parentid year using `parent_obs', gen(_merge_parent_ceo) keep(1 3)	
		
	
* do CEOs drop out of the sample and then come back? ___________________________ 
	preserve
		* keep only CEOs
		keep if char_ceo == 1
		
		* make sure unique
			bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
			egen total_dup = total(duplicates)
			assert total_dup == 6 // 6 hosps that end up with two CEOs in the same year
			drop if duplicates == 1
			drop total_dup duplicates 
		
		* keep one observation per ceo-year
		bysort contact_uniqueid year: keep if _n == 1
		
		* identify skips
		bysort contact_uniqueid (year): gen skip = year != (year[_n-1]+1) if _n > 1
				
		* add labels 
		label define skippers 0 "No Skip" 1 "Skip"
		label values skip skippers
		
		* share of ceos skipping over time
		graph bar skip, over(year) title("Share of Returning CEOs who Skipped >=1 Year") ///
			ytitle("Share") ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_skips_byyear.pdf", as(pdf) name("Graph") replace
		
		* share of ceos skipping over time HOSP ONLY
		graph bar skip if is_hospital ==1, over(year) title("Share of Returning CEOs who Skipped >=1 Year") ///
			ytitle("Share") ///
			subtitle("Hospitals Only") ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_skips_byyear_hosp.pdf", as(pdf) name("Graph") replace
		
		* descriptives on skippers/non-skippers
		graph bar forprofit char_female char_md, over(skip) ///
			title("Descriptives on Skip vs Non-Skip CEOs") ///
			ytitle("Share") ///
			legend(order(1 "For-Profit" 2 "Female" 3 "MD")) ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_skips_char.pdf", as(pdf) name("Graph") replace
		
		* descriptives on skippers/non-skippers HOSP ONLY
		graph bar forprofit char_female char_md if is_hospital ==1, over(skip) ///
			title("Descriptives on Skip vs Non-Skip CEOs") ///
			subtitle("Hospitals Only") ///
			ytitle("Share") ///
			legend(order(1 "For-Profit" 2 "Female" 3 "MD")) ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_skips_char_hosp.pdf", as(pdf) name("Graph") replace
		
		* collapse to person-level
		collapse (max) ever_skip = skip (max) char_md (mean) char_female (mean) forprofit, by(contact_uniqueid is_hospital)
		* tag one obs per person (for people who have worked at hosp and non hosp)
		bysort contact_uniqueid: gen tag = 1 if _n ==1
		sum ever_skip
		
		* descriptives on skippers/non-skippers
		graph bar forprofit char_female char_md if tag==1, over(ever_skip) ///
			title("Descriptives on Ever-Skip vs Non-Skip CEOs") ///
			ytitle("Share") ///
			legend(order(1 "For-Profit" 2 "Female" 3 "MD")) ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_everskips_char.pdf", as(pdf) name("Graph") replace
		
		* descriptives on skippers/non-skippers HOSP ONLY
		graph bar forprofit char_female char_md if is_hospital ==1, over(ever_skip) ///
			title("Descriptives on Ever-Skip vs Non-Skip CEOs") ///
			subtitle("Hospitals Only") ///
			ytitle("Share") ///
			legend(order(1 "For-Profit" 2 "Female" 3 "MD")) ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_everskips_char_hosp.pdf", as(pdf) name("Graph") replace
		
	restore
	
* does a hospital often report the same CEO as the whole health system? ________

	preserve
		
		keep if char_ceo == 1
		keep if is_hospital == 1
		
		* by CEO's unique ID and year, count unique HIMSS IDs:
			* make sure unique
			bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
			egen total_dup = total(duplicates)
			assert total_dup == 2 // 2 hospitals that end up with two CEOs in the same year
			drop if duplicates == 1
			drop total_dup duplicates 
		
		* check if CEOs are the same
		gen same_ceo = contact_uniqueid == contact_uniqueid_parent
		tab same_ceo entity_type_parent
		
		* keep a list of SHHS observations with the same hospital CEO for later 
			* to drop from CEO transitions analysis
		tempfile same_ceos
		save `same_ceos'
		
		* make count variables for graphing
		gen count_same = 1 if same_ceo == 1
		gen count_diff = 1 if same_ceo == 0
		gen count_unmerged = 1 if missing(same_ceo)

		* make graphics
		
			* shares
		graph bar same_ceo, over(entity_type_parent) ///
			title("Share of Hospitals With Same CEO as System") ///
			subtitle("by System Type") ///
			ytitle("Share of Hospitals") ///
			blabel(bar)
		graph export "${overleaf}/notes/CEO Descriptives/figures/same_ceo_shares.pdf", as(pdf) name("Graph") replace
		
			* counts
		collapse (rawsum) count_*, by(entity_type_parent)	
		graph bar count_same count_diff, over(entity_type_parent) stack ///
			legend(order(1 "Same CEO" 2 "Different CEOs")) ///
			title("Count of Hospitals With Same CEO as System") ///
			subtitle("by System Type") ///
			ytitle("Count of Hospitals") ///
			blabel(bar, position(base))
		graph export "${overleaf}/notes/CEO Descriptives/figures/same_ceo_counts.pdf", as(pdf) name("Graph") replace


	restore
	
	
* does CEO hold role at multiple hosps? how many by year? ______________________
	preserve
		* keep only CEOs
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* by CEO's unique ID and year, count unique HIMSS IDs:
			* make sure unique
			bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
			egen total_dup = total(duplicates)
			assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
			drop if duplicates == 1
			drop total_dup duplicates 
			* count hosps
			bysort contact_uniqueid year: gen count_ceo_roles = _N
			bysort contact_uniqueid year: keep if _n ==1 // one obs per person-yr
			keep contact_uniqueid year count_ceo_roles
			
		* save file to pull into main data
		tempfile mult_ceo_roles
		save `mult_ceo_roles'
	restore
	
	* repeat by AHAID
	preserve
		* keep only CEOs
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1
		
		* by CEO's unique ID and year, count unique AHA IDs:
			* make sure unique
			bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
			egen total_dup = total(duplicates)
			assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
			drop if duplicates == 1
			drop total_dup duplicates 
			* count hosps
			bysort contact_uniqueid year: gen count_ceo_roles_aha = _N
			bysort contact_uniqueid year: keep if _n ==1 // one obs per person-yr
			keep contact_uniqueid year count_ceo_roles_aha
			
		* save file to pull into main data
		tempfile mult_ceo_roles_aha
		save `mult_ceo_roles_aha'	
		
		* save file to pull into main data
		tempfile mult_ceo_roles_aha
		save `mult_ceo_roles_aha'
			
	restore
	
	* repeat for any c-suite role
	preserve
		* keep only C-suite obs
		keep if c_suite == 1
		* keep only hospital observations
		keep if is_hospital ==1
		
		gen count = 1
		collapse (rawsum) count (max) char_ceo, by(contact_uniqueid entity_uniqueid year)
		
		* count hosps
			bysort contact_uniqueid year: gen count_csuite_roles = _N
			bysort contact_uniqueid year: keep if _n ==1 // one obs per person-yr
			keep contact_uniqueid year count_csuite_roles
			
		* save file to pull into main data
		tempfile mult_csuite_roles
		save `mult_csuite_roles'	
	
	restore
	
	* repeat for systems?
	preserve
		keep if char_ceo == 1
		keep if is_hospital == 1
		
		gen count = 1
		collapse (rawsum) count (max) char_ceo, by(contact_uniqueid parent_system year)
		
		* count systems
			bysort contact_uniqueid year: gen count_system_roles = _N
			bysort contact_uniqueid year: keep if _n ==1 // one obs per person-yr
			keep contact_uniqueid year count_system_roles
			
		* save file to pull into main data
		tempfile mult_system_roles
		save `mult_system_roles'	
	
	
	restore
	
	* merge into main data
	merge m:1 contact_uniqueid year using `mult_ceo_roles', nogen keep(1 3)
	merge m:1 contact_uniqueid year using `mult_ceo_roles_aha', nogen keep(1 3)
	merge m:1 contact_uniqueid year using `mult_csuite_roles', nogen keep(1 3)
	merge m:1 contact_uniqueid year using `mult_system_roles', nogen keep(1 3)
	
	foreach var in count_ceo_roles count_ceo_roles_aha count_csuite_roles count_system_roles {
		forval i = 1/2 {
			gen `var'_`i' = `var' == `i'
				replace `var'_`i' = . if missing(`var')
		}
		gen `var'_3 = `var' >= 3
			replace `var'_3 = . if missing(`var')
	}
	
* CEO tenure ___________________________________________________________________ 
	preserve
	
		* keep only CEOs
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* make sure unique
			bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
			egen total_dup = total(duplicates)
			assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
			drop if duplicates == 1
			drop total_dup duplicates 
		
		* identify CEO changes
		sort entity_uniqueid year
		bysort entity_uniqueid (year): gen first_obs = 1 if _n ==1
		bysort entity_uniqueid (year): gen change_ceo = contact_uniqueid!= contact_uniqueid[_n-1] 
		replace change_ceo = . if first_obs==1
		
		* make a number for each CEO episode in a HIMSS facility
		gen episode_num = 0 if first_obs ==1
		bysort entity_uniqueid (year): replace episode_num = episode_num[_n-1] + change_ceo if _n > 1
		
		* length of each episode
		bysort entity_uniqueid episode_num: gen episode_length = _N
		
		* identify unfinished episodes
			gsort entity_uniqueid -year
			gen last_change_ceo_year = .
			bysort entity_uniqueid: replace last_change_ceo_year = year if change_ceo == 1
			bysort entity_uniqueid: replace last_change_ceo_year = last_change_ceo_year[_n-1] if missing(last_change_ceo_year)
			
			gen unfinished_yr = missing(last_change_ceo_year)
			bysort entity_uniqueid contact_uniqueid episode_num: egen unfinished = max(unfinished_yr)
			drop last_change_ceo_year unfinished_yr
			
		* identify first episodes
			sort entity_uniqueid year
			gen last_change_ceo_year = .
			bysort entity_uniqueid (year): replace last_change_ceo_year = year if change_ceo == 1
			bysort entity_uniqueid (year): replace last_change_ceo_year = last_change_ceo_year[_n-1] if missing(last_change_ceo_year)
			
			gen first = missing(last_change_ceo_year)
			drop last_change_ceo_year
			
		* make a unified episode_type variable
		gen episode_type_str = "First" if first==1
			replace episode_type_str = "Unfinished" if unfinished==1
			replace episode_type_str = "Completed" if first != 1 & unfinished != 1
		encode episode_type_str, gen(episode_type)	
		
		* prepare to pull into main data
		keep entity_uniqueid contact_uniqueid year episode* change_ceo
		tempfile ceo_episodes
		save `ceo_episodes'
		
	restore
	
	* merge into main data
	merge m:1 entity_uniqueid contact_uniqueid year using `ceo_episodes', nogen keep(1 3)
	
* make CEO turnover variable ___________________________________________________ 

	preserve 
	
		* keep only CEOs
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 | inlist(entity_type,"IDS/RHA","Single Hospital Health System")
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		tab total_dup
// 		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* check that observations are unique by hospital-year
		bysort entity_uniqueid year: gen count = 1 if _n > 1
		egen total_dup = total(count)
		tab total_dup
// 		assert total_dup == 1 // there is 1 facility in 2017 with co-CEOs (both female). The same two co-CEOs for both facilities. Just keeping one.
		drop if count ==1 
		drop count total_dup
 
		* initial version: any change in the CEO from year to year
		bysort entity_uniqueid (year): gen ceo_turnover1 = contact_uniqueid != contact_uniqueid[_n-1] if _n > 1
		
		keep entity_uniqueid year ceo_turnover1 gov_priv_type forprofit is_hospital
		
		tempfile ceo_turnover1_xwalk
		save `ceo_turnover1_xwalk'
		
		gen ceo_turnover_govpriv = ceo_turnover1 if !missing(gov_priv_type) & is_hospital ==1
		gen ceo_turnover_forprofit = ceo_turnover1 if !missing(forprofit) & is_hospital ==1
		
		collapse ceo_turnover1 ceo_turnover_govpriv ceo_turnover_forprofit, by(year)
		rename ceo_turnover1 ceo_turnover_all
		save "${dbdata}/derived/temp/ceoturnover_all", replace
		
	restore	
	
* merge turnover stats back into main sample
	merge m:1 entity_uniqueid year using `ceo_turnover1_xwalk', gen(_merge_turnover_xwalk)	
	
* tracking CEO changes _________________________________________________________ 

	* PULL IN LIST OF SHHS W/ SAME CEOS	
	preserve
		use `same_ceos', clear
		keep if same_ceo == 1 & entity_type_parent == "Single Hospital Health System"
		
		* entity_uniqueid_parent is what we want to keep a list of
		* these are the IDs for the SHHS "system-level" observations
		
		keep entity_uniqueid_parent year
		rename entity_uniqueid_parent entity_uniqueid
		
		bysort entity_uniqueid year: keep if _n == 1 // one dup
		
		tempfile list_same_ceo_shhs
		save `list_same_ceo_shhs'
	restore

	preserve 
		keep if ever_hospital_ceo ==1 | ever_sys_ceo == 1
		
		* drop the SHHS CEOs when they are the same as the facility CEOs
		merge m:1 entity_uniqueid year using `list_same_ceo_shhs', gen(_merge_same_ceo)
		* don't want to keep observations that match -> want to keep master only (1)
		keep if _merge_same_ceo == 1
		drop _merge_same_ceo
		
		* make a person-year dataset with all roles
		bysort contact_uniqueid year: gen role_num = _n
		* retain the max row_num as a local
			quietly summarize role_num
			local max_role = r(max) 
		keep entity_uniqueid title_standardized contact_uniqueid year role_num hospital_ceo parent_system entity_uniqueid_parent system_ceo entity_type entity_type_parent ever_hospital_ceo ever_sys_ceo
		reshape wide entity_uniqueid title_standardized hospital_ceo system_ceo parent_system entity_uniqueid_parent entity_type entity_type_parent, i(contact_uniqueid year ever_hospital_ceo ever_sys_ceo) j(role_num) 
	
		* make person-rownumber variable
		bysort contact_uniqueid (year): gen row_in_person = _n
		
		* indicator for any CEO role in the year
		egen hosp_ceo_this_year = rowmax(hospital_ceo*)
		
		* indicator for any CEO role in the year
		egen sys_ceo_this_year = rowmax(system_ceo*)
		
		* make gained_hosp_ceo variable
		bysort contact_uniqueid (year): gen gained_hosp_ceo = hosp_ceo_this_year ==1 ///
			& hosp_ceo_this_year[_n-1] == 0 if _n > 1
		
		* make gained_sys_ceo variable
		bysort contact_uniqueid (year): gen gained_sys_ceo = sys_ceo_this_year ==1 ///
			& sys_ceo_this_year[_n-1] == 0 if _n > 1
		
		* hospital CEO last year
		bysort contact_uniqueid (year): gen hosp_ceo_last_year = hosp_ceo_this_year[_n-1]
		
		* system CEO last year
		bysort contact_uniqueid (year): gen sys_ceo_last_year = sys_ceo_this_year[_n-1]
		
		* Previous year variable
		bysort contact_uniqueid (year): gen year_prev = year[_n-1]
		
		* make variables to track relevant entities by year
		gen strL all_hosp_ceo_entities = ""
		gen strL all_hosp_ceo_systems = ""
		gen strL all_sys_ceo_entities = ""
		gen strL all_entities = ""
		gen strL all_systems = ""
		gen n_hosp_ceo_entities = 0
		gen n_sys_ceo_entities = 0
		gen n_hosp_ceo_systems = 0
		* Loop over each pair
		forvalues j = 1/`max_role' {
			
			***** ENTITIES *****
			
			* all entities (unique) 
				* list of all entities worked at in the given year
			replace all_entities = ///
				cond( all_entities == "", ///
					  string(entity_uniqueid`j'), ///
					  all_entities + ";" + string(entity_uniqueid`j') ) ///
				if !missing(entity_uniqueid`j') ///
				&  strpos(";" + all_entities + ";",   ///
						 ";" + string(entity_uniqueid`j') + ";") == 0
			
			* unique hospital CEO entities counter
				* count of all entities worked at as hospital CEO in the given year
			replace n_hosp_ceo_entities = n_hosp_ceo_entities + 1 ///
				if  hospital_ceo`j' == 1 ///
				&  !missing(entity_uniqueid`j') ///
				&  strpos(";" + all_hosp_ceo_entities + ";",   ///
						 ";" + string(entity_uniqueid`j') + ";") == 0
			
			* hospital CEO entities only (unique)
				* list of all entities worked at as hospital CEO in the given year
			replace all_hosp_ceo_entities = ///
				cond( all_hosp_ceo_entities == "", ///
					  string(entity_uniqueid`j'), ///
					  all_hosp_ceo_entities + ";" + string(entity_uniqueid`j') ) ///
				if  hospital_ceo`j' == 1 ///
				&  !missing(entity_uniqueid`j') ///
				&  strpos(";" + all_hosp_ceo_entities + ";",   ///
						 ";" + string(entity_uniqueid`j') + ";") == 0	
						 
			* unique system CEO entities counter
				* count of all entities worked at as system CEO in the given year
			replace n_sys_ceo_entities = n_sys_ceo_entities + 1 ///
				if  system_ceo`j' == 1 ///
				&  !missing(entity_uniqueid`j') ///
				&  strpos(";" + all_sys_ceo_entities + ";",   ///
						 ";" + string(entity_uniqueid`j') + ";") == 0
			
			* system CEO entities only (unique)
				* list of all entities worked at as system CEO in the given year
			replace all_sys_ceo_entities = ///
				cond( all_sys_ceo_entities == "", ///
					  string(entity_uniqueid`j'), ///
					  all_sys_ceo_entities + ";" + string(entity_uniqueid`j') ) ///
				if  system_ceo`j' == 1 ///
				&  !missing(entity_uniqueid`j') ///
				&  strpos(";" + all_sys_ceo_entities + ";",   ///
						 ";" + string(entity_uniqueid`j') + ";") == 0	
			
			***** SYSTEMS *****
			
			* all systems (unique) 
				* list of all systems worked at in the given year
			replace all_systems = ///
				cond( all_systems == "", ///
					  string(parent_system`j'), ///
					  all_systems + ";" + string(parent_system`j') ) ///
				if !missing(parent_system`j') ///
				&  strpos(";" + all_systems + ";",   ///
						 ";" + string(parent_system`j') + ";") == 0
			
			* unique hospital CEO systems counter
				* count of all entities worked at as hospital CEO in the given year
			replace n_hosp_ceo_systems = n_hosp_ceo_systems + 1 ///
				if  hospital_ceo`j' == 1 ///
				&  !missing(parent_system`j') ///
				&  strpos(";" + all_hosp_ceo_systems + ";",   ///
						 ";" + string(parent_system`j') + ";") == 0
			
			* hospital CEO systems only (unique)
				* list of all entities worked at as hospital CEO in the given year
			replace all_hosp_ceo_systems = ///
				cond( all_hosp_ceo_systems == "", ///
					  string(parent_system`j'), ///
					  all_hosp_ceo_systems + ";" + string(parent_system`j') ) ///
				if  hospital_ceo`j' == 1 ///
				&  !missing(parent_system`j') ///
				&  strpos(";" + all_hosp_ceo_systems + ";",   ///
						 ";" + string(parent_system`j') + ";") == 0	
		
		}
		
		* Lagged entities variable: entities_prev
		bysort contact_uniqueid (year): gen entities_prev = all_entities[_n-1]
		* Lagged systems variable: systems_prev
		bysort contact_uniqueid (year): gen systems_prev = all_systems[_n-1]
		* the prior_all variable isn't working (it's not cumulative) and it isn't used yet
		gen entities_prior_all = ""
		bysort contact_uniqueid (year): replace entities_prior_all = ///
			cond(!missing(entities_prev), entities_prev[_n-1]+";"+entities_prev, entities_prev[_n-1])
			
		* Make an "added CEOs" variable	and counters
		
		foreach stub in hosp sys {
			bysort contact_uniqueid (year): gen `stub'_ceos_prev = all_`stub'_ceo_entities[_n-1]
			gen added_`stub'_ceo_entities = ""
			gen n_`stub'_ceo_added = 0
			gen n_`stub'_ceo_add_overlap = 0
			gen n_`stub'_ceo_ent_overlap = 0
			gen n_`stub'_ceo_sys_overlap = 0
			gen covered_`stub'_ceo_ent = "" // temporary var to track the entities I've looped over
			forvalues j = 1/`max_role' {
				if "`stub'" == "hosp" {
					local longstub hospital
				}
				else if "`stub'" == "sys" {
					local longstub system
				}
				
				* counter for whether current ceo entities overlap with prior ceo entities
				replace n_`stub'_ceo_ent_overlap = n_`stub'_ceo_ent_overlap + 1 ///
						if  `longstub'_ceo`j' == 1 ///
						&  !missing(entity_uniqueid`j') ///
						&  strpos(";" + covered_`stub'_ceo_ent + ";",   /// not counted yet
								 ";" + string(entity_uniqueid`j') + ";") == 0 ///
						&  strpos(";" + `stub'_ceos_prev + ";", /// ID was in last year list of any entity
								 ";" + string(entity_uniqueid`j') + ";") > 0
				
				* track the unique entities we have counted so far
				replace covered_`stub'_ceo_ent = ///
					cond( covered_`stub'_ceo_ent == "", ///
						  string(entity_uniqueid`j'), ///
						  covered_`stub'_ceo_ent + ";" + string(entity_uniqueid`j') ) ///
					if `longstub'_ceo`j' == 1 ///
					&  !missing(entity_uniqueid`j') ///
					&  strpos(";" + covered_`stub'_ceo_ent + ";",   ///
							 ";" + string(entity_uniqueid`j') + ";") == 0
				
				* added CEOs counter
				replace n_`stub'_ceo_added = n_`stub'_ceo_added + 1 ///
					if  `longstub'_ceo`j' == 1 /// conditions to do the replacement:
						&  !missing(entity_uniqueid`j') /// the ID isn't missing
						&  strpos(";" + added_`stub'_ceo_entities + ";",   /// ID is not already in the list
							 ";" + string(entity_uniqueid`j') + ";") == 0 ///
						&  	strpos(";" + `stub'_ceos_prev + ";",   /// ID wasn't in last year's list
							 ";" + string(entity_uniqueid`j') + ";") == 0 ///
						&  	row_in_person > 1 // not the first year for the person 
				
				* counter for "added CEO entity was in the entities_prev" overlap
				replace n_`stub'_ceo_add_overlap = n_`stub'_ceo_add_overlap + 1 ///
					if `longstub'_ceo`j' == 1 /// conditions to do the replacement:
						&  !missing(entity_uniqueid`j') /// the ID isn't missing
						&  strpos(";" + added_`stub'_ceo_entities + ";",   /// ID is not already in the list
							 ";" + string(entity_uniqueid`j') + ";") == 0 ///
						&  	strpos(";" + `stub'_ceos_prev + ";",   /// ID wasn't in last year's list
							 ";" + string(entity_uniqueid`j') + ";") == 0 ///
						&  	row_in_person > 1 /// not the first year for the person 
						&  strpos(";" + entities_prev + ";", /// ID was in last year list of any entity
							 ";" + string(entity_uniqueid`j') + ";") > 0 
				
				* counter for "added CEO system was in the systems_prev" overlap 
				replace n_`stub'_ceo_sys_overlap = n_`stub'_ceo_sys_overlap + 1 ///
					if `longstub'_ceo`j' == 1 /// conditions to do the replacement:
						&  !missing(entity_uniqueid`j') /// the ID isn't missing
						&  strpos(";" + added_`stub'_ceo_entities + ";",   /// ID is not already in the list
								";" + string(entity_uniqueid`j') + ";") == 0 ///
						&  	strpos(";" + `stub'_ceos_prev + ";",   /// ID wasn't in last year's list
								";" + string(entity_uniqueid`j') + ";") == 0 ///
						&  	row_in_person > 1 /// not the first year for the person 
						&  strpos(";" + systems_prev + ";", /// system ID was in last year list of any sysid
								";" + string(parent_system`j') + ";") > 0 
							 
				* append to the added ceos list 
				replace added_`stub'_ceo_entities = ///
					cond( added_`stub'_ceo_entities == "", ///
						  string(entity_uniqueid`j'), ///
						  added_`stub'_ceo_entities + ";" + string(entity_uniqueid`j') ) ///
					if  `longstub'_ceo`j' == 1 /// conditions to do the replacement:
						&  !missing(entity_uniqueid`j') /// the ID isn't missing
						&  strpos(";" + added_`stub'_ceo_entities + ";",   /// ID is not already in the list
							 ";" + string(entity_uniqueid`j') + ";") == 0 ///
						&  	strpos(";" + `stub'_ceos_prev + ";",   /// ID wasn't in last year's list
							 ";" + string(entity_uniqueid`j') + ";") == 0 ///
						&  	row_in_person > 1 // not the first year for the person 
			}
			drop covered_`stub'_ceo_ent
			
			* Count (overlapping) CEO roles by year 	
			* how many total CEO entities?
				* how many overlap with previous CEO entities (retaining roles)?
			* how many added CEO entities?
				* how many overlap with previous entities (any role)? i.e. is the CEO role at a new place?
			
			* any prior year promotion variable
			gen `stub'_promotion_type = ""
				* Internal promotion: CEO entity appears in any prior entity list
				replace `stub'_promotion_type = "internal" if gained_`stub'_ceo == 1 ///
					& n_`stub'_ceo_add_overlap >= 1 // at least one promotion to CEO where they already worked	
				* External promotion: CEO entity not in any prior entity list
				replace `stub'_promotion_type = "external" if gained_`stub'_ceo == 1 ///
					& n_`stub'_ceo_add_overlap == 0 // zero promotions to CEO where they already worked	
			
			* consecutive promotions variable
			gen `stub'_promotion_type_consec = ""
				* Internal promotion: CEO entity appears in any prior entity list
				replace `stub'_promotion_type_consec = "internal" if gained_`stub'_ceo == 1 ///
					& n_`stub'_ceo_add_overlap >= 1 /// at least one promotion to CEO where they already worked
					& year_prev == year - 1
				* External promotion: CEO entity not in any prior entity list
				replace `stub'_promotion_type_consec = "external" if gained_`stub'_ceo == 1 ///
					& n_`stub'_ceo_add_overlap == 0 /// zero promotions to CEO where they already worked
					& year_prev == year - 1
				replace `stub'_promotion_type_consec = "missing year" if gained_`stub'_ceo == 1 ///
					& year_prev != year - 1
							
			* move type 
			gen `stub'_move_type = ""
				* External
				replace `stub'_move_type = "external" if gained_`stub'_ceo == 0 /// not the first CEO promotion
					& row_in_person > 1 /// not first year for person
					& `stub'_ceo_this_year == 1 /// CEO this year
					& `stub'_ceo_last_year == 1 /// CEO last year
					& n_`stub'_ceo_added >= 1 /// added a CEO role this year
					& n_`stub'_ceo_add_overlap == 0 /// all CEO entities are new (didn't work there last obs in any role)
					& n_`stub'_ceo_ent_overlap == 0 // did not retain any old CEO roles
				replace `stub'_move_type = "internal" if gained_`stub'_ceo == 0 /// not the first CEO promotion
					& row_in_person > 1 /// not first year for person
					& `stub'_ceo_this_year == 1 /// CEO this year
					& `stub'_ceo_last_year == 1 /// CEO last year
					& n_`stub'_ceo_added >= 1 /// added a CEO role this year
					& n_`stub'_ceo_added == n_`stub'_ceo_add_overlap // already worked at all the places they are now CEO (internal only)
				replace `stub'_move_type = "lateral" if gained_`stub'_ceo == 0 /// not the first CEO promotion
					& row_in_person > 1 /// not first year for person
					& `stub'_ceo_this_year == 1 /// CEO this year
					& `stub'_ceo_last_year == 1 /// CEO last year
					& n_`stub'_ceo_added >= 1 /// added a CEO role this year
					& n_`stub'_ceo_add_overlap < n_`stub'_ceo_added /// they have at least one external promotion
					& n_`stub'_ceo_ent_overlap > 0 // they are retaining at least one old CEO role
					
			* move type (consecutive)
			gen `stub'_move_type_consec = ""
				* External
				replace `stub'_move_type_consec = "external" if gained_`stub'_ceo == 0 /// not the first CEO promotion
					& year_prev == year-1 /// consecutive years
					& row_in_person > 1 /// not first year for person
					& `stub'_ceo_this_year == 1 /// CEO this year
					& `stub'_ceo_last_year == 1 /// CEO last year
					& n_`stub'_ceo_added >= 1 /// added a CEO role this year
					& n_`stub'_ceo_add_overlap == 0 /// all CEO entities are new (didn't work there last obs)
					& n_`stub'_ceo_ent_overlap == 0 // did not retain any old CEO roles
				replace `stub'_move_type_consec = "internal" if gained_`stub'_ceo == 0 /// not the first CEO promotion
					& year_prev == year-1 /// consecutive years
					& row_in_person > 1 /// not first year for person
					& `stub'_ceo_this_year == 1 /// CEO this year
					& `stub'_ceo_last_year == 1 /// CEO last year
					& n_`stub'_ceo_added >= 1 /// added a CEO role this year
					& n_`stub'_ceo_added == n_`stub'_ceo_add_overlap // already worked at all the places they are now CEO
				replace `stub'_move_type_consec = "lateral" if gained_`stub'_ceo == 0 /// not the first CEO promotion
					& year_prev == year-1 /// consecutive years
					& row_in_person > 1 /// not first year for person
					& `stub'_ceo_this_year == 1 /// CEO this year
					& `stub'_ceo_last_year == 1 /// CEO last year
					& n_`stub'_ceo_added >= 1 /// added a CEO role this year
					& n_`stub'_ceo_add_overlap < n_`stub'_ceo_added /// they have at least one external promotion
					& n_`stub'_ceo_ent_overlap > 0 // they are retaining at least one old CEO role	
				replace `stub'_move_type_consec = "missing year" if gained_`stub'_ceo == 0 /// not the first CEO promotion
					& year_prev != year-1 /// consecutive years
					& row_in_person > 1 /// not first year for person
					& `stub'_ceo_this_year == 1 /// CEO this year
					& `stub'_ceo_last_year == 1 /// CEO last year
					& n_`stub'_ceo_added >= 1 // added a CEO role this year
			
					
			* COMBINE promotions and moves together
			* any prev year
			gen `stub'_transition_type = ""
				replace `stub'_transition_type = "internal promotion" ///
					if gained_`stub'_ceo == 1 ///
					& `stub'_promotion_type == "internal"
				replace `stub'_transition_type = "external promotion" ///
					if gained_`stub'_ceo == 1 ///
					& `stub'_promotion_type == "external"
				replace `stub'_transition_type = "internal move" ///
					if gained_`stub'_ceo == 0 ///
					& `stub'_move_type == "internal"
				replace `stub'_transition_type = "external move" ///
					if gained_`stub'_ceo == 0 ///
					& `stub'_move_type == "external"
				replace `stub'_transition_type = "lateral move" ///
					if gained_`stub'_ceo == 0 ///
					& `stub'_move_type == "lateral"
			* we want to know if they stayed within or across systems
			gen `stub'_test_transition_sys = ""
				replace `stub'_test_transition_sys = "within system" if !missing(`stub'_transition_type) ///
					& n_`stub'_ceo_sys_overlap > 0
				replace hosp_test_transition_sys = "across system" if !missing(`stub'_transition_type) ///
					& n_`stub'_ceo_sys_overlap == 0
			
			* consecutive only
			gen `stub'_transition_type_consec = ""
				replace `stub'_transition_type_consec = "internal promotion" ///
					if gained_`stub'_ceo == 1 ///
					& `stub'_promotion_type_consec == "internal"
				replace `stub'_transition_type_consec = "external promotion" ///
					if gained_`stub'_ceo == 1 ///
					& `stub'_promotion_type_consec == "external"
				replace `stub'_transition_type_consec = "internal move" ///
					if gained_`stub'_ceo == 0 ///
					& `stub'_move_type_consec == "internal"
				replace `stub'_transition_type_consec = "external move" ///
					if gained_`stub'_ceo == 0 ///
					& `stub'_move_type_consec == "external"
				replace `stub'_transition_type_consec = "lateral move" ///
					if gained_`stub'_ceo == 0 ///
					& `stub'_move_type_consec == "lateral"
			
			* add definition variable 
			if "`stub'" == "hosp" {
				gen `stub'_definition = "First time becoming hospital CEO at a hospital previously worked at (non-CEO)" ///
					if `stub'_transition_type == "internal promotion"
					replace `stub'_definition = "First time becoming hospital CEO at a hospital never worked at before" ///
						if `stub'_transition_type == "external promotion"
					replace `stub'_definition = "Already hospital CEO; add CEO role at a hospital previously worked at (non-CEO)" ///
						if `stub'_transition_type == "internal move"
					replace `stub'_definition = "Already hospital CEO; add CEO role at a hospital never worked at before" ///
						if `stub'_transition_type == "lateral move"
					replace `stub'_definition = "Drop previous hospital CEO roles; become CEO at a hospital never worked at before" ///
					if `stub'_transition_type == "external move"
			}
			if "`stub'" == "sys" {
				gen `stub'_definition = "First time becoming system CEO at a system previously worked at (non-system-CEO)" ///
					if `stub'_transition_type == "internal promotion"
					replace `stub'_definition = "First time becoming system CEO at a system never worked at before" ///
						if `stub'_transition_type == "external promotion"
					replace `stub'_definition = "Already system CEO; add CEO role at a system previously worked at (non-system-CEO)" ///
						if `stub'_transition_type == "internal move"
					replace `stub'_definition = "Already system CEO; add CEO role at a system never worked at before" ///
						if `stub'_transition_type == "lateral move"
					replace `stub'_definition = "Drop previous system CEO roles; become CEO at a system never worked at before" ///
						if `stub'_transition_type == "external move"
			}
			
			
			* add example variable
			gen `stub'_example = "Year t−1: CFO at Hospital B → Year t: CEO at Hospital B" ///
				if `stub'_transition_type == "internal promotion"
				replace `stub'_example = "Year t−1: No role at Hospital B → Year t: CEO at Hospital B" ///
					if `stub'_transition_type == "external promotion"
				replace `stub'_example = "Year t−1: CEO at Hospital A, CFO at Hospital B → Year t: CEO at A and B" ///
					if `stub'_transition_type == "internal move"
				replace `stub'_example = "Year t−1: CEO at Hospital A → Year t: CEO at A and B" ///
					if `stub'_transition_type == "lateral move"
				replace `stub'_example = "Year t−1: CEO at Hospital A → Year t: CEO at Hospital B (no longer CEO at A)" ///
					if `stub'_transition_type == "external move"
				
		}
		
		* save xwalk file to pull into the full dataset		
			tempfile career_transitions_xwalk
			save `career_transitions_xwalk'
		
		* make descriptive table: 
		***** HOSPITAL CEO TRANSITIONS ****	
			keep if ever_hospital_ceo == 1
			* number of career transitions
				gen n_transitions = 1 if !missing(hosp_transition_type)
			* number of unique individuals by career transition
				bysort contact_uniqueid hosp_transition_type hosp_transition_type: gen n_individuals = 1 if _n ==1 & !missing(hosp_transition_type)
			* number of unique CEOs regardless of transition
				bysort contact_uniqueid: gen total_ceos_counter = 1 if _n == 1
				egen total_ceos = total(total_ceos_counter)
			collapse (rawsum) n_transitions (rawsum) n_individuals (mean) total_ceos (firstnm) hosp_definition, by(hosp_transition_type)
			gen share_of_ceos = n_individuals/total_ceos
			drop if hosp_transition_type == ""
			
			* order rows
			gen order = 1 if hosp_transition_type == "internal promotion"
				replace order = 2 if hosp_transition_type == "external promotion"
				replace order = 3 if hosp_transition_type == "internal move"
				replace order = 4 if hosp_transition_type == "lateral move"
				replace order = 5 if hosp_transition_type == "external move"
			sort order
				
			* export: set up latex table
			tempname f
			file open `f' using "${overleaf}/notes/CEO Descriptives/figures/career_transitions_table_hosp.tex", write replace
		
			* Write LaTeX table preamble
			file write `f' "\begin{tabular}{l p{5cm} r r r}" _n
			file write `f' "\toprule" _n
			file write `f' "Career Transition & Definition & Transitions & Individuals & Share of CEOs \\" _n
			file write `f' "\midrule" _n

			* Loop through observations
			quietly {
				count
				local N = r(N)
				forvalues i = 1/`N' {
					local ct = hosp_transition_type[`i']
					local defn = hosp_definition[`i']
					local tr = n_transitions[`i']
					local ind = n_individuals[`i']
					local share = round(share_of_ceos[`i'], 0.001)

					* Escape underscores for LaTeX
					foreach v in ct defn {
						local `v' : subinstr local `v' "_" "\\_", all
					}

					* Write table row
					file write `f' "`ct' & `defn' & `tr' & `ind' & `share' \\" _n
				}
			}

			file write `f' "\bottomrule" _n
			file write `f' "\end{tabular}" _n
			file close `f'
			
		***** SYSTEM CEO TRANSITIONS ****	
		use `career_transitions_xwalk', clear
		
		keep if ever_sys_ceo == 1
		* get rid of the SHHS same CEOs here ?
		
		gen n_transitions = 1 if !missing(sys_transition_type)
			* number of unique individuals by career transition
				bysort contact_uniqueid sys_transition_type sys_transition_type: gen n_individuals = 1 if _n ==1 & !missing(sys_transition_type)
			* number of unique CEOs regardless of transition
				bysort contact_uniqueid: gen total_ceos_counter = 1 if _n == 1
				egen total_ceos = total(total_ceos_counter)
			collapse (rawsum) n_transitions (rawsum) n_individuals (mean) total_ceos (firstnm) sys_definition, by(sys_transition_type)
			gen share_of_ceos = n_individuals/total_ceos
			drop if sys_transition_type == ""
			
			* order rows
			gen order = 1 if sys_transition_type == "internal promotion"
				replace order = 2 if sys_transition_type == "external promotion"
				replace order = 3 if sys_transition_type == "internal move"
				replace order = 4 if sys_transition_type == "lateral move"
				replace order = 5 if sys_transition_type == "external move"
			sort order
				
			* export: set up latex table
			tempname f
			file open `f' using "${overleaf}/notes/CEO Descriptives/figures/career_transitions_table_sys.tex", write replace
		
			* Write LaTeX table preamble
			file write `f' "\begin{tabular}{l p{5cm} r r r}" _n
			file write `f' "\toprule" _n
			file write `f' "Career Transition & Definition & Transitions & Individuals & Share of CEOs \\" _n
			file write `f' "\midrule" _n

			* Loop through observations
			quietly {
				count
				local N = r(N)
				forvalues i = 1/`N' {
					local ct = sys_transition_type[`i']
					local defn = sys_definition[`i']
					local tr = n_transitions[`i']
					local ind = n_individuals[`i']
					local share = round(share_of_ceos[`i'], 0.001)

					* Escape underscores for LaTeX
					foreach v in ct defn {
						local `v' : subinstr local `v' "_" "\\_", all
					}

					* Write table row
					file write `f' "`ct' & `defn' & `tr' & `ind' & `share' \\" _n
				}
			}

			file write `f' "\bottomrule" _n
			file write `f' "\end{tabular}" _n
			file close `f'
			
		
	restore
	
* merge career transitions crosswalk into main file
	merge m:1 contact_uniqueid year using `career_transitions_xwalk', nogen keepusing(contact_uniqueid year *transition_type gained_hosp_ceo gained_sys_ceo added_hosp_ceo_entities added_sys_ceo_entities hosp_ceo_last_year hosp_test_transition_sys sys_test_transition_sys) keep(1 3)
	* note that this information is currently at the person-year level
	* it won't be specific to the HIMSS entity or role in the main data
	* could keep the CEO entities and reshape them? 
		* Or otherwise rewrite the code to identify the specific entities concerned in each transition and merge the crosswalk onto them
	
	* fix to questions above:
	* hosps
		rename hosp_transition_type hosp_transition_type_yr // will be the same for all person obs in a year
		gen hosp_transition_type = ""
		replace hosp_transition_type = hosp_transition_type_yr if ///
			strpos(";" + added_hosp_ceo_entities + ";", ";" + string(entity_uniqueid) + ";") > 0 ///
			& char_ceo == 1 // will actually correspond to the right CEO promotion for that year
		* with within/across system breakout
		gen hosp_transition_type_det_yr = hosp_transition_type+hosp_test_transition_sys
		gen hosp_transition_type_det = ""
		replace hosp_transition_type_det = hosp_transition_type_det_yr if ///
			strpos(";" + added_hosp_ceo_entities + ";", ";" + string(entity_uniqueid) + ";") > 0 ///
			& char_ceo == 1 // will actually correspond to the right CEO promotion for that year
	* systems
		rename sys_transition_type sys_transition_type_yr // will be the same for all person obs in a year
		gen sys_transition_type = ""
		replace sys_transition_type = sys_transition_type_yr if ///
			strpos(";" + added_sys_ceo_entities + ";", ";" + string(entity_uniqueid) + ";") > 0 ///
			& char_ceo == 1 // will actually correspond to the right CEO promotion for that year
		* with within/across system breakout
		gen sys_transition_type_det_yr = sys_transition_type+sys_test_transition_sys
		gen sys_transition_type_det = ""
		replace sys_transition_type_det = sys_transition_type_det_yr if ///
			strpos(";" + added_sys_ceo_entities + ";", ";" + string(entity_uniqueid) + ";") > 0 ///
			& char_ceo == 1 // will actually correspond to the right CEO promotion for that year
		
* save temp file _______________________________________________________________

save "${dbdata}/derived/temp/indiv_file_contextual.dta", replace


* SCRATCH - MANUALLY CHECKING SKIPPERS _________________________________________ 

exit

* contact_uniqueid = 76079
	* Laurence Hinsdale
	
	* Our data:
	* President & CEO at carolinas medical center northeast in 2009
	* Missing from our data 2009-2013
	* Executive VP of Regional Group at cone health annie penn hospital 2014-15
	
	* Online says:
		* Hinsdale was an executive at Carolinas HealthCare System (now Atrium Health) and retired in 2014
			* https://www.wbtv.com/story/28029342/carolinas-healthcare-ceo-paid-53-million-in-2014/
		* he worked there as of 2011: Executive Vice President, Carolinas HealthCare System; Total 2011 compensation: $1,693,314
			* https://www.newsobserver.com/news/special-reports/prognosis-profits/article16924658.html 


* contact_uniqueid = 110300
	* Thomas Pentz
	
	* In our data:
	* CEO of Orange Park Medical Center until 2012
	* Comes back as interim CEO of memorial hospital Jackson in 2016-17
	* Joins LewisGale as interim CEO in 2017
	
	* Online:
		* retired from Orange Park at the end of 2013
			* https://www.jacksonville.com/story/business/2013/06/18/orange-park-medical-center-ceo-tom-pentz-will-retire-end-year/15825382007/
		* joined Southern Hills Hospital as interim CEO in 2015; https://www.reviewjournal.com/life/health/southern-hills-hospital-names-new-ceo/ 
		* Finished at Southern Hills in September 2015: https://www.beckershospitalreview.com/hospital-executive-moves/southern-hills-hospital-names-new-ceo-4-things-to-know/
		* joined Memorial as interim CEO in April 2017: https://www.dcmsonline.org/news/337559/Memorial-Hospital-CEO-to-Retire-from-Hospital-Administration.htm 
		* joineed LewisGale in 2018 https://www.beckershospitalreview.com/hospital-executive-moves/lewisgale-regional-health-system-ceo-to-depart-in-june-4-notes/
		
* contact_uniqueid = 95650
	* Mark Sprada
	
	* In our data: 
	* Interim CEO, COO, & CNO at imperial point medical center in 2011
	* Interim CEO & Corporate CNO at broward general medical center in 2016
	
	* Online: 
	* Imperial point is part of broward health
	* seems like he stayed at Broward throughout the whole time (linkedin says 1997-2017)
		* https://www.linkedin.com/in/markspradarn/
	* He is referred to as interim CEO in 2017:
		* https://www.healthcarefacilitiestoday.com/posts/Broward-Health-CEO-discusses-Fort-Lauderdale-airport-shooting--14442 
		

* contact_uniqueid = 1394364
	* Donna Harris
	
	* Our data: 
	* CEO at healthsouth rehabilitation hospital of jonesboro every year except 2012
	
	* Online:
	* Linkedin shows that she has continuously been CEO of this facility 
	* We are missing 2012 for the whole facility
	
	
* contact_uniqueid = 2302493	
	* David Ressler
	
	* Our data:
	* CEO until 2012 at aspen valley hospital
	* Missing 2013-2017
	* CEO again in 2017 at aspen valley hospital
	
	* Online:
	* Linkedin says he was CEO at aspen valley hospital until 2013 then C-suite at Tucson Medical Center from 2013-2015: https://www.linkedin.com/in/david-ressler-aa6112b3/
	* He returned to CEO role at aspen valley hospital in 2016: https://www.aspendailynews.com/news/avh-ceo-announces-2026-retirement/article_9a4143d7-4661-4ff3-a544-ce471b571ebc.html

* contact_uniqueid = 2332613
	* Edward Mirzabegian
	
	* Our data:
	* CEO all over, missing in 2014
	
	* Online: 
	* Transitioned between roles in 2013 and 2014 so may be missing due to timing








