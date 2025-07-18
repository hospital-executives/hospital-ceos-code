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
	use "${dbdata}/derived/final_confirmed_aha_update_530.dta", clear
	
* merge in M&A data	
	merge m:1 entity_uniqueid year using "${dbdata}/derived/temp/merged_ma_sysid_xwalk.dta", gen(_merge_ma_xwalk) keep(1 3)
	* DOUBLE CHECK NONE ARE MISSING:	
		keep if !missing(entity_uniqueid)
		
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
	gen char_female = gender=="F"
		replace char_female = . if missing(gender)
		label define gender_female 0 "Male" 1 "Female"
		label values char_female gender_female
		
* hospital has CEO
	* individual
	gen char_ceo = regexm(title_standardized,"CEO:")
	bysort entity_uniqueid year: egen temp_hosp_has_ceo = max(char_ceo)
		replace char_ceo = 1 if regexm(title,"CEO") & temp_hosp_has_ceo == 0
		replace char_ceo = 1 if regexm(title,"ceo") & temp_hosp_has_ceo == 0
		replace char_ceo = 1 if regexm(title,"Chief Executive Officer") & temp_hosp_has_ceo == 0
		replace char_ceo = 1 if regexm(title,"chief executive officer") & temp_hosp_has_ceo == 0
	*drop temp_hosp_has_ceo
	* hospital
	bysort entity_uniqueid year: egen hosp_has_ceo = max(char_ceo)
	
* label FP/NFP variable
	label define ind_fp 0 "Non-Profit" 1 "For-Profit"
	label values forprofit_ps ind_fp
	
* do CEOs drop out of the sample and then come back?
	preserve
		* keep only CEOs
		keep if char_ceo == 1
		
		* make sure unique
			bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
			egen total_dup = total(duplicates)
			assert total_dup == 6 // 6 facilities that end up with two CEOs in the same year
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
		
		* descriptives on skippers/non-skippers
		graph bar forprofit_ps char_female char_md, over(skip) ///
			title("Descriptives on Skip/vs Non-Skip CEOs") ///
			ytitle("Share") ///
			legend(order(1 "For-Profit" 2 "Female" 3 "MD")) ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_skips_char.pdf", as(pdf) name("Graph") replace
		
		collapse (max) ever_skip = skip (max) char_md (mean) char_female (mean) forprofit_ps, by(contact_uniqueid)

		sum ever_skip
		
		* descriptives on skippers/non-skippers
		graph bar forprofit_ps char_female char_md, over(ever_skip) ///
			title("Descriptives on Ever Skip/vs Non-Skip CEOs") ///
			ytitle("Share") ///
			legend(order(1 "For-Profit" 2 "Female" 3 "MD")) ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_everskips_char.pdf", as(pdf) name("Graph") replace
		
	restore
	
* does CEO hold role at multiple hosps? how many by year?
	preserve
		* keep only CEOs
		keep if char_ceo == 1
		
		* by CEO's unique ID and year, count unique HIMSS IDs:
			* make sure unique
			bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
			egen total_dup = total(duplicates)
			assert total_dup == 6 // 6 facilities that end up with two CEOs in the same year
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
		
		* by CEO's unique ID and year, count unique AHA IDs:
			* make sure unique
			bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
			egen total_dup = total(duplicates)
			assert total_dup == 6 // 6 facilities that end up with two CEOs in the same year
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
	
	* merge into main data
	merge m:1 contact_uniqueid year using `mult_ceo_roles', nogen keep(1 3)
	merge m:1 contact_uniqueid year using `mult_ceo_roles_aha', nogen keep(1 3)
	merge m:1 contact_uniqueid year using `mult_csuite_roles', nogen keep(1 3)
	
	foreach var in count_ceo_roles count_ceo_roles_aha count_csuite_roles {
		forval i = 1/2 {
			gen `var'_`i' = `var' == `i'
				replace `var'_`i' = . if missing(`var')
		}
		gen `var'_3 = `var' >= 3
			replace `var'_3 = . if missing(`var')
	}
	
* CEO tenure
	preserve
	
		* keep only CEOs
		keep if char_ceo == 1
		
		* make sure unique
			bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
			egen total_dup = total(duplicates)
			assert total_dup == 6 // 6 facilities that end up with two CEOs in the same year
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
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 6 // 6 facilities that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* check that observations are unique by hospital-year
		bysort entity_uniqueid year: gen count = 1 if _n > 1
		egen total_dup = total(count)
		assert total_dup == 2 // there are 2 facilities in 2017 with co-CEOs (both female). The same two co-CEOs for both facilities. Just keeping one.
		drop if count ==1 
		drop count total_dup
 
		* initial version: any change in the CEO from year to year
		bysort entity_uniqueid (year): gen ceo_turnover1 = contact_uniqueid != contact_uniqueid[_n-1] if _n > 1
		
		keep entity_uniqueid year ceo_turnover1 gov_priv_type_ps forprofit_ps
		
		tempfile ceo_turnover1_xwalk
		save `ceo_turnover1_xwalk'
		
		gen ceo_turnover_govpriv = ceo_turnover1 if !missing(gov_priv_type_ps)
		gen ceo_turnover_forprofit = ceo_turnover1 if !missing(forprofit_ps)
		
		collapse ceo_turnover1 ceo_turnover_govpriv ceo_turnover_forprofit, by(year)
		rename ceo_turnover1 ceo_turnover1_all
		tempfile ceoturnover_all
		save `ceoturnover_all'
		
	restore	
	
* merge turnover stats back into main sample
	merge m:1 entity_uniqueid year using `ceo_turnover1_xwalk', gen(_merge_turnover_xwalk)	
	
* tracking CEO changes _________________________________________________________ 

	gen hospital = inlist(entity_type,"Hospital","Single Hospital Health System") 
	gen hospital_ceo = hospital==1 & char_ceo==1
	bysort contact_uniqueid: egen ever_hospital_ceo = max(hospital_ceo)

	preserve 
		keep if ever_hospital_ceo == 1
		
		* make a person-year dataset with all roles
		bysort contact_uniqueid year: gen role_num = _n
		* retain the max row_num as a local
			quietly summarize role_num
			local max_role = r(max) 
		keep entity_uniqueid title_standardized contact_uniqueid year role_num hospital_ceo
		reshape wide entity_uniqueid title_standardized hospital_ceo, i(contact_uniqueid year) j(role_num) 
	
		* make person-rownumber variable
		bysort contact_uniqueid (year): gen row_in_person = _n
		
		* indicator for any CEO role in the year
		egen ceo_this_year = rowmax(hospital_ceo*)
		
		* make gained_ceo variable
		bysort contact_uniqueid (year): gen gained_ceo = ceo_this_year ==1 & ceo_this_year[_n-1] == 0 if _n > 1
		
		* CEO last year
		bysort contact_uniqueid (year): gen ceo_last_year = ceo_this_year[_n-1]
		
		* make variables to track relevant entities by year
		gen strL all_ceo_entities = ""
		gen strL all_entities = ""
		gen n_ceo_entities = 0
		* Loop over each pair
		forvalues j = 1/`max_role' {
			* all entities (unique)
			replace all_entities = ///
				cond( all_entities == "", ///
					  string(entity_uniqueid`j'), ///
					  all_entities + ";" + string(entity_uniqueid`j') ) ///
				if !missing(entity_uniqueid`j') ///
				&  strpos(";" + all_entities + ";",   ///
						 ";" + string(entity_uniqueid`j') + ";") == 0
			
			* unique CEO entities counter
			replace n_ceo_entities = n_ceo_entities + 1 ///
				if  hospital_ceo`j' == 1 ///
				&  !missing(entity_uniqueid`j') ///
				&  strpos(";" + all_ceo_entities + ";",   ///
						 ";" + string(entity_uniqueid`j') + ";") == 0
			
			* CEO entities only (unique)
			replace all_ceo_entities = ///
				cond( all_ceo_entities == "", ///
					  string(entity_uniqueid`j'), ///
					  all_ceo_entities + ";" + string(entity_uniqueid`j') ) ///
				if  hospital_ceo`j' == 1 ///
				&  !missing(entity_uniqueid`j') ///
				&  strpos(";" + all_ceo_entities + ";",   ///
						 ";" + string(entity_uniqueid`j') + ";") == 0	
		
		}
		
		* Lagged entities variable: entities_prev
		bysort contact_uniqueid (year): gen entities_prev = all_entities[_n-1]
		gen entities_prior_all = ""
		bysort contact_uniqueid (year): replace entities_prior_all = ///
			cond(!missing(entities_prev), entities_prev[_n-1]+";"+entities_prev, entities_prev[_n-1])
			
		* Make an "added CEOs" variable	and counters
		bysort contact_uniqueid (year): gen ceos_prev = all_ceo_entities[_n-1]
		gen added_ceo_entities = ""
		gen n_ceo_added = 0
		gen n_ceo_add_overlap = 0
		gen n_ceo_ent_overlap = 0
		gen covered_ceo_ent = "" // temporary var to track the entities I've looped over
		forvalues j = 1/`max_role' {
			
			* counter for whether current ceo entities overlap with prior ceo entities
			replace n_ceo_ent_overlap = n_ceo_ent_overlap + 1 ///
					if  hospital_ceo`j' == 1 ///
					&  !missing(entity_uniqueid`j') ///
					&  strpos(";" + covered_ceo_ent + ";",   /// not counted yet
							 ";" + string(entity_uniqueid`j') + ";") == 0 ///
					&  strpos(";" + ceos_prev + ";", /// ID was in last year list of any entity
							 ";" + string(entity_uniqueid`j') + ";") > 0
			
			* track the unique entities we have counted so far
			replace covered_ceo_ent = ///
				cond( covered_ceo_ent == "", ///
					  string(entity_uniqueid`j'), ///
					  covered_ceo_ent + ";" + string(entity_uniqueid`j') ) ///
				if  hospital_ceo`j' == 1 ///
				&  !missing(entity_uniqueid`j') ///
				&  strpos(";" + covered_ceo_ent + ";",   ///
						 ";" + string(entity_uniqueid`j') + ";") == 0
			
			* added CEOs counter
			replace n_ceo_added = n_ceo_added + 1 ///
				if  hospital_ceo`j' == 1 /// conditions to do the replacement:
					&  !missing(entity_uniqueid`j') /// the ID isn't missing
					&  strpos(";" + added_ceo_entities + ";",   /// ID is not already in the list
						 ";" + string(entity_uniqueid`j') + ";") == 0 ///
					&  	strpos(";" + ceos_prev + ";",   /// ID wasn't in last year's list
						 ";" + string(entity_uniqueid`j') + ";") == 0 ///
					&  	row_in_person > 1 // not the first year for the person 
			
			* counter for "added CEO entity was in the entities_prev" overlap
			replace n_ceo_add_overlap = n_ceo_add_overlap + 1 ///
				if hospital_ceo`j' == 1 /// conditions to do the replacement:
					&  !missing(entity_uniqueid`j') /// the ID isn't missing
					&  strpos(";" + added_ceo_entities + ";",   /// ID is not already in the list
						 ";" + string(entity_uniqueid`j') + ";") == 0 ///
					&  	strpos(";" + ceos_prev + ";",   /// ID wasn't in last year's list
						 ";" + string(entity_uniqueid`j') + ";") == 0 ///
					&  	row_in_person > 1 /// not the first year for the person 
					&  strpos(";" + entities_prev + ";", /// ID was in last year list of any entity
						 ";" + string(entity_uniqueid`j') + ";") > 0 		
						 
			* append to the added ceos list 
			replace added_ceo_entities = ///
				cond( added_ceo_entities == "", ///
					  string(entity_uniqueid`j'), ///
					  added_ceo_entities + ";" + string(entity_uniqueid`j') ) ///
				if  hospital_ceo`j' == 1 /// conditions to do the replacement:
					&  !missing(entity_uniqueid`j') /// the ID isn't missing
					&  strpos(";" + added_ceo_entities + ";",   /// ID is not already in the list
						 ";" + string(entity_uniqueid`j') + ";") == 0 ///
					&  	strpos(";" + ceos_prev + ";",   /// ID wasn't in last year's list
						 ";" + string(entity_uniqueid`j') + ";") == 0 ///
					&  	row_in_person > 1 // not the first year for the person 
		}
		drop covered_ceo_ent
		
		* Count (overlapping) CEO roles by year 	
		* how many total CEO entities?
			* how many overlap with previous CEO entities (retaining roles)?
		* how many added CEO entities?
			* how many overlap with previous entities (any role)? i.e. is the CEO role at a new place?
		
		* Previous year variable
		bysort contact_uniqueid (year): gen year_prev = year[_n-1]
		
		* any prior year promotion variable
		gen promotion_type = ""
			* Internal promotion: CEO entity appears in any prior entity list
			replace promotion_type = "internal" if gained_ceo == 1 ///
				& n_ceo_add_overlap >= 1 // at least one promotion to CEO where they already worked	
			* External promotion: CEO entity not in any prior entity list
			replace promotion_type = "external" if gained_ceo == 1 ///
				& n_ceo_add_overlap == 0 // zero promotions to CEO where they already worked	
		
		* consecutive promotions variable
		gen promotion_type_consec = ""
			* Internal promotion: CEO entity appears in any prior entity list
			replace promotion_type_consec = "internal" if gained_ceo == 1 ///
				& n_ceo_add_overlap >= 1 /// at least one promotion to CEO where they already worked
				& year_prev == year - 1
			* External promotion: CEO entity not in any prior entity list
			replace promotion_type_consec = "external" if gained_ceo == 1 ///
				& n_ceo_add_overlap == 0 /// zero promotions to CEO where they already worked
				& year_prev == year - 1
			replace promotion_type_consec = "missing year" if gained_ceo == 1 ///
				& year_prev != year - 1
						
		* move type 
		gen move_type = ""
			* External
			replace move_type = "external" if gained_ceo == 0 /// not the first CEO promotion
				& row_in_person > 1 /// not first year for person
				& ceo_this_year == 1 /// CEO this year
				& ceo_last_year == 1 /// CEO last year
				& n_ceo_added >= 1 /// added a CEO role this year
				& n_ceo_add_overlap == 0 /// all CEO entities are new (didn't work there last obs in any role)
				& n_ceo_ent_overlap == 0 // did not retain any old CEO roles
			replace move_type = "internal" if gained_ceo == 0 /// not the first CEO promotion
				& row_in_person > 1 /// not first year for person
				& ceo_this_year == 1 /// CEO this year
				& ceo_last_year == 1 /// CEO last year
				& n_ceo_added >= 1 /// added a CEO role this year
				& n_ceo_added == n_ceo_add_overlap // already worked at all the places they are now CEO (internal only)
			replace move_type = "lateral" if gained_ceo == 0 /// not the first CEO promotion
				& row_in_person > 1 /// not first year for person
				& ceo_this_year == 1 /// CEO this year
				& ceo_last_year == 1 /// CEO last year
				& n_ceo_added >= 1 /// added a CEO role this year
				& n_ceo_add_overlap < n_ceo_added /// they have at least one external promotion
				& n_ceo_ent_overlap > 0 // they are retaining at least one old CEO role
				
		* move type (consecutive)
		gen move_type_consec = ""
			* External
			replace move_type_consec = "external" if gained_ceo == 0 /// not the first CEO promotion
				& year_prev == year-1 /// consecutive years
				& row_in_person > 1 /// not first year for person
				& ceo_this_year == 1 /// CEO this year
				& ceo_last_year == 1 /// CEO last year
				& n_ceo_added >= 1 /// added a CEO role this year
				& n_ceo_add_overlap == 0 /// all CEO entities are new (didn't work there last obs)
				& n_ceo_ent_overlap == 0 // did not retain any old CEO roles
			replace move_type_consec = "internal" if gained_ceo == 0 /// not the first CEO promotion
				& year_prev == year-1 /// consecutive years
				& row_in_person > 1 /// not first year for person
				& ceo_this_year == 1 /// CEO this year
				& ceo_last_year == 1 /// CEO last year
				& n_ceo_added >= 1 /// added a CEO role this year
				& n_ceo_added == n_ceo_add_overlap // already worked at all the places they are now CEO
			replace move_type_consec = "lateral" if gained_ceo == 0 /// not the first CEO promotion
				& year_prev == year-1 /// consecutive years
				& row_in_person > 1 /// not first year for person
				& ceo_this_year == 1 /// CEO this year
				& ceo_last_year == 1 /// CEO last year
				& n_ceo_added >= 1 /// added a CEO role this year
				& n_ceo_add_overlap < n_ceo_added /// they have at least one external promotion
				& n_ceo_ent_overlap > 0 // they are retaining at least one old CEO role	
			replace move_type_consec = "missing year" if gained_ceo == 0 /// not the first CEO promotion
				& year_prev != year-1 /// consecutive years
				& row_in_person > 1 /// not first year for person
				& ceo_this_year == 1 /// CEO this year
				& ceo_last_year == 1 /// CEO last year
				& n_ceo_added >= 1 // added a CEO role this year
				
		* COMBINE promotions and moves together
		* any prev year
		gen career_transition_type = ""
			replace career_transition_type = "internal promotion" ///
				if gained_ceo == 1 ///
				& promotion_type == "internal"
			replace career_transition_type = "external promotion" ///
				if gained_ceo == 1 ///
				& promotion_type == "external"
			replace career_transition_type = "internal move" ///
				if gained_ceo == 0 ///
				& move_type == "internal"
			replace career_transition_type = "external move" ///
				if gained_ceo == 0 ///
				& move_type == "external"
			replace career_transition_type = "lateral move" ///
				if gained_ceo == 0 ///
				& move_type == "lateral"
		* consecutive only
		gen career_transition_type_consec = ""
			replace career_transition_type_consec = "internal promotion" ///
				if gained_ceo == 1 ///
				& promotion_type_consec == "internal"
			replace career_transition_type_consec = "external promotion" ///
				if gained_ceo == 1 ///
				& promotion_type_consec == "external"
			replace career_transition_type_consec = "internal move" ///
				if gained_ceo == 0 ///
				& move_type_consec == "internal"
			replace career_transition_type_consec = "external move" ///
				if gained_ceo == 0 ///
				& move_type_consec == "external"
			replace career_transition_type_consec = "lateral move" ///
				if gained_ceo == 0 ///
				& move_type_consec == "lateral"
		
		* add definition variable 		
		gen definition = "First time becoming CEO at a hospital previously worked at (non-CEO)" ///
			if career_transition_type == "internal promotion"
			replace definition = "First time becoming CEO at a hospital never worked at before" ///
				if career_transition_type == "external promotion"
			replace definition = "Already CEO; add CEO role at a hospital previously worked at (non-CEO)" ///
				if career_transition_type == "internal move"
			replace definition = "Already CEO; add CEO role at a hospital never worked at before" ///
				if career_transition_type == "lateral move"
			replace definition = "Drop previous CEO roles; become CEO at a hospital never worked at before" ///
				if career_transition_type == "external move"
		
		* add example variable
		gen example = "Year t−1: CFO at Hospital B → Year t: CEO at Hospital B" ///
			if career_transition_type == "internal promotion"
			replace example = "Year t−1: No role at Hospital B → Year t: CEO at Hospital B" ///
				if career_transition_type == "external promotion"
			replace example = "Year t−1: CEO at Hospital A, CFO at Hospital B → Year t: CEO at A and B" ///
				if career_transition_type == "internal move"
			replace example = "Year t−1: CEO at Hospital A → Year t: CEO at A and B" ///
				if career_transition_type == "lateral move"
			replace example = "Year t−1: CEO at Hospital A → Year t: CEO at Hospital B (no longer CEO at A)" ///
				if career_transition_type == "external move"
			
		* save xwalk file to pull into the full dataset		
		tempfile career_transitions_xwalk
		save `career_transitions_xwalk'
		
		* make descriptive tables	
			*keep if !missing(career_transition_type)
			* number of career transitions
				gen n_transitions = 1 if !missing(career_transition_type)
			* number of unique individuals by career transition
				bysort contact_uniqueid career_transition_type career_transition_type: gen n_individuals = 1 if _n ==1 & !missing(career_transition_type)
			* number of unique CEOs regardless of transition
				bysort contact_uniqueid: gen total_ceos_counter = 1 if _n == 1
				egen total_ceos = total(total_ceos_counter)
			collapse (rawsum) n_transitions (rawsum) n_individuals (mean) total_ceos (firstnm) definition example, by(career_transition_type)
			gen share_of_ceos = n_individuals/total_ceos
			drop if career_transition_type == ""
			
			* order rows
			gen order = 1 if career_transition_type == "internal promotion"
				replace order = 2 if career_transition_type == "external promotion"
				replace order = 3 if career_transition_type == "internal move"
				replace order = 4 if career_transition_type == "lateral move"
				replace order = 5 if career_transition_type == "external move"
			sort order
				
			* export: set up latex table
			tempname f
			file open `f' using "${overleaf}/notes/CEO Descriptives/figures/career_transitions_table.tex", write replace
		
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
					local ct = career_transition_type[`i']
					local defn = definition[`i']
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
	merge m:1 contact_uniqueid year using `career_transitions_xwalk', nogen keepusing(contact_uniqueid year career_transition_type gained_ceo added_ceo_entities) keep(1 3)
	* note that this information is currently at the person-year level
	* it won't be specific to the HIMSS entity or role in the main data
	* could keep the CEO entities and reshape them? 
		* Or otherwise rewrite the code to identify the specific entities concerned in each transition and merge the crosswalk onto them
		
	rename career_transition_type career_transition_type_yr // will be the same for all person obs in a year
	gen career_transition_type = ""
	replace career_transition_type = career_transition_type_yr if ///
		strpos(";" + added_ceo_entities + ";", ";" + string(entity_uniqueid) + ";") > 0 ///
		& char_ceo == 1 // will actually correspond to the right CEO promotion for that year
		
* save temp file _______________________________________________________________

save "${dbdata}/derived/temp/indiv_file_contextual.dta", replace
