/* MAKE_WIDE_TRAMINER.do ****************************************************

Program name: 	07_make_wide_traminer.do
Programmer: 	Julia Paris

Goal: 			Look at CEO trends around M&A events

*******************************************************************************/

* setup ________________________________________________________________________ 

* check setup is complete
	check_setup
	
* load data
	use "${dbdata}/derived/temp/indiv_file_contextual.dta", clear
	
* make status variable _________________________________________________________

/*
Notes to think about: 

*	What about hospitals that aren't in our sample, but they're "hosps"?
	I'm going to call them non-hospital facilities I think
	Because they are generally specialty/psychiatric/etc.
	Not general hosps
	
* 	What about people who work at SHHS? 
	All of them will be hospital employees, if they work at the system itself
	
* 	Remove the requirement of the is_hospital flag? Do we need uniqueness here?
	
*/

* Make hospital variable for these purposes
	* actual hospital facilities
	gen partofsample = 1 if is_hospital ==1 & inlist(type,"General Medical","General Medical & Surgical","Critical Access")
	* supplement with SHHS
	replace partofsample = 1 if entity_type == "Single Hospital Health System"
	
* now make status_entity variable
	gen status_entity = "Hospital" if partofsample == 1
		replace status_entity = "System" if entity_type == "IDS/RHA"
		replace status_entity = "Non-Hospital, Non-System Facility" if missing(status_entity)
		

* make status_role variable 
	* ORDER: CEO > CFO > COO > CIO > CMO > CNIS > CSIO > Head of Facility > President > VP > Medical Staff Chief > Other Directors > CNH > Other Head
	gen status_role = "CEO" if char_ceo == 1
		* other C-suite ("officers")
		replace status_role = "CFO" if regexm(title_standardized,"CFO:") & missing(status_role)
		replace status_role = "COO" if regexm(title_standardized,"COO:") & missing(status_role)
		replace status_role = "CIO" if regexm(title_standardized,"CIO:") & missing(status_role)
		replace status_role = "CMO" if regexm(title_standardized,"Chief Medical Officer") & missing(status_role)
		replace status_role = "CCO" if regexm(title_standardized,"Chief Compliance Officer") & missing(status_role)
		replace status_role = "CNIS" if regexm(title_standardized,"CNIS:") & missing(status_role)
		
		* if all C-suite missing: 
		replace status_role = "HeadofFacility" if regexm(title_standardized,"Head of Facility") & missing(status_role)
		replace status_role = "President" if regexm(title,"President") & missing(status_role)
		replace status_role = "VP" if regexm(title,"VP|Vice President") & missing(status_role)
		
		* other "chief"
		replace status_role = "MedicalStaffChief" if regexm(title_standardized,"Medical Staff Chief") & missing(status_role)
		
		* "directors"
		replace status_role = "OtherDirector" if regexm(title_standardized,"Director|Adm Dir|Med Dir") & missing(status_role)
		
		* "heads"
		replace status_role = "CNH" if regexm(title_standardized,"Chief Nursing Head") & missing(status_role)
		replace status_role = "OtherHead" if regexm(title_standardized,"Head") & missing(status_role)
	
* make an ordinal ranking of the jobs/systems
	* system > hospital > non
	gen status_entity_rank = 1 if status_entity == "System" 
		replace status_entity_rank = 2 if status_entity == "Hospital" 
		replace status_entity_rank = 3 if status_entity == "Non-Hospital, Non-System Facility" 
		
	* facility: 
	local role_order = "CFO COO CIO CMO CCO CNIS HeadofFacility President VP MedicalStaffChief OtherDirector CNH OtherHead"
	
	gen status_role_rank = .
	local i = 1
	foreach var in `role_order' {
		replace  status_role_rank = `i' if status_role == "`var'"
		local i = `i' + 1
	}
	
* sort within year: keep highest-ranked facility first
	bysort contact_uniqueid year (status_entity_rank status_role_rank): gen keep = 1 if _n == 1
	
* make combined variable
	gen traminer_status = status_role+", "+status_entity

* make dataset for hosp CEOs in 2017 ___________________________________________

* make a flag for hospital CEO in 2017

	gen hosp_ceo_2017 = 1 if year == 2017 & hospital_ceo == 1 // does the hospital_ceo flag depend on is_hospital? Matters if we take that requirement away

	bysort contact_uniqueid: egen was_hosp_ceo_2017 = max(hosp_ceo_2017)

	*preserve
		keep if was_hosp_ceo_2017 ==1
		keep if keep == 1
		drop was_hosp_ceo_2017 keep 
		
		* reshape
		keep contact_uniqueid year traminer_status
		
		reshape wide traminer_status, i(contact_uniqueid) j(year)
		* problem: mismatch between hospital_ceo variable and the roles that I have defined?
		* or is it the way I have assigned role ranks? 


	restore



* make dataset for ever-hosp CEOs ______________________________________________ 
