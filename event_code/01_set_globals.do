/* SET_GLOBALS *****************************************************************

Program name: 	01_set_globals.do
Programmer: 	Julia Paris, modified by Katherine Papen

Goal: 			Set globals for Hospital CEOs analysis
				Write programs that will be re-used throughout analysis

*******************************************************************************/

* SET GLOBALS __________________________________________________________________
	 
	* Dropbox data
	glob dbdata "${dropbox}/_data"
	
	* add other globals here 
	
* SET WORKING DIRECTORY ________________________________________________________
	
	cd "${dropbox}"
	
* WRITE A PROGRAM TO ENSURE THAT SETUP IS COMPLETE _____________________________

	program define check_setup
	
		* check that the working directory is correct
		local current_dir = c(pwd)
		if "`current_dir'" != "${dropbox}" {
			di as error "The current working directory is not correct. It is `current_dir'. Ensure that the setup code has been executed properly."
			exit 1 
		} 
		else {
			di as result "Working directory is correct: `current_dir'"
		}
		
		* check that globals are defined
		cap confirm file "${dbdata}"
		if _rc {
			di as error "The data directory ${dbdata} does not exist."
			exit 1
		}
	
	end
	* this will be run at the start of all other programs. 

* WRITE A PROGRAM TO EXPORT MERGE RESULTS ______________________________________
	
	program export_merge 
	
		syntax , Folder(string) Filename(string) Target(string) 
		
		* write merge results
		tempname f
		file open `f' using "${overleaf}/notes/`folder'/figures/`filename'.tex", write replace
		file write `f' "\begin{tabular}{lr}" _n
		file write `f' "Group & Count \\" _n
		file write `f' "\hline" _n

		quietly levelsof `target', local(vals)
		foreach v of local vals {
			count if `target' == `v'
			local n = r(N)
			local label : label (`target') `v'
			file write `f' "`label' & `n' \\" _n
		}

		file write `f' "\end{tabular}" _n
		file close `f'
		
	end
	
* WRITE A PROGRAM TO RESTRICT TO HOSPITAL SAMPLE _______________________________ 


	program restrict_hosp_sample 
	
		keep if is_hospital == 1
		gen partofsample = 1 if inlist(type,"General Medical","General Medical & Surgical","Critical Access")
		bysort entity_uniqueid: egen ever_partofsample = max(partofsample)
		keep if ever_partofsample == 1
		drop ever_partofsample
	


	end

* WRITE A PROGRAM TO RESTRICT TO HOSPITAL SAMPLE _______________________________ 

	program make_target_sample

        * get target relative time
		bys entity_uniqueid (year): egen tar_event_year = min(cond(tar == 1, year, .))
        gen tar_reltime = year - tar_event_year
        gen tar_treated = year >= tar_event_year

        * get never treated flags
        bys entity_uniqueid: egen ever_tar_1 = max(tar == 1)
        gen never_tar = (ever_tar_1 == 0)

        bys entity_uniqueid: egen ever_acq_1 = max(acq == 1)
        gen never_acq = (ever_acq_1 == 0)

        gen never_m_and_a = never_tar & never_acq

        * get flags for sample
        gen full_tar_sample_temp = ever_tar_1
        gen temp_sample_2_years = ever_tar_1 & (tar_event_year >= 2011 & tar_event_year <= 2015)
        gen temp_sample_3_years = ever_tar_1 & (tar_event_year >= 2012 & tar_event_year <= 2014)

        * create flags for pre/post sample
        bys entity_uniqueid: egen ever_second_tar = max(tar == 1 & tar_reltime > 0)

        gen full_treated_sample = full_tar_sample_temp & !ever_second_tar
        gen balanced_2_year_sample = temp_sample_2_years & !ever_second_tar
		gen balanced_3_year_sample = temp_sample_3_years & !ever_second_tar
		
		drop full_tar_sample_temp temp_sample_2_years temp_sample_3_years ever_second_tar

    end
	
* WRITE A PROGRAM TO RESTRICT TO GET CUMULATIVE OUTCOMES _______________________________ 

	program make_outcome_vars
		
		// merge in previous year's CEO info
		preserve
		
			use "${dbdata}/derived/temp/updated_trajectories.dta", clear
			drop entity_aha
			rename aha_id entity_aha
			keep entity_uniqueid entity_aha year contact_uniqueid confirmed
			keep if !missing(entity_aha)

			bys entity_aha year: egen n_unique_contacts = nvals(contact_uniqueid)
			gen byte multi_contact = n_unique_contacts > 1
			count if multi_contact == 1
			assert r(N) == 0
			drop if multi_contact == 1
			bysort entity_aha year: keep if _n == 1

			tostring contact_uniqueid, replace

			sort entity_aha year
			
			by entity_aha: gen year_lag1    = year[_n-1]
			by entity_aha: gen year_lag2    = year[_n-2]
			by entity_aha: gen year_lag3    = year[_n-3]
			
			by entity_aha: gen contact_lag1 = contact_uniqueid[_n-1] if year_lag1 == year - 1
			by entity_aha: gen contact_lag2 = contact_uniqueid[_n-2] if year_lag2 == year - 2
			by entity_aha: gen contact_lag3 = contact_uniqueid[_n-3] if year_lag3 == year - 3
			
			by entity_aha: gen confirmed_lag1 = confirmed[_n-1] if year_lag1 == year - 1

			gen missing_ceo_lag1 = contact_lag1 == ""
			gen missing_ceo_lag2 = contact_lag2 == ""
			
			// get any turnover measures
			gen byte contact_changed_prev2yrs = ///
				((contact_lag1 != contact_uniqueid & !missing(contact_lag1) & year == year_lag1 + 1) | ///
				 (contact_lag2 != contact_uniqueid & !missing(contact_lag2) & year == year_lag2 + 2))
			gen byte contact_changed_prev3yrs = ///
				((contact_changed_prev2yrs & !missing(year_lag3)) | ///
				 (contact_lag3 != contact_uniqueid & !missing(contact_lag3) & year == year_lag3 + 3))
				 
			// get different CEO now than x years prior
			gen byte diff_contact_from_two_years = (contact_lag1 != contact_uniqueid & !missing(contact_lag1) & year == year_lag1 + 1)
			gen byte diff_contact_from_three_years = (contact_lag3 != contact_uniqueid & !missing(contact_lag3) & year == year_lag3 + 3)

			rename entity_aha aha_id
			keep aha_id year missing_ceo* contact_changed* diff_contact* ///
			contact_lag1 confirmed_lag1 contact_lag3
			bys aha_id year: keep if _n == 1
			
			destring aha_id, replace
			tempfile prev_ceos
			save `prev_ceos', replace

		restore
		
		destring aha_id, replace
		merge m:1 aha_id year using `prev_ceos', gen(_merge_turnover)
		
		tostring contact_uniqueid, replace
		
		gen ceo_turnover1 = (contact_lag1) != contact_uniqueid
		replace ceo_turnover1 = . if year == 2009
		replace ceo_turnover1 = . if missing(confirmed_lag1) | missing(confirmed) | confirmed_lag1 == 0 | confirmed == 0
		
		gen vacancy_turnover = (real(contact_lag1) > 0 & real(contact_uniqueid) < 0) | ///
                       (real(contact_lag1) < 0 & real(contact_uniqueid) > 0) ///
                       if !missing(real(contact_lag1)) & !missing(real(contact_uniqueid))		
		replace ceo_turnover1 = 1 if vacancy_turnover == 1
		
		keep if _merge_turnover == 3
		drop _merge_turnover
		
		// merge in trajectories
		preserve
		
			use "${dbdata}/derived/temp/updated_trajectories.dta", clear
			keep contact_uniqueid aha_id next_year *_future  future_* max_bdtot_* year_of_max_bdtot has_match_next_year
			rename (contact_uniqueid next_year) (contact_lag1 year)
			tostring contact_lag1, replace
			destring aha_id, replace
			
			duplicates tag contact_lag1 year, gen(dup)
			tab dup
			keep if dup == 0
						
			tempfile trajectories
			save `trajectories'
		restore
		merge m:1 contact_lag1 aha_id year using `trajectories', gen(_merge_trajectories)

		gen temp_prev_left = 1 if _merge_trajectories == 1
		replace temp_prev_left = 1 if exists_future == 0
		drop if _merge_trajectories == 2
		drop _merge_trajectories
		
		// merge in individual characteristics
		preserve 
		
		* Merge in female + md indicators
			use "${dbdata}/derived/temp/indiv_file_contextual.dta", clear
			keep contact_uniqueid year char_female char_md

			egen mode_female = mode(char_female), by(contact_uniqueid)
			egen mode_md = mode(char_md), by(contact_uniqueid)

			replace char_female = mode_female
			replace char_md = mode_md

			drop mode_female mode_md year
			duplicates drop
			
			tempfile indiv_traits
			save `indiv_traits'

		* Get world of contact_uniqueid that are CEOs
		use "${dbdata}/derived/temp/updated_individual_titles.dta", clear
		rename entity_aha aha_id
		keep aha_id year contact_uniqueid all_leader_flag ceo_himss_title_exact ceo_himss_title_fuzzy confirmed
		merge m:1 contact_uniqueid using `indiv_traits', gen(_char_merge)

			// confirm that ceo flag is unique
			keep if all_leader_flag == 1 & !missing(aha_id)
			bys aha_id year: egen n_unique_contacts = nvals(contact_uniqueid)
			gen multi_contact = n_unique_contacts > 1
			count if multi_contact == 1
			display r(N)
			assert r(N) == 4
			drop multi_contact
			
			tostring char_female, replace
			tostring char_md, replace
			
			// get lags
			sort aha_id year
			by aha_id: gen year_lag_1 = year[_n-1]
			by aha_id: gen year_lag_2 = year[_n-2]
			
			by aha_id: gen char_female_lag_1 = char_female[_n-1] if year_lag_1 == year - 1
			by aha_id: gen char_female_lag_2 = char_female[_n-2] if year_lag_2 == year - 2
			by aha_id: gen char_md_lag_1 = char_md[_n-1] if year_lag_1 == year - 1
			by aha_id: gen char_md_lag_2 = char_md[_n-2] if year_lag_2 == year - 2
			
			destring char_md*, replace
			destring char_female*, replace
			
			keep aha_id year* char_female* char_md*
			bys aha_id year: keep if _n == 1
			destring aha_id, replace

			tempfile individual_characteristics
			save `individual_characteristics'
		restore
		
		merge m:1 aha_id year using `individual_characteristics'
		keep if _merge == 3 | vacancy_turnover == 1
		drop _merge
			
    end
