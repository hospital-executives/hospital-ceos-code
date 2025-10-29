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
        gen restricted_tar_sample_temp = ever_tar_1 & (tar_event_year >= 2009 & tar_event_year <= 2015)

        * create flags for pre/post sample
        bys entity_uniqueid: egen ever_second_tar = max(tar == 1 & tar_reltime > 0)

        gen full_treated_sample = full_tar_sample_temp & !ever_second_tar
        gen restricted_treated_sample = restricted_tar_sample_temp & !ever_second_tar

    end