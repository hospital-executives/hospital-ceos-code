/* SET_GLOBALS *****************************************************************

Program name: 	01_set_globals.do
Programmer: 	Julia Paris

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
