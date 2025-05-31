/* SET_GLOBALS *****************************************************************

Program name: 	01_set_globals.do
Programmer: 	Julia Paris

Goal: 			Set globals for Hospital CEOs analysis

*******************************************************************************/

* SETUP ________________________________________________________________________

* Clear environment
	clear all 
	macro drop _all
	program drop _all
	set more off
	
* Set directories
	if "`c(username)'" == "juliaparis" {
		glob dropbox "/Users/juliaparis/Dropbox/hospital_ceos"
		glob github "/Users/juliaparis/GitHub/hospital-ceos-code"
		glob overleaf "/Users/juliaparis/Dropbox/Apps/Overleaf/Hospital CEOs"
	} // can add other users and file locations
	 
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
