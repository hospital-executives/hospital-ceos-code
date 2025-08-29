/* MASTER **********************************************************************

Program name: 	00_master.do
Programmer: 	Julia Paris

Goal: 			Master do-file for Hospital CEOs analysis.

*******************************************************************************/

* SETUP ________________________________________________________________________

* Clear environment
	clear all 
	macro drop _all
	program drop _all
	set more off
	
* Set directories
	if "`c(username)'" == "juliaparis" {
		glob dropbox "/Users/juliaparis/Library/CloudStorage/Dropbox/hospital_ceos"
		glob github "/Users/juliaparis/GitHub/hospital-ceos-code"
		glob overleaf "/Users/juliaparis/Library/CloudStorage/Dropbox/Apps/Overleaf/Hospital CEOs"
	} // can add other users and file locations

* RUN PROGRAMS _________________________________________________________________	
	
* Set globals
	do "${github}/analysis_code/01_set_globals.do"
	
* Merge in M&A data
	do "${github}/analysis_code/02_add_mergers_acq.do"
	
* Harmonize system ID variables
	do "${github}/analysis_code/03_harmonize_sysid.do"
	
* Build individual file
	do "${github}/analysis_code/04_build_indiv_file.do"	
	
* CEO descriptives
	do "${github}/analysis_code/05_ceo_descriptives.do"

* M&A descriptives
	do "${github}/analysis_code/06_mergers_acq_descriptives.do"


