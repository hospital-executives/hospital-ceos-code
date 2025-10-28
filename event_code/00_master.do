/* MASTER **********************************************************************

Program name: 	00_master.do
Programmer: 	Julia Paris, modified by Katherine Papen for event_code

Goal: 			Master do-file for Hospital CEOs event-study.

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
	}
	if "`c(username)'" == "katherinepapen" {
		glob dropbox "/Users/katherinepapen/Library/CloudStorage/Dropbox/hospital_ceos"
		glob github "/Users/katherinepapen/github/hospital-ceos-4"
		glob overleaf "/Users/katherinepapen/Library/CloudStorage/Dropbox/Apps/Overleaf/Hospital CEOs"
	}
	// can add other users and file locations

* RUN PROGRAMS _________________________________________________________________	
	
* Set globals
	do "${github}/analysis_code/01_set_globals.do"
	