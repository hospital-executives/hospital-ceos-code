/* HARMONIZE_SYSID *************************************************************

Program name: 	03_harmonize_sysid.do
Programmer: 	Julia Paris

Goal: 			Clean system ID variable from Cooper et al data
				Clean and reduce data to xwalk for individual-level analyses
				Save xwalk

*******************************************************************************/

* check setup
	check_setup

* quick catalogue of the system ID variables:
	* system_id: from HIMSS, numeric IDs that differ from AHA
	* sysid_orig: from AHA/HIMSS data (from AHA originally)
	* sysname: from AHA/HIMSS data (from AHA originally)
	* sysid: cleaned AHA system ID from cooper et al, merged onto entity_aha
	* sysid_campus: cleaned AHA system ID from cooper et al, merged onto campus_aha
	
* load data
	use "${dbdata}/derived/temp/merged_ma_nonharmonized.dta", clear
	
* challenges to think about: what to do with unmerged AHA/HIMSS systems?
	
* creating our own variable

* ion each year, find the modal entity_uniqueid_parent for each sysid, and vice versa
* then check if they agree on the specific system for different observations. 	

* FIND MODAL OBS
* start by only looking at general and CAH hosps - TEST SECTION
	preserve 
		keep if is_hospital == 1 // only use hospital observations for this part
		
		* restrict to hospitals that we expect should have matched to M&A data
		gen partofsample = 1 if inlist(type,"General Medical","General Medical & Surgical","Critical Access")
		bysort entity_uniqueid: egen ever_partofsample = max(partofsample)
		keep if ever_partofsample == 1
		drop ever_partofsample
		codebook sysid
		
		* collapse to counts by sysid entity_uniqueid_parent
		gen count = 1
		collapse (count) count, by(entity_uniqueid_parent sysid year)
// 		tempfile collapsed
// 		save `collapsed'

		* what share of parent_ids correspond to multiple sysids in a year?
		bysort entity_uniqueid_parent year: gen sysid_count = _N if _n == _N & !missing(entity_uniqueid_parent) // parent id never missing in this sample
		* 95% of the time, a parent corresponds to one system (missing system counts as one system too). 
		
		* what share of sysids correspond to multiple parent_ids in a year?
		bysort sysid year: gen parentid_count = _N if _n == _N & !missing(sysid)
		* 97% of the time when sysid isn't missing, a sysid corresponds to one parent (missing parent counts as one system too). 
		
		* what does it mean when there isn't a unique mapping?
			* my current guess is that an acquisition happened that isn't reflected in the parentid field. 
			* so the hospital merges to a new sysid but the old parentid is still attached to it
		
	restore

* Implementing current strategy 
	preserve 
		* restrict to hospitals that we expect should have matched to M&A data
		restrict_hosp_sample // defined in "set globals"
		
		tab type
		tab type if missing(sysid)
		// test if always missing?
	
		* collapse to counts by sysid entity_uniqueid_parent
		gen count = 1
		collapse (count) count, by(entity_uniqueid_parent sysid year)
		tempfile collapsed
		save `collapsed'
		
		* modal system_id for each sysid
		bysort sysid: gen countalt_parentid = _N
		bysort sysid (count): keep if _n ==_N
		rename entity_uniqueid_parent modal_entity_uniqueid_parent
		rename count ctmod_entity_uniqueid_parent
		save "${dbdata}/derived/temp/modal_parentid_bysysid.dta", replace
	
		* modal sysid for each system_id
		use `collapsed', clear
		bysort entity_uniqueid_parent: gen countalt_sysid = _N
		bysort entity_uniqueid_parent (count): keep if _n ==_N
		rename sysid modal_sysid
		rename count ctmod_sysid
		save "${dbdata}/derived/temp/modal_sysid_byparentid.dta", replace
	restore
		
* merge into full data		
	merge m:1 sysid using "${dbdata}/derived/temp/modal_system_id_bysysid.dta", nogen
	merge m:1 entity_uniqueid_parent using "${dbdata}/derived/temp/modal_sysid_byparentid.dta", nogen
	
	
* create system change variables _______________________________________________

	* im going to create a filled sysid variable that uses campus information when entity is missing and campus isn't
// 	gen sysid_harmonized = sysid
// 	replace sysid_harmonized = sysid_campus if missing(sysid) & !missing(sysid_campus)
	* unclear if I want to use this
	
	* first, make a variable to show when system is different from prior year within entity_uniqueid
	foreach sysvar in sysid system_id {
		bysort entity_uniqueid (year): gen syschng_`sysvar' = `sysvar' != `sysvar'[_n-1] if _n > 1
	}
	* replace uncertain years with missing 
		* then any years following those missings also have to be missing because we didn't have system info for prior year
	* system_id_ma missing means we really don't have any information about it (didn't merge)
	replace syschng_system_id = . if missing(system_id)
		replace syschng_system_id = . if missing(system_id[_n-1])
	* if _merge_entity_aha == 1, then missing sysid means that we don't have information
	replace syschng_sysid = . if _merge_entity_aha == 1
		replace syschng_sysid = . if _merge_entity_aha[_n-1] == 1
 	
	foreach sysvar in system_id sysid {
		* generate variable to count total # of cumulative system changes
		gen syschng_ct_`sysvar' = 0 if !missing(syschng_`sysvar')
		* replace with 1 system change if the second non-missing system observation is actually a change
		bysort entity_uniqueid (year): replace syschng_ct_`sysvar' = 1 if syschng_`sysvar'==1 & missing(syschng_ct_`sysvar'[_n-1])
		* pull last year's number forward
		bysort entity_uniqueid (year): replace syschng_ct_`sysvar' = syschng_ct_`sysvar'[_n-1]+syschng_`sysvar' if !missing(syschng_ct_`sysvar'[_n-1])
//	think about what to do when the change variable is missing
	}
	
* acquisition descriptives _____________________________________________________ 

	* mean number of acquisitions over period by entity_uniqueid
	preserve
		keep if is_hospital ==1 & _merge_entity_aha == 3
		collapse (rawsum) syschng* tar, by(year)
		graph bar syschng_system_id syschng_sysid tar, over(year) ///
			legend(position(bottom) label(1 "HIMSS Data - Sytem ID Change") label(2 "M&A Data - Sytem ID Change") label(3 "M&A Data Target")) ///
			title("Count of System M&A Events by Year") ///
			blabel(bar, size(vsmall))
		graph export "${overleaf}/notes/M&A Merge/figures/counts_syschng_byyear.pdf", as(pdf) name("Graph") replace
	restore
	* share of non-missing observations in a given year with an acquisition event
	preserve
		keep if is_hospital ==1 & _merge_entity_aha == 3
		collapse syschng* tar, by(year)
		graph bar syschng_system_id syschng_sysid tar, over(year) ///
			legend(position(bottom) label(1 "HIMSS Data - Sytem ID Change") label(2 "M&A Data - Sytem ID Change") label(3 "M&A Data Target")) ///
			title("Share of Facilities With System M&A Events by Year") ///
			blabel(bar, size(vsmall) format(%4.3f))
		graph export "${overleaf}/notes/M&A Merge/figures/shares_syschng_byyear.pdf", as(pdf) name("Graph") replace
	restore
	
* make a forprofit variable that excludes government hospitals
	gen forprofit = aha_own_fp
	replace forprofit = aha_own_fp_campus if missing(aha_own_fp) & !missing(aha_own_fp_campus)
	replace forprofit = . if aha_own_gov == 1
	replace forprofit = . if missing(aha_own_gov) & aha_own_gov_campus == 1
	
* make an ownership variable that includes govt
	gen gov_priv_type = 1 if aha_own_gov == 1
	replace gov_priv_type = 3 if aha_own_fp == 1
	replace gov_priv_type = 4 if aha_own_np == 1
	replace gov_priv_type = 1 if missing(aha_own_gov) & aha_own_gov_campus == 1
	replace gov_priv_type = 3 if missing(aha_own_fp) & aha_own_fp_campus == 1
	replace gov_priv_type = 4 if missing(aha_own_np) & aha_own_np_campus == 1
	
* how many FP/NFP conversions?
	bysort entity_uniqueid (year): gen test_to_fp = forprofit == 1 & forprofit[_n-1] == 0
		replace test_to_fp = . if missing(forprofit) | missing(forprofit[_n-1])
	bysort entity_uniqueid (year): gen test_to_nfp = forprofit == 0 & forprofit[_n-1] == 1
		replace test_to_nfp = . if missing(forprofit) | missing(forprofit[_n-1])

	bysort entity_uniqueid (year): gen nfp_to_fp = tar == 1 & forprofit[_n-1] == 0 & forprofit[_n+1] == 1
		replace nfp_to_fp = . if tar != 1 | missing(forprofit[_n-1]) | missing(forprofit[_n+1])
	bysort entity_uniqueid (year): gen fp_to_nfp = tar == 1 & forprofit[_n-1] == 1 & forprofit[_n+1] == 0
		replace fp_to_nfp = . if tar != 1 | missing(forprofit[_n-1]) | missing(forprofit[_n+1])
	
	* graph
	preserve
		collapse (rawsum) nfp_to_fp fp_to_nfp, by(year)
		graph bar nfp_to_fp fp_to_nfp, over(year) ///
				legend(position(bottom) label(1 "NFP to FP") label(2 "FP to NFP")) ///
				title("Count of Converstions by Year") ///
				blabel(bar, size(vsmall))
			graph export "${overleaf}/notes/M&A Merge/figures/counts_conversions_byyear.pdf", as(pdf) name("Graph") replace
	restore
	
	* create example file for RA
// 	preserve
		* reduce sample 
		restrict_hosp_sample
		
		* clean variables
		keep entity_uniqueid entity_name entity_type medicarenumber entity_city entity_state type year entity_aha system_id sysname aha_sys_name sysid tar any acq _merge_entity_aha syschng_system_id forprofit aha_hos_name entity_uniqueid_parent entity_name_parent entity_type_parent syschng_sysid
		
		sort entity_uniqueid year
		
		* make it clear what comes from HIMSS
		foreach himssvar in medicarenumber system_id sysname {
			rename `himssvar' entity_`himssvar'
		}
		rename type entity_detail_type
	
		order entity_uniqueid year entity_name entity_type entity_detail_type entity_system_id entity_sysname entity_uniqueid_parent entity_type_parent entity_name_parent entity_medicarenumber entity_city entity_state entity_aha _merge_entity_aha aha_hos_name sysid aha_sys_name forprofit tar any acq syschng_sysid syschng_system_id 	
		
		* pre-make flag variables
		gen entity_misschange = tar ==1 & syschng_system_id == 0
		gen aha_misschange = tar == 0 & syschng_system_id == 1
		bysort entity_uniqueid: egen ever_entity_misschange = max(entity_misschange)
		bysort entity_uniqueid: egen ever_aha_misschange = max(aha_misschange)
		
		* save a version for RA
		save "${dbdata}/derived/temp/merge_checks_RA.dta", replace
		
		* check around for system-to-system acquisitions
			* tar ==1 and sysid changes and permanently disappears from the data
			
			* flag SYSIDs if it is their last year
			bysort sysid: egen last_sysid_year = max(year)
			replace last_sysid_year = . if last_sysid_year == 2017 // don't count end of sample
			* just note that we might be missing conversions in the last year
			replace last_sysid_year = . if missing(sysid)
			* problem is that a lot of individual facilities have a sysid only for them
			bysort sysid year: gen mult_sysid = 1 if _N > 1
			
			* make a variable for last entity year
			bysort entity_uniqueid: egen last_entity_year = max(year)
			replace last_entity_year = . if last_entity_year == 2017
			
			* make a first system-tar variable
			* should this be lagged one year?
			bysort entity_uniqueid (year): gen v1_tar_sys = tar ==1 ///
				& year[_n-1] == last_sysid_year[_n-1] ///
				& mult_sysid[_n-1] == 1
			bysort entity_uniqueid: egen ever_v1_tar_sys = max(v1_tar_sys)
			
			* make another system-tar variable that doesn't require tar == 1
			bysort entity_uniqueid (year): gen v2_tar_sys = year[_n-1] == last_sysid_year[_n-1] ///
				&  mult_sysid[_n-1] == 1 ///
				& entity_type_parent[_n-1] == "IDS/RHA" // only apply to former system-owned
			bysort entity_uniqueid: egen ever_v2_tar_sys = max(v2_tar_sys)
			
	
	restore
	
	
* reduce sample to info needed for crosswalking to indiv. file _________________
	 
* fix variable names - eventually want to do this early in the code
	
	* sysid -> sysid_ma
	cap rename sysid sysid_ma
	cap rename syschng_sysid syschng_sysid_ma
	cap rename syschng_ct_sysid syschng_ct_sysid_ma
	
	* medicarenumber -> ccn_himss
// 	destring medicarenumber, gen(ccn_himss)
	
// 	* mcrnum_y -> ccn_aha
// 	cap rename mcrnum ccn_aha
//	
// 	* make sure all the for-profit variables from PS data are correctly named
// 	foreach profvar in forprofit forprofit_lag forprofit_chng {
// 		cap rename `profvar' `profvar'_ps
// 	}
//	
* keep key variables
	keep 	entity_uniqueid year sysid_ma sysid_orig sysid_campus is_hospital ///
			aha_* tar acq any hsanum hrrnum _merge_entity_aha _merge_campus_aha  ///
			syschng_sysid_ma syschng_system_id syschng_ct_system_id syschng_ct_sysid_ma ///
			forprofit gov_priv_type *_parent
			
* make unique by entity_uniqueid year
	bysort entity_uniqueid year: keep if _n == 1
		* 0 duplicate observations dropped
			
* save a crosswalk for merging with large file
	save "${dbdata}/derived/temp/merged_ma_sysid_xwalk.dta", replace
	
* SYSTEM info __________________________________________________________________ 
	
	
* get forprofit system descriptives 
	* pull in modal sysid here? need to update below
	bysort entity_uniqueid_parent year forprofit: gen count_fp = _N
	drop if missing(forprofit)
	bysort entity_uniqueid_parent year (count_fp): keep if _n == _N
	keep entity_uniqueid_parent year forprofit
	rename entity_uniqueid_parent entity_uniqueid
	merge 1:1 entity_uniqueid year using "${dbdata}/derived/temp/systems_nonharmonized.dta", gen(_merge_sys) 
	gen himss_forprofit = entity_profitstatus == "For Profit" 
	replace himss_forprofit = . if missing(entity_profitstatus)
	* within a entity_uniqueid_parent year, assign most common non-missing for-profit status
	* check against HIMSS for profit variable
	gen sameprofit = himss_forprofit == forprofit
	tab sameprofit if !missing(himss_forprofit) & !missing(forprofit)
	
	* does HIMSS forprofit variable change from year to year?
	foreach var in forprofit himss_forprofit {
		bysort entity_uniqueid year: gen change`var' = `var' != `var'[_n-1] & !missing(`var') & !missing(`var'[_n-1])
	}
	tab change* // NEVER any changes within IDs - actually think this is wrong
	
	gen forprofit_imputed = forprofit
	tab forprofit, m
	replace forprofit_imputed = himss_forprofit if missing(forprofit)
	
	foreach var in forprofit_imputed {
		bysort entity_uniqueid year: gen change`var' = `var' != `var'[_n-1] & !missing(`var') & !missing(`var'[_n-1])
	}
	tab changeforprofit_imputed
	drop change* himss_forprofit
	
	drop forprofit
	gen forprofit = forprofit_imputed 
	
	save "${dbdata}/derived/temp/systems_nonharmonized_withprofit.dta", replace
	
* test whether there is a system change or a indiv. hosp change



	
* case studies _________________________________________________________________

exit

* jones memorial hospital AHA 6215260
ahanumber	medicarenumber	year	sameissystem	system_id_aha	sysid	sysname	sys_aha	system_id_ma	system_id_qtr_ma	system_id_yr_ma	sysid_final	merge_status
6215260	330096	2010		46569			6215260	573	573	573		Initially Merged
6215260	330096	2011		46569			6215260	573	573	573		Initially Merged
6215260	330096	2012		46569			6215260	573	573	573		Initially Merged
6215260	330096	2013		46569			6215260	573	573	573		Initially Merged
6215260	330096	2014		46569			6215260	573	573	573		Initially Merged
6215260	330096	2015		46569			6215260	143	143	143		Initially Merged
6215260	330096	2016		24554	223	University of Rochester Medical Center	6215260	143	143	143	223	Initially Merged
6215260	330096	2017	0	24554	223	University of Rochester Medical Center	6215260	143	143	143	223	Initially Merged

* nyack hospital
ahanumber	medicarenumber	year	sameissystem	system_id_aha	sysid	sysname	sys_aha	system_id_ma	system_id_qtr_ma	system_id_yr_ma	sysid_final	merge_status
6213720	330104	2010		26615	142	New York Presbyterian Healthcare System	6213720	63	63	63	142	Initially Merged
6213720	330104	2011		26615	142	New York Presbyterian Healthcare System	6213720	63	63	63	142	Initially Merged
6213720	330104	2012		26615	142	New York Presbyterian Healthcare System	6213720	63	63	63	142	Initially Merged
6213720	330104	2013		26615	142	New York Presbyterian Healthcare System	6213720	63	63	63	142	Initially Merged
6213720	330104	2014		26615			6213720	242	242	242	142	Initially Merged
6213720	330104	2015		26615			6213720	242	242	242	142	Initially Merged
6213720	330104	2016		26774	343	Montefiore Health System	6213720	242	242	242	343	Initially Merged
6213720	330104	2017	0	26774	343	Montefiore Health System	6213720	242	242	242	343	Initially Merged

* peconic bay medical center
ahanumber	medicarenumber	year	sameissystem	system_id_aha	sysid	sysname	sys_aha	system_id_ma	system_id_qtr_ma	system_id_yr_ma	sysid_final	merge_status	entity_name
6214195	330107	2010		29414			6214195	554	554	554		Initially Merged	peconic bay medical center
6214195	330107	2011		29414			6214195	554	554	554		Initially Merged	peconic bay medical center
6214195	330107	2012		29414			6214195	554	554	554		Initially Merged	peconic bay medical center
6214195	330107	2013		29414			6214195	554	554	554		Initially Merged	peconic bay medical center
6214195	330107	2014		29414	62	North Shore-Long Island Jewish Health System	6214195	554	554	554	62	Initially Merged	peconic bay medical center
6214195	330107	2015		32278	62	Northwell Health	6214195	554	554	554	62	Initially Merged	peconic bay medical center
6214195	330107	2016		32278	62	Northwell Health	6214195	472	472	472	62	Initially Merged	peconic bay medical center
6214195	330107	2017	0	32278	62	Northwell Health	6214195	472	472	472	62	Initially Merged	peconic bay medical center

* st joseph's health center
ahanumber	medicarenumber	year	sameissystem	system_id_aha	sysid	sysname	sys_aha	system_id_ma	system_id_qtr_ma	system_id_yr_ma	sysid_final	merge_status	entity_name
6214870	330140	2010		34090	5955	Sisters of Saint Francis	6214870	350	350	350	5955	Initially Merged	st. josephs hospital
6214870	330140	2011		34090	5955	Sisters of Saint Francis	6214870	350	350	350	5955	Initially Merged	st. josephs hospital
6214870	330140	2012		34090	5955	Sisters of Saint Francis	6214870	350	350	350	5955	Initially Merged	st. josephs hospital
6214870	330140	2013		34090	5955	Sisters of Saint Francis	6214870	350	350	350	5955	Initially Merged	st. josephs hospital
6214870	330140	2014		34090	906	Trinity Health	6214870	350	350	350	219	Initially Merged	st. josephs hospital health center
6214870	330140	2015		37952	906	Trinity Health	6214870	2664	2664	2664	219	Initially Merged	st. josephs hospital health center
6214870	330140	2016		37952	906	Trinity Health	6214870	2664	2664	2664	219	Initially Merged	st. josephs hospital health center
6214870	330140	2017	0	37952	906	Trinity Health	6214870	2664	2664	2664	219	Initially Merged	st. josephs hospital health center

* st. francis medical center
ahanumber	medicarenumber	year	sameissystem	system_id_aha	sysid	sysname	sys_aha	system_id_ma	system_id_qtr_ma	system_id_yr_ma	sysid_final	merge_status	entity_name	sysid_final_partial_sysname
6931790	050104	2010		36759	1075	Daughters of Charity Health System	6931790	33	33	33	1075	Initially Merged	st. francis medical center	VERITY HEALTH SYSTEM
6931790	050104	2011		36759	1075	Daughters of Charity Health System	6931790	33	33	33	1075	Initially Merged	st. francis medical center	VERITY HEALTH SYSTEM
6931790	050104	2012		36759	1075	Daughters of Charity Health System	6931790	33	33	33	198	Initially Merged	st. francis medical center	ASCENSION HEALTHCARE
6931790	050104	2013		36759	1075	Daughters of Charity Health System	6931790	33	33	33	198	Initially Merged	st. francis medical center	ASCENSION HEALTHCARE
6931790	050104	2014		36759	1075	Daughters of Charity Health System	6931790	33	33	33	198	Initially Merged	st. francis medical center	ASCENSION HEALTHCARE
6931790	050104	2015		36759	1075	Verity Health System	6931790	33	33	33	10012	Initially Merged	st. francis medical center	MEDICAL FACILITIES CORPORATION
6931790	050104	2016		36759	1075	Verity Health System	6931790	33	33	33	10012	Initially Merged	st. francis medical center	MEDICAL FACILITIES CORPORATION
6931790	050104	2017	0	36759	1075	Verity Health System	6931790	33	33	33	10012	Initially Merged	st. francis medical center	MEDICAL FACILITIES CORPORATION

* huntsville hospital health system - sysid_final 117
	* helen keller hospital
medicarenumber	year	system_id_aha	sysid	sysname	sys_aha	system_id_ma	sysid_final	merge_status	entity_name	sysid_final_partial_sysname
010019	2010	28250			6530880	1361		Initially Merged	helen keller hospital	
010019	2011	28250			6530880	1361		Initially Merged	helen keller hospital	
010019	2012	28250	117	Huntsville Hospital Health System	6530880	1361	117	Initially Merged	helen keller hospital	HUNTSVILLE HOSPITAL HEALTH SYSTEM
010019	2013	28250	117	Huntsville Hospital Health System	6530880	1361	117	Initially Merged	helen keller hospital	HUNTSVILLE HOSPITAL HEALTH SYSTEM
010019	2014	28250	117	Huntsville Hospital Health System	6530880	41	117	Initially Merged	helen keller hospital	HUNTSVILLE HOSPITAL HEALTH SYSTEM
010019	2015	28250	117	Huntsville Hospital Health System	6530880	41	117	Initially Merged	helen keller hospital	HUNTSVILLE HOSPITAL HEALTH SYSTEM
010019	2016	31431	117	Huntsville Hospital Health System	6530880	41	117	Initially Merged	helen keller hospital	HUNTSVILLE HOSPITAL HEALTH SYSTEM
010019	2017	31431	117	Huntsville Hospital Health System	6530880	41	117	Initially Merged	helen keller hospital	HUNTSVILLE HOSPITAL HEALTH SYSTEM


* not yet added to document ____________________________________________________

* lewis county general hospital
ahanumber	medicarenumber	year	sameissystem	system_id_aha	sysid	sysname	sys_aha	system_id_ma	system_id_qtr_ma	system_id_yr_ma	sysid_final	merge_status	entity_name
6212320	330213	2011		39605			6212320	533	533	533		Initially Merged	lewis county general hospital
6212320	330213	2012		39605			6212320	533	533	533		Initially Merged	lewis county general hospital
6212320	330213	2013		39605			6212320	533	533	533		Initially Merged	lewis county general hospital
6212320	330213	2014		39605			6212320	533	533	533		Secondary Merge	lewis county general hospital
* In 2014 Lewis County General Hospital affiliated with St. Joseph's Health hospital network of Syracuse
* CCN changes in 2014. Does affiliation = actual acquisition? Neither dataset has this one

* little company of mary san pedro hospital 
	* owned by providence or swedish?
	* swedish acquired by providence in 2012?
	* 2016 change is listed as a merger of equals in the M&A data
* more complete:
ahanumber	medicarenumber	year	sameissystem	system_id_aha	sysid	sysname	sys_aha	system_id_ma	system_id_qtr_ma	system_id_yr_ma	sysid_final	merge_status	entity_name	sysid_final_partial_sysname
6933340	050078	2010		15281	344	Providence Health & Services	6933340	243	243	243	344	Initially Merged	little company of mary san pedro hospital	PROVIDENCE HEALTH & SERVICES
6933340	050078	2011		15281	344	Providence Health & Services	6933340	243	243	243	344	Initially Merged	little company of mary san pedro hospital	PROVIDENCE HEALTH & SERVICES
6933340	050078	2012		15281	344	Providence Health & Services	6933340	243	243	243	871	Initially Merged	little company of mary san pedro hospital	SWEDISH HEALTH SERVICES
6933340	050078	2013		15281	344	Providence Health & Services	6933340	243	243	243	871	Initially Merged	little company of mary san pedro hospital	SWEDISH HEALTH SERVICES
6933340	050078	2014		15281	344	Providence Health & Services	6933340	243	243	243	871	Initially Merged	providence little company of mary medical center san pedro	SWEDISH HEALTH SERVICES
6933340	050078	2015		15281	1006	Providence St. Joseph Health	6933340	243	243	243	1006	Initially Merged	providence little company of mary medical center san pedro	PROVIDENCE ST. JOSEPH HEALTH
6933340	050078	2016		15281	1006	Providence St. Joseph Health	6933340	10	10	10	1006	Initially Merged	providence little company of mary medical center san pedro	PROVIDENCE ST. JOSEPH HEALTH
6933340	050078	2017	0	15281	1006	Providence St. Joseph Health	6933340	10	10	10	1006	Initially Merged	providence little company of mary medical center san pedro	PROVIDENCE ST. JOSEPH HEALTH

* little company of mary hospital torrance 
	* system changes?
ahanumber	medicarenumber	year	sameissystem	system_id_aha	sysid	sysname	sys_aha	system_id_ma	system_id_qtr_ma	system_id_yr_ma	sysid_final	merge_status	entity_name	sysid_final_partial_sysname
6933340	050078	2010		15281	344	Providence Health & Services	6933340	243	243	243	344	Initially Merged	little company of mary san pedro hospital	PROVIDENCE HEALTH & SERVICES
6933340	050078	2011		15281	344	Providence Health & Services	6933340	243	243	243	344	Initially Merged	little company of mary san pedro hospital	PROVIDENCE HEALTH & SERVICES
6933340	050078	2012		15281	344	Providence Health & Services	6933340	243	243	243	871	Initially Merged	little company of mary san pedro hospital	SWEDISH HEALTH SERVICES
6933340	050078	2013		15281	344	Providence Health & Services	6933340	243	243	243	871	Initially Merged	little company of mary san pedro hospital	SWEDISH HEALTH SERVICES
6933340	050078	2014		15281	344	Providence Health & Services	6933340	243	243	243	871	Initially Merged	providence little company of mary medical center san pedro	SWEDISH HEALTH SERVICES
6933340	050078	2015		15281	1006	Providence St. Joseph Health	6933340	243	243	243	1006	Initially Merged	providence little company of mary medical center san pedro	PROVIDENCE ST. JOSEPH HEALTH
6933340	050078	2016		15281	1006	Providence St. Joseph Health	6933340	10	10	10	1006	Initially Merged	providence little company of mary medical center san pedro	PROVIDENCE ST. JOSEPH HEALTH
6933340	050078	2017	0	15281	1006	Providence St. Joseph Health	6933340	10	10	10	1006	Initially Merged	providence little company of mary medical center san pedro	PROVIDENCE ST. JOSEPH HEALTH

* petaluma valley hospital
	* PSJH?
ahanumber	medicarenumber	year	sameissystem	system_id_aha	sysid	sysname	sys_aha	system_id_ma	system_id_qtr_ma	system_id_yr_ma	sysid_final	merge_status	entity_name	sysid_final_partial_sysname
6932412	050136	2010		13693	5425	St. Joseph Health System	6932412	336	336	336	5425	Initially Merged	petaluma valley hospital	ST. JOSEPH HEALTH
6932412	050136	2011		13693	5425	St. Joseph Health	6932412	336	336	336	5425	Initially Merged	petaluma valley hospital	ST. JOSEPH HEALTH
6932412	050136	2012		13693	5425	St. Joseph Health	6932412	336	336	336	36	Initially Merged	petaluma valley hospital	COVENANT HEALTH SYSTEM
6932412	050136	2013		13693	5425	St. Joseph Health	6932412	336	336	336	36	Initially Merged	petaluma valley hospital	COVENANT HEALTH SYSTEM
6932412	050136	2014		13693	5425	St. Joseph Health	6932412	336	336	336	36	Initially Merged	petaluma valley hospital	COVENANT HEALTH SYSTEM
6932412	050136	2015		13693	1006	Providence St. Joseph Health	6932412	336	336	336	1006	Initially Merged	petaluma valley hospital	PROVIDENCE ST. JOSEPH HEALTH
6932412	050136	2016		15281	1006	Providence St. Joseph Health	6932412	10	10	10	1006	Initially Merged	petaluma valley hospital	PROVIDENCE ST. JOSEPH HEALTH

