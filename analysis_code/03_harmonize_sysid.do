/* HARMONIZE_SYSID *************************************************************

Program name: 	03_harmonize_sysid.do
Programmer: 	Julia Paris

Goal: 			Compare M&A system ID to Ellie's xwalk sysid
				Make a harmonized system ID variable
				Clean and reduce data to xwalk for individual-level analyses
				Save xwalk

*******************************************************************************/

* check setup
	check_setup

* quick catalogue of the system ID variables:
	* system_id_aha: from AHA/HIMSS data (unclear from where originally)
	* sysid: from AHA/HIMSS data (unclear from where originally)
	* sysname: from AHA/HIMSS data (unclear from where originally)
	* sys_ada: from AHA/HIMSS data (unclear from where originally)
	* clean_aha: unclear whether this is a system ID too?
	* system_id_ma system_id_yr_ma system_id_qtr_ma: all from M&A data
	* sysid_final: from Ellie's crosswalk
	
* load data
	use "${dbdata}/derived/temp/merged_ma_nonharmonized.dta", clear
	
* visual inspection
	sort entity_name year (medicarenumber)
	sort medicarenumber year
	br ahanumber medicarenumber year system_id_aha sysid sysname sys_aha system_id_ma system_id_qtr_ma system_id_yr_ma sysid_final merge_status entity_name sysid_final_partial_sysname
	
* make a combined_medicarenumber variable that fills missing values of medicare with tostringed mcrnum_y with appropriate leading zeroes? 
	
* challenges to think about: what to do with unmerged AHA/HIMSS systems?
* in the M&A data, do individual hospitals still have a system ID? 
	* yes, it seems like they assign a system ID to every facility even indiv.
	
* creating our own variable

* if sysid_final is always missing for a system_id_ma, then it's an independent hospital
* find the modal sysid_final for each system_id_ma, and vice versa
* use entity name from Ellie's crosswalk when available. 
* then check if they agree on the specific system for different observations. 	

* FIND MODAL OBS
	preserve 
		* collapse to counts by sysid_final system_id_ma
		gen count = 1
		collapse (count) count, by(sysid_final system_id_ma)
		tempfile collapsed
		save `collapsed'
		
		* modal sysid_final for each system_id_ma
		bysort system_id_ma: gen countalt_sysid_final = _N
		bysort system_id_ma (count): keep if _n ==_N
		rename sysid_final modal_sysid_final
		rename count countmodal_sysid_final
		save "${dbdata}/derived/temp/modal_sysid_final_bysystem_id_ma.dta", replace
	
		* modal system_id_ma for each sysid_final
		use `collapsed', clear
		bysort sysid_final: gen countalt_system_id_ma = _N
		bysort sysid_final (count): keep if _n ==_N
		rename system_id_ma modal_system_id_ma
		rename count countmodal_system_id_ma
		save "${dbdata}/derived/temp/modal_system_id_ma_bysysid_final.dta", replace
	restore
		
* merge into full data		
	merge m:1 system_id_ma using "${dbdata}/derived/temp/modal_sysid_final_bysystem_id_ma.dta", nogen
	merge m:1 sysid_final using "${dbdata}/derived/temp/modal_system_id_ma_bysysid_final.dta", nogen
	
* visual inspection
	sort medicarenumber year
	br medicarenumber year system_id_aha sysid sysname sys_aha system_id_ma sysid_final merge_status entity_name sysid_final_partial_sysname *modal* *count*
	
	* so when both of them match their modal equiv, 
		* I see system_id_ma = 170. Usually, that corresponds to sysid_final = 249 (249 is modal). In other words, I think that Ellie's numerical equivalent to that system is 249. So if I see that the sysid_final actually is 249, then I feel good about it. 
		
	* if they BOTH match I think there are no disagreements. 
	gen bothmatch = system_id_ma == modal_system_id_ma & sysid_final == modal_sysid_final
	tab bothmatch

	* I also know that many won't match because Ellie's crosswalk just doesn't have systemID for independents. So first I'm going to try to identify system_id_ma observations that are independent hospitals. 
	gen independent = 0
	
	* A few possible approaches:
		* sysid_final is always missing
			* obstacle: what if this is actually just a mistake, like Ellie missed true system membership? 
			* obstacle: account for the 20 obs that didn't match to Ellie's dataset
			
			** find obs that are always missing sysid_final
			gen missing_sysid_final = sysid_final ==. 
			bysort system_id_ma: egen always_missing_sysid_final = min(missing_sysid_final) // missing.. = 1 if missing. So ALWAYS missing implies NEVER equal to 0. Therefore we need to use the minimum here: only ALWAYS missing if minimum is 1. 
			
		replace independent = 1 if always_missing_sysid_final == 1 & _merge_ellie_xwalk == 3
		
			** look into some questionable observations
			tab system_id_ma if countmodal_sysid_final > 8
			replace independent = 0 if system_id_ma == 2681 // seems to be a missing system in ellie's data: Froedtert & Medical College of Wisconsin health network
				* unclear what to do with this one:
				*replace independent = 0 if inlist(entity_name,"viera hospital") // seems like it has the wrong AHA ID number in 2011?
			
		* another aproach we could add: only one CCN ever corresponds to the system_id_ma
			* obstacle: one facility could have multiple CCNs (under-identifying the true # of independents)
			* obstacle: need to account for missing CCNs
			* obstacle: how to include mcrnum_y?
			
			* ADD CONDITION THAT SYSID_FINAL IS MISSING??
			
			** calc number of CCNs by system_id_final
			bysort system_id_ma medicarenumber: gen tag_ccns = 1 if _n == 1
			bysort system_id_ma: egen sum_ccns = total(tag_ccns)
			drop tag_ccns
			
			** generate indicator for ONE total unique CCN
			gen one_ccn = 0
			replace one_ccn = 1 if sum_ccns == 1 & medicarenumber != "" // is this the right way to think about missing values?
		
		* another aproach we could add: only one facility name ever corresponds to the system_id_ma
			* obstacle: one facility could have multiple names (under-identifying the true # of independents)

* tag observations we don't have to worry about 			
	gen looksgood = 0
	replace looksgood = 1 if bothmatch
	replace looksgood = 1 if independent 
	replace looksgood = 1 if missing(system_id_ma) // we will just use the Ellie info in these cases
	
	bysort medicarenumber (year): egen always_good = min(looksgood)
	replace always_good = looksgood if medicarenumber == "" // for the group of obs missing medicarenumber
	
	codebook medicarenumber if always_good 
	codebook medicarenumber if !always_good 
	
* remaining observations: should we worry about them?
	sort medicarenumber year
	br medicarenumber year system_id_aha sysid sysname sys_aha system_id_ma sysid_final merge_status entity_name sysid_final_partial_sysname *modal* *count* looksgood if always_good == 0
	
* create system change variables _______________________________________________

	* first, make a variable to show when system is different from prior year within entity_uniqueid
		
	foreach sysvar in system_id_ma sysid_final {
		bysort entity_uniqueid (year): gen syschng_`sysvar' = `sysvar' != `sysvar'[_n-1] if _n > 1
	}
	* replace uncertain years will missing 
		* then any years following those missings also have to be missing because we didn't have system info for prior year
	* system_id_ma missing means we really don't have any information about it (didn't merge)
	replace syschng_system_id_ma = . if missing(system_id_ma)
		replace syschng_system_id_ma = . if missing(system_id_ma[_n-1])
	* if _merge_ellie_xwalk == 1, then missing sysid_final means that we don't have information
	replace syschng_sysid_final = . if _merge_ellie_xwalk == 1
		replace syschng_sysid_final = . if _merge_ellie_xwalk[_n-1] == 1
 	
	foreach sysvar in system_id_ma sysid_final {
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
		collapse (rawsum) syschng*, by(year)
		drop if year == 2010
		graph bar syschng_system_id_ma syschng_sysid_final, over(year) ///
			legend(position(bottom) label(1 "M&A Data") label(2 "PS Data")) ///
			title("Count of System M&A Events by Year") ///
			blabel(bar, size(vsmall))
		graph export "${overleaf}/notes/M&A Merge/figures/counts_syschng_byyear.pdf", as(pdf) name("Graph") replace
	restore
	* share of non-missing observations in a given year with an acquisition event
	preserve
		collapse syschng*, by(year)
		drop if year == 2010
		graph bar syschng_system_id_ma syschng_sysid_final, over(year) ///
			legend(position(bottom) label(1 "M&A Data") label(2 "PS Data")) ///
			title("Share of Facilities With System M&A Events by Year") ///
			blabel(bar, size(vsmall) format(%4.3f))
		graph export "${overleaf}/notes/M&A Merge/figures/shares_syschng_byyear.pdf", as(pdf) name("Graph") replace
	restore
	
* reduce sample to info needed for crosswalking to indiv. file _________________
	 
* fix variable names - eventually want to do this early in the code
	* sysid_final -> sysid_ps
	cap rename sysid_final sysid_ps
	cap rename syschng_sysid_final syschng_sysid_ps
	cap rename syschng_ct_sysid_final syschng_ct_sysid_ps
	
	* system_id_ma -> sysid_ma
	cap rename system_id_ma sysid_ma
	cap rename syschng_system_id_ma syschng_sysid_ma
	cap rename syschng_ct_system_id_ma syschng_ct_sysid_ma
	
	* medicarenumber -> ccn_himss
	cap rename medicarenumber ccn_himss
	
	* mcrnum_y -> ccn_aha
	cap rename mcrnum_y ccn_aha
	
	* make sure all the for-profit variables from PS data are correctly named
	foreach profvar in forprofit forprofit_lag forprofit_chng {
		cap rename `profvar' `profvar'_ps
	}
	
* keep key variables
	keep 	ccn_himss ccn_aha /// medicare number variables
			*sysid_ps *sysid_ma /// system ID variables
			ahanumber year entity_uniqueid ///
			forprofit_ps forprofit_lag_ps forprofit_chng_ps gov_priv_type_ps
			
* make unique by entity_uniqueid year
	bysort entity_uniqueid year: keep if _n == 1
		* 12 duplicate observations dropped
			
* save a crosswalk for merging with large file
	save "${dbdata}/derived/temp/merged_ma_sysid_xwalk.dta", replace
	
	
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

