/* ADD_MERGERS_AQC *****************************************************************

Program name: 	02_add_mergers_acq.do
Programmer: 	Julia Paris

Goal: 			Merge M&A data into hospital-level dataset

*******************************************************************************/

* SETUP ________________________________________________________________________ 

* check setup is complete
	check_setup
	
* write program to export merge results
	cap program drop export_merge
	program export_merge 
	
		syntax , Filename(string) Target(string)
		
		* write merge results
		tempname f
		file open `f' using "${overleaf}/notes/M&A Merge/figures/`filename'.tex", write replace
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
	
* PREPARE FOR MERGE  ___________________________________________________________

* load data
	use "${dbdata}/derived/himss_aha_hospitals_0529.dta", clear
	
	duplicates report
	duplicates tag, gen(dup)
	duplicates drop

	* test merge variable
	codebook medicarenumber
		* seems like the data have inconsistent medicarenumber formats; occasionally missing leading zeroes.
		* this will add leading zeroes to short observations
// 		gen ccn_6 = substr("000000" + medicarenumber, -6, .)
// 		replace ccn_6 = "" if ccn_6 == "000000"
// 		replace medicarenumber = ccn_6 if length(medicarenumber) < 6
// 		drop ccn_6
	* only results in one more merged obs from 2014
	* all the other times we see this issue are in 2005
	
	* missing for 5,375/63,866 observations
	tab year
	tab year if !missing(medicarenumber)
	tab year if !missing(medicarenumber) | !missing(mcrnum_y)
	
	preserve
		gen count_aha_all = 1 
		gen count_aha_withmcrnum = 1 if !missing(medicarenumber) | !missing(mcrnum_y)
		collapse (rawsum) count_aha_all count_aha_withmcrnum, by(year)
		tempfile ccn_aha_counts
		save `ccn_aha_counts'
	restore
	
	rename system_id system_id_aha
	
* Prepare M&A data for merge	
	preserve 
		use "${dbdata}/supplemental/strategic_ma_db_v2/strategic_ma_db_v2.dta", clear
		rename medicare_ccn_str medicarenumber
		rename (system_id system_id_yr system_id_qtr) (system_id_ma system_id_yr_ma system_id_qtr_ma)
		
		* collapse quarterly data to annual data
		foreach var in bankruptcy system_exit system_split merger_of_equals target {
			bysort medicarenumber year: egen `var'_yr = max(`var')
			drop `var'
			rename `var'_yr `var'
		}
		
		* keep the last observation in the year
		bysort medicarenumber year (year_qtr month): keep if _n == _N
		
		* ensure that data are now unique by CCN-year 
		bysort medicarenumber year: gen dup = _N
		assert dup == 1
		drop dup
		
		tab year
		keep if year <= 2017
		
		tempfile ma_data
		save `ma_data'
		
		* make a graph with counts by year
		gen count_ma = 1
		collapse (rawsum) count_ma, by(year)
		merge 1:1 year using `ccn_aha_counts'
		keep if inrange(year,2010,2017)
		graph bar count_ma count_aha_all count_aha_withmcrnum, over(year) ///
			legend(position(bottom) label(1 "M&A Data") label(2 "All AHA/HIMSS") label(3 "AHA/HIMSS With Non-Missing CCN")) ///
			title("Count of Observations by Year") ///
			blabel(bar, size(vsmall))
		graph export "${overleaf}/notes/M&A Merge/figures/counts_aha_ma_byyear.pdf", as(pdf) name("Graph") replace
	restore

* MERGE ________________________________________________________________________	
	
	merge m:1 medicarenumber year using `ma_data'
	* keep overlapping date ranges
	keep if inrange(year,2010,2017)
	
	tab _merge 
	tab _merge if !missing(medicarenumber)
	tab _merge if !missing(medicarenumber) | !missing(mcrnum_y)
	
	* format and export merge results
	label define mergelab 1 "Unmerged from AHA/HIMSS" 2 "Unmerged from M\&A" 3 "Merged"
	label values _merge mergelab
	export_merge, filename(merge1_tab) target(_merge)
	
	* unmerged observations from AHA/HIMSS that are NOT MISSING CCN: 4,136
	* unmerged observations from AHA/HIMSS that are MISSING CCN: 2,259
		tab _merge if missing(medicarenumber)
	* unmerged observations from M&A data: 3,317
	
	* UNMERGED FROM AHA:
		* check hospital types?
		* could these be specialty centers that aren't included in M&A data?
	tab type if _merge==1 & !missing(medicarenumber)
		* most commonly Long Term Acute Hospitals (49%)
		* other specialty (15%)
		* pediatric (10%)
	* should match: 
		* general medical and surgical (7%)
		* general medical (0.5%)
		* critical access (11%)
	* the M&A dataset is based on general and CAH "We included short-term acute care general hospitals (from here on: general hospitals) and critical access hospitals (CAHs) as classified by the Medicare provider of service files. There were 4896 unique AHA respondent hospitals in our sample spanning 2010–2019."
	* look into CAH first:
	br if type == "Critical Access" & _merge==1 & !missing(medicarenumber)
	tab year if type == "Critical Access" & _merge==1 & !missing(medicarenumber)
		* almost all observations are from 2010 - they are in the AHA data but not the M&A data
	* in 2017, a lot of CCNs have F's in them. check what the correct CCN is (look at other CCNs with the same AHAID/HIMSS entity?)
		* I'm noticing that these seem to be army hospitals (ACH - army community hospital; AMC - army medical center)
		* F =FEDERAL?
		* 2779E - Assigning Emergency Hospital CMS Certification Numbers (Non-Participating Hospitals) p. 13 https://www.cms.gov/regulations-and-guidance/guidance/transmittals/downloads/r29soma.pdf 
		* medicarenumber	entity_name
		// 11032F	martin ach fort benning
		// 11033F	eisenhower amc fort gordon
		// 11035F	winn ach fort stewart
		// 21006F	the national institutes of health clinical center
		
		
* LET'S INVESTIGATE 
	sort medicarenumber
	br ahanumber medicarenumber entity_city entity_state entity_name mcrnum_x clean_name mname mcrnum_y sysname fac_name _merge if year == 2010 & _merge != 3
	
	* some should have matched but don't:
		* ketchikan general hospital / KETCHIKAN GENERAL HOSPITAL
			* medicarenumber in AHA is 020004 but in M&A it is 021311
				* mcrnum_y is 21311 (matches M&A)
		* south peninsula hospital / SOUTH PENINSULA HOSPITAL
			* medicarenumber in AHA is 020014 but in M&A it is 021313
				* BUT mcrnum_y is 21313 (matches M&A)
				
	* one potential approach: use the mcrnum_y and medicarenumber to create a list of linked CCNs
		* try merging on each one. Maybe using interchangeable CCNs by accident.  
	
	preserve
		keep if _merge == 2
		drop _merge
		keep fac_name year medicarenumber year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id_ma system_id_qtr_ma system_id_yr
		destring medicarenumber, gen(mcrnum_y)
		tempfile unmerged1
		save `unmerged1'
	restore
	
	* keep a separate file of all the initial merges
	preserve
		keep if _merge == 3
		gen merge_status = "Initially Merged"
		tempfile merged1
		save `merged1'
	restore
		
	keep if _merge ==1
	drop fac_name year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id_ma system_id_qtr_ma system_id_yr
	merge m:1 mcrnum_y year using `unmerged1', gen(_merge2)
	gen merge_status = "Secondary Merge" if _merge2 == 3
	replace merge_status = "Unmerged from AHA/HIMSS" if _merge2 == 1
	replace merge_status = "Unmerged from M&A" if _merge2 == 2
	append using `merged1'
	
	tab merge_status
	tab merge_status if !missing(medicarenumber) | !missing(mcrnum_y)
	
	* format and export merge results
	encode merge_status, gen(enc_merge_status)
	label define enc_merge_status 4 "Unmmerged From M\&A", modify
	export_merge, filename(merge2_tab) target(enc_merge_status)
	
	* resolved the 2010 issue:
	tab year if type == "Critical Access" & merge_status=="Unmerged from AHA/HIMSS" & !missing(medicarenumber)

* merge quality checks	
	br _merge year medicarenumber entity_name mcrnum_x cleaned_mcr mcrnum_y fac_name medicare_ccn merge_status type 				
	/*
* Pull in Ellie dataset	for merge #3 __ no new matches ___ delete eventually ___
	* pull unmerged from M&A into own file
	preserve
		keep if merge_status == "Unmerged from M&A"
		drop _merge
		keep fac_name year medicarenumber year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id_ma system_id_qtr_ma system_id_yr
		gen mcrnum_ellie = medicarenumber
		tempfile unmerged2
		save `unmerged2'
	restore

	* prepare for merge
	drop if merge_status == "Unmerged from M&A"
	*/
	tostring ahanumber, gen(ahaid_noletter)	
	preserve
		use "${dbdata}/supplemental/hospital_ownership.dta", clear
		replace ahaid_noletter = ahaid if missing(ahaid_noletter)
		bysort ahaid_noletter year: drop if _n>1 // rare, but make unique
		* rename profit variables to make source clear
		foreach profvar in forprofit forprofit_lag forprofit_chng gov_priv_type {
			rename `profvar' `profvar'_ps
		}
		tempfile ellie_xwalk
		save `ellie_xwalk'
	restore

	
	* merge Ellie's xwalk
	merge m:1 ahaid_noletter year using `ellie_xwalk', gen(_merge_ellie_xwalk) keepusing(ahaid sysid_final mcrnum sysid_final_partial_sysname forprofit* gov_priv_type_ps) keep(1 3)
	rename mcrnum mcrnum_ellie

	/*
	destring mcrnum_ellie, gen(mcrnum_ellie_destr)
	gen same_ccn = mcrnum_ellie_destr == mcrnum_y
	* only different for 385 obs (0.9%), of which 294 have a missing mcrnum_y
	gen same_ccn2 = mcrnum_ellie==medicarenumber
	* different 
	
	* keep a separate file of all the successful merges
	preserve
		keep if merge_status == "Initially Merged" | merge_status == "Secondary Merge"
		tempfile merged2
		save `merged2'
	restore
	
	keep if merge_status == "Unmerged from AHA/HIMSS"
	drop fac_name year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id_ma system_id_qtr_ma system_id_yr
	merge m:1 mcrnum_ellie year using `unmerged2', gen(_merge3)
	replace merge_status = "Unmerged from M&A" if _merge3 == 2
	append using `merged2'

* try the crosswalk approach ___________________________________________________
	
* first, create a crosswalk for every CCN of every other CCN that appears on a shared observation
	preserve
		* keep only relevant variables
		keep medicarenumber mcrnum_y 
		* keep unique combinations
		bysort medicarenumber mcrnum_y: keep if _n == 1
		* drop any observations where they are the same
		foreach lett in y {
			gen mcrnum_`lett'_str = string(mcrnum_`lett', "%06.0f")
			replace mcrnum_`lett'_str = "" if mcrnum_`lett'_str == "."
		}
		drop if mcrnum_y_str == medicarenumber	
		drop mcrnum_y_str
		tempfile base
		save `base'
		
		* first, try finding every option of mcrnum_y for each different ccn. 
		drop if medicarenumber == "" | mcrnum_y == .
		bysort medicarenumber (mcrnum_y): gen num=_n
		rename mcrnum_y alt_mcrnum_y
		reshape wide alt_mcrnum_y, i(medicarenumber) j(num)
		save "${dbdata}/derived/temp/alt_CCNs_medicarenumber.dta", replace
		
		* second, try finding every option of medicarenumber for each different mcrnum_y
		use `base', clear
		drop if medicarenumber == "" | mcrnum_y == .
		bysort mcrnum_y (medicarenumber): gen num=_n
		rename medicarenumber alt_medicarenumber
		reshape wide alt_medicarenumber, i(mcrnum_y) j(num)
		save "${dbdata}/derived/temp/alt_CCNs_mcrnum_y.dta", replace
	restore
	
* first, try merging crosswalk on medicarenumber. 
merge m:1 medicarenumber using "${dbdata}/derived/temp/alt_CCNs_medicarenumber.dta", gen(_merge_medicarenumber)	

	foreach num in 1 2 3 {
	* set up merge 
		preserve
			keep if merge_status == "Unmerged from M&A"
			drop _merge
			keep fac_name year medicarenumber year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id_ma system_id_qtr_ma system_id_yr_ma 
			gen alt_mcrnum_y`num' = medicare_ccn
			tempfile unmerged3a_`num'
			save `unmerged3a_`num''
		restore
		
		* keep a separate file of all the successful merges or observations we can't use here
		preserve
			keep if merge_status == "Initially Merged" | merge_status == "Secondary Merge" | missing(alt_mcrnum_y`num')
			tempfile merged3a_`num'
			save `merged3a_`num''
		restore
		
		keep if merge_status == "Unmerged from AHA/HIMSS" & !missing(alt_mcrnum_y`num')
		drop fac_name year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id_ma system_id_qtr_ma system_id_yr
		merge m:1 alt_mcrnum_y`num' year using `unmerged3a_`num'', gen(_merge4a_`num') keep(1 3)
		replace merge_status = "Unmerged from M&A" if _merge4a_`num' == 2
		replace merge_status = "Final Merges" if _merge4a_`num' == 3
		append using `merged3a_`num''
		* got 7 more on this round
	}
	
* now try the merging the crosswalk on mcrnum_y
merge m:1 mcrnum_y using "${dbdata}/derived/temp/alt_CCNs_mcrnum_y.dta", gen(_merge_mcrnum_y)	
	
	foreach num in 1 2 {
		* set up for merge
		preserve
			keep if merge_status == "Unmerged from M&A"
			drop _merge
			keep fac_name year medicarenumber year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id_ma system_id_qtr_ma system_id_yr_ma 
			gen alt_medicarenumber`num' = medicarenumber
			tempfile unmerged3b_`num'
			save `unmerged3b_`num''
		restore
		
		* keep a separate file of all the successful merges or observations we can't use here
		preserve
			keep if merge_status == "Initially Merged" | merge_status == "Secondary Merge" | missing(alt_medicarenumber`num')
			tempfile merged3b_`num'
			save `merged3b_`num''
		restore

		keep if merge_status == "Unmerged from AHA/HIMSS" & !missing(alt_medicarenumber`num')
		drop fac_name year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id_ma system_id_qtr_ma system_id_yr
		merge m:1 alt_medicarenumber`num' year using `unmerged3b_`num'', gen(_merge4b_`num') keep(1 3)
		replace merge_status = "Unmerged from M&A" if _merge4b_`num' == 2
		replace merge_status = "Final Merges" if _merge4b_`num' == 3
		append using `merged3b_`num''
	} 
*/

* need to add variable cleaning and renaming
* save merged file
	preserve
		drop if merge_status == "Unmerged from M&A"
		save "${dbdata}/derived/temp/merged_ma_nonharmonized.dta", replace
	restore

* narrow down
	keep fac_name year medicarenumber year_qtr medicare_ccn hrrcode month source_completed bankruptcy sysid_final_partial_sysname system_exit system_split merger_of_equals target notes system_id_ma system_id_aha system_id_qtr_ma system_id_yr_ma merge_status type ahanumber ahaid_noletter entity_uniqueid x himss_entityid surveyid entity_parentid entityno entity_name haentitytypeid entity_type medicarenumber yearopened ownershipstatus county mcrnum_x cleaned_mcr address_clean zip clean_name ahanumber_filled mcrnum_y mname hospn sysid sysname sysid_final

	sort medicarenumber year (mcrnum_y)

	br ahanumber himss_entityid entity_uniqueid medicarenumber entity_name year mcrnum_y type sysid_final_partial_sysname sysid sysid_final system_id_ma system_id_aha medicare_ccn fac_name merge_status system_id_qtr_ma system_id_yr_ma bankruptcy system_exit system_split merger_of_equals target if (inlist(type,"General Medical & Surgical","Critical Access","General Medical") | merge_status!="Unmerged from AHA/HIMSS") & (!missing(medicarenumber) | !missing(mcrnum_y))
	
	
* MERGE QUALITY INSPECTIONS ____________________________________________________

exit // so that real code doesn't run into this		

gen unmerg_ma = merge_status == "Unmerged from M&A"
bysort medicarenumber: egen ever_unmerg_ma = max(unmerg_ma)
br ahanumber himss_entityid entity_uniqueid medicarenumber entity_name year mcrnum_y type sysid sysid_final system_id_mamedicare_ccn fac_name merge_status system_id_qtr_ma system_id_yr_ma bankruptcy system_exit system_split merger_of_equals target if ever_unmerg_ma == 1


* CASE STUDIES _________________________________________________________________

* Tri-county hospital 	
	* this one matches for multiple years except 2013 - is it in the M&A data in 2013?
ahanumber	himss_entityid	entity_uniqueid	medicarenumber	entity_name	year	mcrnum_y	type	sysid	sysid_final	system_id	medicare_ccn	fac_name	merge_status	system_id_qtr	system_id_yr	bankruptcy	system_exit	system_split	merger_of_equals	target
6391146	497954	46900	100139	nature coast regional hospital	2010	100139	General Medical & Surgical			46739	100139	TRI COUNTY HOSPITAL - WILLISTON	Initially Merged	935	935	0	1	0	0	0
6391146	541449	46900	100139	tri county hospital in williston	2011	100139	General Medical & Surgical			46739	100139	TRI COUNTY HOSPITAL - WILLISTON	Initially Merged	935	935	0	0	0	0	0
6391146	587836	46900	100139	tri county hospital in williston	2012	100139	General Medical & Surgical			46739	100139	TRI COUNTY HOSPITAL - WILLISTON	Initially Merged	935	935	0	0	0	0	0
6391146	639498	46900	100139	regional general hospital williston	2013	100322	General Medical & Surgical						Unmerged from AHA/HIMSS							
6391146	697595	46900	100139	regional general hospital williston	2014	100322	General Medical & Surgical			935	100322	REGIONAL GENERAL HOSPITAL WILLISTON	Secondary Merge	935	935	0	0	0	0	0
6391146	768041	46900	100139	regional general hospital williston	2015	100322	General Medical & Surgical			935	100322	REGIONAL GENERAL HOSPITAL WILLISTON	Secondary Merge	935	935	0	0	0	0	0
6391146	845370	46900	100139	regional general hospital of williston	2016	100322	General Medical & Surgical			935	100322	REGIONAL GENERAL HOSPITAL WILLISTON	Secondary Merge	935	935	0	0	0	0	0
6391146	932686	46900	100322	regional general hospital of williston	2017	100322	General Medical & Surgical			46739	100322	REGIONAL GENERAL HOSPITAL WILLISTON	Initially Merged	935	935	0	0	0	0	0

* Doctors specialty hospital 
	* this one drops out of M&A data after 2014
	_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Matched (3)	2013	110186	doctors specialty hospital	110186	110186	110186	DOCTORS SPECIALTY HOSPITAL	110186	Initially Merged	General Medical & Surgical
Matched (3)	2013	110186	doctors specialty hospital	110186	110186	110186	DOCTORS SPECIALTY HOSPITAL	110186	Initially Merged	General Medical & Surgical
Master only (1)	2014	110186	doctors specialty hospital	110186	110186				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2014	110186	doctors specialty hospital	110186	110186				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2015	110186	midtown medical center west campus	110186	110186				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2016	110186	midtown medical center west	110186	110186				Unmerged from AHA/HIMSS	General Medical
Master only (1)	2017	110186	midtown medical center west	110186	110186				Unmerged from AHA/HIMSS	General Medical
	* listed as a "never claimer" hospital on GME reporting from GA: https://nosorh.org/wp-content/uploads/2022/05/GME131Elig_GA.pdf
	* seems likely it's actually closed?
	* https://www.ahd.com/free_profile/110186/Midtown_Medical_Center_-_West_Campus/Columbus/Georgia/
		* says that this facility became an urgent care center on april 16 2015
		* now at the same address is Columbus Specialty Hospital (CCN 112012)
		
* HAND-CHECKING UNMERGED FROM M&A ______________________________________________ 

* many of these hospitals look like IHS - our data may have an F in the CCN?
* ex 1
mcrnum_y	sysid	sysname	fac_name	year_qtr	medicare_ccn
30113			WHITERIVER PHS INDIAN HOSPITAL	20104	30113
30113			WHITERIVER PHS INDIAN HOSPITAL	20114	30113
30113			WHITERIVER PHS INDIAN HOSPITAL	20124	30113
30113			WHITERIVER PHS INDIAN HOSPITAL	20134	30113
30113			WHITERIVER PHS INDIAN HOSPITAL	20144	30113
30113			WHITERIVER PHS INDIAN HOSPITAL	20154	30113
30113			WHITERIVER PHS INDIAN HOSPITAL	20164	30113
30113			WHITERIVER PHS INDIAN HOSPITAL	20174	30113
	* U. S. Public Health Service Indian Hospital-Whiteriver
	* ID 6860550
* ex 2 
mcrnum_y	sysid	sysname	fac_name	year_qtr	medicare_ccn
30078			PHOENIX INDIAN MEDICAL CENTER	20104	30078
30078			PHOENIX INDIAN MEDICAL CENTER	20114	30078
30078			PHOENIX INDIAN MEDICAL CENTER	20124	30078
30078			PHOENIX INDIAN MEDICAL CENTER	20134	30078
30078			PHOENIX INDIAN MEDICAL CENTER	20144	30078
30078			PHOENIX INDIAN MEDICAL CENTER	20154	30078
30078			PHOENIX INDIAN MEDICAL CENTER	20164	30078
30078			PHOENIX INDIAN MEDICAL CENTER	20174	30078
	* ID	6860260
	* U. S. Public Health Service Phoenix Indian Medical Center

* ex 3
mcrnum_y	sysid	sysname	fac_name	year_qtr	medicare_ccn
30071			FORT DEFIANCE INDIAN HOSPITAL	20104	30071
30071			FORT DEFIANCE INDIAN HOSPITAL	20114	30071
30071			FORT DEFIANCE INDIAN HOSPITAL	20124	30071
30071			FORT DEFIANCE INDIAN HOSPITAL	20134	30071
30071			FORT DEFIANCE INDIAN HOSPITAL	20144	30071
30071			FORT DEFIANCE INDIAN HOSPITAL	20154	30071
30071			FORT DEFIANCE INDIAN HOSPITAL	20164	30071
30071			FORT DEFIANCE INDIAN HOSPITAL	20174	30071
	* Tsehootsooi Medical Center
	* ID 6860090
* ex 4
30074			SELLS INDIAN HEALTH SERVICE HOSPITAL	20104	30074
30074			SELLS INDIAN HEALTH SERVICE HOSPITAL	20114	30074
30074			SELLS INDIAN HEALTH SERVICE HOSPITAL	20124	30074
30074			SELLS INDIAN HEALTH SERVICE HOSPITAL	20134	30074
30074			SELLS INDIAN HEALTH SERVICE HOSPITAL	20144	30074
30074			SELLS INDIAN HEALTH SERVICE HOSPITAL	20154	30074
30074			SELLS INDIAN  HOSPITAL	20164	30074
30074			SELLS HOSPITAL	20174	30074
	* ID 6860369
	* U. S. Public Health Service Indian Hospital-Sells
* ex 5
30077			PHS INDIAN HOSPITAL-SAN CARLOS	20104	30077
30077			SAN CARLOS INDIAN HOSPITAL	20114	30077
30077			SAN CARLOS INDIAN HOSPITAL	20124	30077
30077			SAN CARLOS INDIAN HOSPITAL	20134	30077
30077			SAN CARLOS INDIAN HOSPITAL	20144	30077
30077			SAN CARLOS INDIAN HOSPITAL	20154	30077
30077			SAN CARLOS APACHE HEALTHCARE	20164	30077
30077			SAN CARLOS APACHE HEALTHCARE	20174	30077
	* ID 6860370
	* U. S. Public Health Service Indi
	* San Carlos Apache Healthcare Cor

* ex 6 
mcrnum_y	sysid	sysname	fac_name	year_qtr	medicare_ccn
241358			CASS LAKE INDIAN HEALTH SERVICES HOSPITAL	20104	241358
241358			CASS LAKE INDIAN HEALTH SERVICES HOSPITAL	20114	241358
241358			CASS LAKE INDIAN HEALTH SERVICES HOSPITAL	20124	241358
241358			CASS LAKE INDIAN HEALTH SERVICES HOSPITAL	20134	241358
241358			CASS LAKE INDIAN HEALTH SERVICES HOSPITAL	20144	241358
241358			CASS LAKE INDIAN HEALTH SERVICES HOSPITAL	20154	241358
241358			CASS LAKE INDIAN HEALTH SERVICES HOSPITAL	20164	241358
241358			CASS LAKE INDIAN HEALTH SERVICES HOSPITAL	20174	241358
* ex 7
mcrnum_y	sysid	sysname	fac_name	year_qtr	medicare_ccn
250127			CHOCTAW HEALTH CENTER	20104	250127
250127			CHOCTAW HEALTH CENTER	20114	250127
250127			CHOCTAW HEALTH CENTER	20124	250127
250127			CHOCTAW HEALTH CENTER	20134	250127
250127			CHOCTAW HEALTH CENTER	20144	250127
250127			CHOCTAW HEALTH CENTER	20154	250127
250127			CHOCTAW HEALTH CENTER	20164	250127
250127			CHOCTAW HEALTH CENTER	20174	250127
	* ID 6540710
* ex 8
mcrnum_y	sysid	sysname	fac_name	year_qtr	medicare_ccn
270074			P H S INDIAN HOSPITAL AT BROWNING - BLACKFEET	20104	270074
270074			P H S INDIAN HOSPITAL AT BROWNING - BLACKFEET	20114	270074
270074			P H S INDIAN HOSPITAL AT BROWNING - BLACKFEET	20124	270074
270074			P H S INDIAN HOSPITAL AT BROWNING - BLACKFEET	20134	270074
270074			P H S INDIAN HOSPITAL AT BROWNING - BLACKFEET	20144	270074
270074			P H S INDIAN HOSPITAL AT BROWNING - BLACKFEET	20154	270074
270074			P H S INDIAN HOSPITAL AT BROWNING - BLACKFEET	20164	270074
270074			P H S INDIAN HOSPITAL AT BROWNING - BLACKFEET	20174	270074
* ex 9
mcrnum_y	sysid	sysname	fac_name	year_qtr	medicare_ccn
271315			P H S INDIAN HOSPITAL-FT BELKNAP AT HARLEM - CAH	20104	271315
271315			P H S INDIAN HOSPITAL-FT BELKNAP AT HARLEM - CAH	20114	271315
271315			P H S INDIAN HOSPITAL-FT BELKNAP AT HARLEM - CAH	20124	271315
271315			P H S INDIAN HOSPITAL-FT BELKNAP AT HARLEM - CAH	20134	271315
271315			P H S INDIAN HOSPITAL-FT BELKNAP AT HARLEM - CAH	20144	271315
271315			P H S INDIAN HOSPITAL-FT BELKNAP AT HARLEM - CAH	20154	271315
271315			P H S INDIAN HOSPITAL-FT BELKNAP AT HARLEM - CAH	20164	271315
271315			P H S INDIAN HOSPITAL-FT BELKNAP AT HARLEM - CAH	20174	271315
* ex 10
271339			P H S INDIAN HOSPITAL CROW / NORTHERN CHEYENNE	20104	271339
271339			P H S INDIAN HOSPITAL CROW / NORTHERN CHEYENNE	20114	271339
271339			P H S INDIAN HOSPITAL CROW / NORTHERN CHEYENNE	20124	271339
271339			P H S INDIAN HOSPITAL CROW / NORTHERN CHEYENNE	20134	271339
271339			P H S INDIAN HOSPITAL CROW / NORTHERN CHEYENNE	20144	271339
271339			P H S INDIAN HOSPITAL CROW / NORTHERN CHEYENNE	20154	271339
271339			P H S INDIAN HOSPITAL CROW / NORTHERN CHEYENNE	20164	271339
271339			P H S INDIAN HOSPITAL CROW / NORTHERN CHEYENNE	20174	271339
* ex 11
mcrnum_y	sysid	sysname	fac_name	year_qtr	medicare_ccn
280119			WINNEBAGO IHS HOSPITAL	20104	280119
280119			WINNEBAGO IHS HOSPITAL	20114	280119
280119			WINNEBAGO IHS HOSPITAL	20124	280119
280119			WINNEBAGO IHS HOSPITAL	20134	280119
280119			WINNEBAGO IHS HOSPITAL	20144	280119
280119			WINNEBAGO IHS HOSPITAL	20154	280119
280119			WINNEBAGO IHS HOSPITAL	20164	280119
280119			WINNEBAGO IHS HOSPITAL	20174	280119




* other ex 1
mcrnum_y	sysid	sysname	fac_name	year_qtr	medicare_ccn
370225			SUMMIT MEDICAL CENTER	20104	370225
370225			SUMMIT MEDICAL CENTER	20114	370225
370225			SUMMIT MEDICAL CENTER	20124	370225
370225			SUMMIT MEDICAL CENTER	20134	370225
370225			SUMMIT MEDICAL CENTER	20144	370225
370225			SUMMIT MEDICAL CENTER	20154	370225
370225			SUMMIT MEDICAL CENTER	20164	370225
370225			SUMMIT MEDICAL CENTER	20174	370225
	* ID 6730073
	* Foundation Bariatric Hospital
	* Summit Medical Center



	

