/* ADD_MERGERS_AQC *****************************************************************

Program name: 	02_add_mergers_acq.do
Programmer: 	Julia Paris

Goal: 			Merge M&A data into hospital-level dataset

*******************************************************************************/

* check setup is complete
	check_setup
	
* Load data
	use "${dbdata}/derived/himss_aha_hospitals_0529.dta", clear
	
	duplicates report
	duplicates tag, gen(dup)
	duplicates drop

	* test merge variable
	codebook medicarenumber
	* missing for 5,375/63,866 observations
	tab year
	tab year if !missing(medicarenumber)
	tab year if !missing(medicarenumber) | !missing(mcrnum_y)
	
* Prepare M&A data for merge	
	preserve 
		use "${dbdata}/supplemental/strategic_ma_db_v2/strategic_ma_db_v2.dta", clear
		rename medicare_ccn_str medicarenumber
		
		* collapse quarterly data to annual data
		foreach var in bankruptcy system_exit system_split merger_of_equals target {
			bysort medicarenumber year: egen `var'_yr = max(`var')
			drop `var'
			rename `var'_yr `var'
		}
		
		* keep the last observation in the year
		bysort medicarenumber year (year_qtr): keep if _n == _N
		
		* ensure that data are now unique by CCN-year 
		bysort medicarenumber year: gen dup = _N
		assert dup == 1
		drop dup
		
		tab year
		keep if year <= 2017
		
		tempfile ma_data
		save `ma_data'
	restore

	merge m:1 medicarenumber year using `ma_data'
	* just going to keep 2010-2017 while figuring out the merge
	keep if inrange(year,2010,2017)
	
	tab _merge 
	tab _merge if !missing(medicarenumber)
	tab _merge if !missing(medicarenumber) | !missing(mcrnum_y)
	
	* the M&A data spans 2010-2017
	* approx 4,500-4,700 obs per year. 
	
	* the AHA data is missing 5,375 obs of medicarenumber
	* in the years 2010-2017, AHA has approx 5,000 observations annually
		* 4,600-4,900 with non-missing CCN
			* increases over sample
	
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
		* check individual CCNs:
			* 670104 only exists in 2010
			* 530019 " "
			* 530022 " "
			* 530029 " "
			* 670081 exists in one dataset in some years and the other dataset in other years
			* 020004 just 2010
			* 020014 just 2010
			* 030040 ""
			* 030060 just 2010
			* 030099 just 2010
			* 030067 falls out of M&A data in 2013 but not AHA
			* 040024 just 2010
			* 040053 just 2010
			* 040066 ""
			* 040105 just 2010
			* 040107 just 2010
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
		keep fac_name year medicarenumber year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id system_id_qtr system_id_yr
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
	drop fac_name year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id system_id_qtr system_id_yr
	merge m:1 mcrnum_y year using `unmerged1', gen(_merge2)
	gen merge_status = "Secondary Merge" if _merge2 == 3
	replace merge_status = "Unmerged from AHA/HIMSS" if _merge2 == 1
	replace merge_status = "Unmerged from M&A" if _merge2 == 2
	append using `merged1'
	
	tab merge_status
	tab merge_status if !missing(medicarenumber) | !missing(mcrnum_y)

* merge quality checks	
	br _merge year medicarenumber entity_name mcrnum_x cleaned_mcr mcrnum_y fac_name medicare_ccn merge_status type 				
		
* Pull in Ellie dataset	for merge #3 __ no new matches ___ delete eventually ___
	* pull unmerged from M&A into own file
	preserve
		keep if merge_status == "Unmerged from M&A"
		drop _merge
		keep fac_name year medicarenumber year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id system_id_qtr system_id_yr
		gen mcrnum_ellie = medicarenumber
		tempfile unmerged2
		save `unmerged2'
	restore

	* prepare for merge
	drop if merge_status == "Unmerged from M&A"
	tostring ahanumber, gen(ahaid_noletter)	
	preserve
		use "${dbdata}/supplemental/hospital_ownership.dta", clear
		replace ahaid_noletter = ahaid if missing(ahaid_noletter)
		bysort ahaid_noletter year: drop if _n>1 // rare, but make unique
		tempfile ellie_xwalk
		save `ellie_xwalk'
	restore
	
	* merge Ellie's xwalk
	merge m:1 ahaid_noletter year using `ellie_xwalk', gen(_merge_ellie_xwalk) keepusing(ahaid sysid_final mcrnum) keep(1 3)
	drop _merge_ellie_xwalk
	rename mcrnum mcrnum_ellie
	destring mcrnum_ellie, gen(mcrnum_ellie_destr)
	gen same_ccn = mcrnum_ellie_destr == mcrnum_y
	* only different for 385 obs (0.9%), of which 294 have a missing mcrnum_y
	
	* keep a separate file of all the successful merges
	preserve
		keep if merge_status == "Initially Merged" | merge_status == "Secondary Merge"
		tempfile merged2
		save `merged2'
	restore
	
	keep if merge_status == "Unmerged from AHA/HIMSS"
	drop fac_name year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id system_id_qtr system_id_yr
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
			keep fac_name year medicarenumber year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id system_id_qtr system_id_yr 
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
		drop fac_name year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id system_id_qtr system_id_yr
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
			keep fac_name year medicarenumber year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id system_id_qtr system_id_yr 
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
		drop fac_name year_qtr medicare_ccn hrrcode month source_completed bankruptcy system_exit system_split merger_of_equals target notes system_id system_id_qtr system_id_yr
		merge m:1 alt_medicarenumber`num' year using `unmerged3b_`num'', gen(_merge4b_`num') keep(1 3)
		replace merge_status = "Unmerged from M&A" if _merge4b_`num' == 2
		replace merge_status = "Final Merges" if _merge4b_`num' == 3
		append using `merged3b_`num''
	}
	
	
* MERGE QUALITY INSPECTIONS ____________________________________________________

exit // so that real code doesn't run into this		
		
* reasons that general hosps may be unmerged:
	* not actually general hosps? 
	* these all have "long-term care" or "geriatric" in the title despite being called general medical
	_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Master only (1)	2012	112005	wesley woods long term care hospital	112005	112005	112005			Unmerged from AHA/HIMSS	General Medical
Master only (1)	2013	112005	wesley woods long term care hospital	112005	112005	112005			Unmerged from AHA/HIMSS	General Medical
Master only (1)	2014	112005	emory wesley woods geriatric hospital	112005	112005	112005			Unmerged from AHA/HIMSS	General Medical
Master only (1)	2015	112005	emory wesley woods geriatric hospital	112005	112005	112005			Unmerged from AHA/HIMSS	General Medical
Master only (1)	2016	112005	emory wesley woods geriatric hospital	112005	112005	112005			Unmerged from AHA/HIMSS	General Medical

see also: levindale hebrew geriatric center hospital, CCN 

		
* once I have loaded the AHA/HIMSS data: _______________________________________

* never in AHA it seems
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
.	2010	140145				140145	ST JOSEPHS HOSPITAL	140145	Unmerged from M&A	
.	2011	140145				140145	ST JOSEPHS HOSPITAL	140145	Unmerged from M&A	
.	2012	140145				140145	ST JOSEPHS HOSPITAL	140145	Unmerged from M&A	
.	2013	140145				140145	ST JOSEPHS HOSPITAL	140145	Unmerged from M&A	
.	2014	140145				140145	ST JOSEPHS HOSPITAL	140145	Unmerged from M&A	
.	2015	140145				140145	ST JOSEPHS HOSPITAL	140145	Unmerged from M&A	
.	2016	140145				140145	ST JOSEPHS HOSPITAL	140145	Unmerged from M&A	
.	2017	140145				140145	ST JOSEPHS HOSPITAL	140145	Unmerged from M&A	
* never in the AHA data - have nothing with the corresponding ZIP 
* https://www.ahd.com/free_profile/140145/HSHS_St_Joseph_s_Hospital_Breese/Breese/Illinois/
		
* missing years
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
.	2010	150057				150057	ST FRANCIS HOSPITAL MOORESVILLE	150057	Unmerged from M&A	
.	2011	150057				150057	FRANCISCAN ST FRANCIS HEALTH - MOORESVILLE	150057	Unmerged from M&A	
Matched (3)	2014	150057	franciscan st. francis health mooresville	150057	150057	150057	FRANCISCAN ST FRANCIS HEALTH - MOORESVILLE	150057	Initially Merged	General Medical & Surgical
Matched (3)	2015	150057	franciscan st. francis health mooresville	150057	150057	150057	FRANCISCAN ST FRANCIS HEALTH - MOORESVILLE	150057	Initially Merged	General Medical & Surgical
Matched (3)	2016	150057	franciscan health mooresville	150057	150057	150057	FRANCISCAN HEALTH MOORESVILLE	150057	Initially Merged	General Medical & Surgical
Matched (3)	2017	150057	franciscan health mooresville	150057	150057	150057	FRANCISCAN HEALTH MOORESVILLE	150057	Initially Merged	General Medical & Surgical		
	* have 2005-2007 and then 2014-2017 in AHA/HIMSS	
	* ahanumber: 6421030
		
* once I have loaded the M&A data: _____________________________________________
 
	* this one matches for multiple years except 2013 - is it in the M&A data in 2013?
_merge	year	medicarenumber	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Master only (1)	2014	100139	100139	100139	100322	REGIONAL GENERAL HOSPITAL WILLISTON	100322	Secondary Merge	General Medical & Surgical
Master only (1)	2013	100139	100139	100139	100322			Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2015	100139	100139	100139	100322	REGIONAL GENERAL HOSPITAL WILLISTON	100322	Secondary Merge	General Medical & Surgical
Master only (1)	2016	100139	100139	100139	100322	REGIONAL GENERAL HOSPITAL WILLISTON	100322	Secondary Merge	General Medical & Surgical
* The hospital, once known as Tri-County Hospital, has existed for 50 years. The next closest hospitals are in Gainesville and Ocala. Lander said the hospital has taken a positive turn since Pagidipati purchased it in 2014, shortly after it went through bankruptcy reorganization in 2013.
* notably, the M&A dataset doesn't have this change – it is missing 2013, when the bankruptcy happened. And no record of an ownership change in 2014. 	
	
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
		* says that this facility became an urgent care center on april 16 2025
		* now at the same address is Columbus Specialty Hospital (CCN 112012)
	
	* this one is in the M&A data but not the AHA/HIMSS from 2010-2013, then is in both	
	_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
.	2010	130067				130067	IDAHO DOCTORS HOSPITAL	130067	Unmerged from M&A	
.	2011	130067				130067	IDAHO DOCTORS HOSPITAL	130067	Unmerged from M&A	
.	2012	130067				130067	IDAHO DOCTORS HOSPITAL	130067	Unmerged from M&A	
.	2013	130067				130067	IDAHO DOCTORS HOSPITAL	130067	Unmerged from M&A	
Matched (3)	2014	130067	idaho doctors hospital	130067	130067	130067	IDAHO DOCTORS HOSPITAL	130067	Initially Merged	Other Specialty
Matched (3)	2015	130067	idaho doctors hospital	130067	130067	130067	IDAHO DOCTORS HOSPITAL	130067	Initially Merged	Other Specialty
Matched (3)	2016	130067	idaho doctors hospital	130067	130067	130067	IDAHO DOCTORS HOSPITAL	130067	Initially Merged	Other Specialty
Matched (3)	2017	130067	idaho doctors hospital	130067	130067	130067	IDAHO DOCTORS HOSPITAL	130067	Initially Merged	Other Specialty
	
		* this one is never in the M&A data (it seems)
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Master only (1)	2010	140079	st. james hospital health centers	140079	140079	192050			Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2011	140079	franciscan st. james health	140079	140079	192050			Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2012	140079	franciscan st. james health chicago heights	140079	140079	192050			Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2013	140079	franciscan st. james health chicago heights	140079	140079	192050			Unmerged from AHA/HIMSS	General Medical & Surgical
	* HMM it seems like there is another franciscan st james health that only matches in 2017.....
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
.	2010	140172				140172	ST JAMES HOSP & HLTH CTR-OLYMPIA FLDS	140172	Unmerged from M&A	
.	2011	140172				140172	FRANCISCAN ST JAMES HEALTH	140172	Unmerged from M&A	
.	2012	140172				140172	FRANCISCAN ST JAMES HEALTH	140172	Unmerged from M&A	
.	2013	140172				140172	FRANCISCAN ST JAMES HEALTH	140172	Unmerged from M&A	
.	2014	140172				140172	FRANCISCAN ST JAMES HEALTH	140172	Unmerged from M&A	
.	2015	140172				140172	FRANCISCAN ST JAMES HEALTH	140172	Unmerged from M&A	
Matched (3)	2017	140172	franciscan health olympia fields	140172	140172	140172	FRANCISCAN HEALTH OLYMPIA & CHICAGO HEIGHTS	140172	Initially Merged	General Medical & Surgical

* skips years - merged only in 2016
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
.	2010	150003				150003	ST ELIZABETH CENTRAL	150003	Unmerged from M&A	
.	2011	150003				150003	AHHS CRYSTAL LAKE BRANCH OFFICE	150003	Unmerged from M&A	
.	2012	150003				150003	AHHS CRYSTAL LAKE BRANCH OFFICE	150003	Unmerged from M&A	
.	2013	150003				150003	FRANCISCAN ST ELIZABETH HEALTH - LAFAYETTE CENTRAL	150003	Unmerged from M&A	
Master only (1)	2016	150003	franciscan health lafayette east	150003	150003	150109	FRANCISCAN HEALTH LAFAYETTE	150109	Secondary Merge	General Medical & Surgical
	* M&A data always available for CCN: 150109. Would work if used that CCN in the merge instead. 

* weirdness with CCNs
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
.	2010	150033				150033	ST FRANCIS HOSPITAL AND HEALTH CENTERS	150033	Unmerged from M&A	
.	2011	150033				150033	FRANCISCAN ST FRANCIS HEALTH - BEECH GROVE	150033	Unmerged from M&A	
Master only (1)	2012	150033	franciscan st. francis health indianapolis	150033	150033	150162	FRANCISCAN ST FRANCIS HEALTH - INDIANAPOLIS	150162	Secondary Merge	General Medical & Surgical
Master only (1)	2013	150033	franciscan st. francis health indianapolis	150033	150033	150162	FRANCISCAN ST FRANCIS HEALTH - INDIANAPOLIS	150162	Secondary Merge	General Medical & Surgical
Master only (1)	2014	150033	franciscan st. francis health indianapolis	150033	150033	150162	FRANCISCAN ST FRANCIS HEALTH - INDIANAPOLIS	150162	Secondary Merge	General Medical & Surgical
	* Franciscan Health Indianapolis announced plans in 2008 to consolidate services from its Beech Grove[11] to its Indianapolis campus upon completion of an inpatient bed tower in 2011.[12] The first phase of the tower construction opened in April 2011.[13][14] The Beech Grove hospital closed all inpatient and emergency services in March 2012. Outpatient services are still available. https://en.wikipedia.org/wiki/Franciscan_Health_Indianapolis

* falls out of M&A sample in 2017
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Matched (3)	2010	171366	harper hospital	171366	171366	171366	HARPER HOSPITAL DISTRICT NO 5	171366	Initially Merged	Critical Access
Matched (3)	2011	171366	harper hospital	171366	171366	171366	HARPER HOSPITAL DISTRICT NO 5	171366	Initially Merged	Critical Access
Matched (3)	2012	171366	harper hospital	171366	171366	171366	HARPER HOSPITAL DISTRICT NO 5	171366	Initially Merged	Critical Access
Matched (3)	2013	171366	harper hospital	171366	171366	171366	HARPER HOSPITAL DISTRICT NO 5	171366	Initially Merged	Critical Access
Matched (3)	2014	171366	harper hospital	171366	171366	171366	HARPER HOSPITAL DISTRICT NO 5	171366	Initially Merged	Critical Access
Matched (3)	2015	171366	harper hospital	171366	171366	171366	HARPER HOSPITAL DISTRICT NO 5	171366	Initially Merged	Critical Access
Matched (3)	2016	171366	harper hospital	171366	171366	171366	HARPER HOSPITAL DISTRICT NO 5	171366	Initially Merged	Critical Access
Master only (1)	2017	171366	harper hospital	171366	171366	171346			Unmerged from AHA/HIMSS	Critical Access
* https://www.ahd.com/free_profile/171366/_Hospital_District_%236_-_Harper_Campus/Harper/Kansas/ consolidated with 171346 officially in 2019. But earlier? 
* last operational year seems to have been 2017 - in 2018, terminated its 340B participation due to "site closure" https://340bopais.hrsa.gov/CePrint/27999?AspxAutoDetectCookieSupport=1

* never in M&A?
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Master only (1)	2010	180014	norton audubon hospital	180014	180014				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2011	180014	norton audubon hospital	180014	180014				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2012	180014	norton audubon hospital	180014	180014				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2013	180014	norton audubon hospital	180014	180014				Unmerged from AHA/HIMSS	General Medical & Surgical

* never in M&A 
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Master only (1)	2010	180037	sts. mary elizabeth hospital	180037	180037				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2011	180037	sts. mary elizabeth hospital	180037	180037				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2012	180037	sts. mary elizabeth hospital	180037	180037				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2013	180037	sts. mary elizabeth hospital	180037	180037				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2014	180037	sts. mary elizabeth hospital	180037	180037				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2015	180037	sts. mary elizabeth hospital	180037	180037				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2016	180037	sts. mary elizabeth hospital	180037	180037				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2017	180037	sts. mary elizabeth hospital	180037	180037				Unmerged from AHA/HIMSS	General Medical & Surgical

* never in M&A 
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Master only (1)	2010	180123	norton suburban hospital	180123	180123				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2011	180123	norton suburban hospital	180123	180123				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2012	180123	norton suburban hospital	180123	180123				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2013	180123	norton suburban hospital	180123	180123				Unmerged from AHA/HIMSS	General Medical & Surgical
		
* never in M&A
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Master only (1)	2010	190182	tulane lakeside hospital	190182	190182				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2011	190182	tulane lakeside hospital	190182	190182				Unmerged from AHA/HIMSS	General Medical & Surgical
		
* never in M&A 
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Master only (1)	2010	210010	dorchester general hospital	210010	210010				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2011	210010	dorchester general hospital	210010	210010				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2012	210010	dorchester general hospital	210010	210010				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2013	210010	um shore medical center at dorchester	210010	210010				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2014	210010	um shore medical center at dorchester	210010	210010				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2015	210010	um shore medical center at dorchester	210010	210010				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2016	210010	um shore medical center at dorchester	210010	210010				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2017	210010	um shore medical center at dorchester	210010	210010				Unmerged from AHA/HIMSS	General Medical & Surgical
		
* missing some years in M&A? 
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Master only (1)	2010	210025	western maryland regional medical center	210025	210025				Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2011	210025	western maryland regional medical center	210025	210025	210027	WESTERN MARYLAND REGIONAL MEDICAL CENTER	210027	Secondary Merge	General Medical & Surgical
Master only (1)	2012	210025	western maryland regional medical center	210025	210025	210027	WESTERN MARYLAND REGIONAL MEDICAL CENTER	210027	Secondary Merge	General Medical & Surgical
Master only (1)	2013	210025	western maryland regional medical center	210025	210025	210027	WESTERN MARYLAND REGIONAL MEDICAL CENTER	210027	Secondary Merge	General Medical & Surgical
Master only (1)	2014	210025	western maryland regional medical center	210025	210025	210027	WESTERN MARYLAND REGIONAL MEDICAL CENTER	210027	Secondary Merge	General Medical & Surgical
Matched (3)	2015	210027	western maryland regional medical center	210027	210027	210027	WESTERN MARYLAND REGIONAL MEDICAL CENTER	210027	Initially Merged	General Medical & Surgical
Matched (3)	2016	210027	western maryland regional medical center	210027	210027	210027	WESTERN MARYLAND REGIONAL MEDICAL CENTER	210027	Initially Merged	General Medical & Surgical
Matched (3)	2017	210027	western maryland regional medical center	210027	210027	210027	WESTERN MARYLAND REGIONAL MEDICAL CENTER	210027	Initially Merged	General Medical & Surgical
		
* weird.... 
_merge	year	medicarenumber	entity_name	mcrnum_x	cleaned_mcr	mcrnum_y	fac_name	medicare_ccn	merge_status	type
Matched (3)	2010	210054	southern maryland hospital center	210054	210054	210054	SOUTHERN MARYLAND HOSPITAL CENTER	210054	Initially Merged	General Medical & Surgical
Matched (3)	2011	210054	southern maryland hospital center	210054	210054	210054	SOUTHERN MARYLAND HOSPITAL CENTER	210054	Initially Merged	General Medical & Surgical
Master only (1)	2012	210054	southern maryland hospital center	210054	210054	210062			Unmerged from AHA/HIMSS	General Medical & Surgical
Master only (1)	2013	210054	medstar southern maryland hospital center	210054	210054	210062	MEDSTAR SOUTHERN MARYLAND HOSPITAL CENTER	210062	Secondary Merge	General Medical & Surgical
Master only (1)	2014	210054	medstar southern maryland hospital center	210054	210054	210062	MEDSTAR SOUTHERN MARYLAND HOSPITAL CENTER	210062	Secondary Merge	General Medical & Surgical
Master only (1)	2015	210054	medstar southern maryland hospital	210054	210054	210062	MEDSTAR SOUTHERN MARYLAND HOSPITAL CENTER	210062	Secondary Merge	General Medical & Surgical
		

		