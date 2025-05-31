/* ADD_MERGERS_AQC *****************************************************************

Program name: 	01_add_mergers_acq.do
Programmer: 	Julia Paris

Goal: 			Set globals for Hospital CEOs analysis

*******************************************************************************/

* check setup is complete
	check_setup
	
* Load data
	use "${dbdata}/derived/himss_aha_hospitals_0529.dta", clear

	* test merge variable
	codebook medicarenumber
	* missing 6,426/86,586 observations
	tab year
	
* Prepare M&A data for merge	
	preserve 
		use "${dbdata}/supplemental/strategic_ma_db_v2/strategic_ma_db_v2.dta", clear
		rename medicare_ccn_str medicarenumber
		
		* collapse quarterly data to annual data
		foreach var in bankruptcy system_exit system_split merger_of_equals target {
			bysort medicarenumber year: egen `var'_yr = max(`var')
		}
		
		* keep the last observation in the year
		bysort medicarenumber year (year_qtr): keep if _n == _N
		
		* ensure that data are now unique by CCN-year 
		bysort medicarenumber year: gen dup = _N
		assert dup == 1
		drop dup
		
		keep if year <= 2017
		
		tempfile ma_data
		save `ma_data'
	restore

	merge m:1 medicarenumber year using `ma_data'
