/* CEO_DESCRIPTIVES ************************************************************

Program name: 	05_ceo_descriptives.do
Programmer: 	Julia Paris

Goal: 			Compute descriptive stats for CEOs

*******************************************************************************/

* setup ________________________________________________________________________ 

* check setup is complete
	check_setup
	
* load data
	use "${dbdata}/derived/temp/indiv_file_contextual.dta", clear
	
* calculate CEO descriptive statistics _________________________________________


* descriptives table 1 (Table of hospital-level cross-sectional descriptive stats by year)

program define descriptives_table

	syntax , tabyear(integer)
	
//   	preserve

	* make a variable capturing whether a person is a system CEO this year
		replace system_ceo = 0 if entity_type == "Single Hospital Health System"
		bysort contact_uniqueid year: egen system_ceo_thisyear = max(system_ceo) // needs to be done before we drop systems
		
	* ------- keep hospitals only -------
		keep if is_hospital == 1 & entity_type == "Hospital"
		restrict_hosp_sample // will keep only general and CAH
		
	* ------- sort observations into categories for the columns -------
		/* Columns
			– Overall (column 1)
			– For-profit vs. non-profit (column 2-3)
			– System vs. independent hospitals (3 categories: single, small, large)
			– PE vs. not 
		*/
		
		* For-profit vs. non-profit (column 2-3)
		tab forprofit, m // can use this for profit status (IMPUTED FROM PARENTS WHEN MISSING)
		 
		* System vs independent: 
		bysort parent_system entity_uniqueid year: gen system_counter = 1 if _n == 1
		bysort parent_system year: egen system_N = total(system_counter)
			* minimum 3
		gen system_ind_cleaned_3min = entity_type_parent == "IDS/RHA" & system_N >= 3
			* 3 categories
		gen system_cat = 1 if entity_type_parent == "Single Hospital Health System"
		replace system_cat = 2 if entity_type_parent == "IDS/RHA" & system_N < 10
		replace system_cat = 3 if entity_type_parent == "IDS/RHA" & system_N >= 10
			* need to decide what to do with these: 
		count if  entity_type_parent == "IDS/RHA" & system_N == 1
		
		* private equity vs not
		egen pe_acq_row = rowmax(pe_acq_already pe_acq_window)
			* 2017
		bysort entity_uniqueid: egen pe_acq_by2017 = max(pe_acq_row)
		replace pe_acq_by2017 = 0 if missing(pe_acq_by2017)
		if "`tabyear'" != "2017" {
			gen pe_acq_by`tabyear' = 1 if pe_acq_already == 1
			replace pe_acq_by`tabyear' = 1 if pe_acq_window_year <= `tabyear'
			replace pe_acq_by`tabyear' = 0 if missing(pe_acq_by`tabyear')
		}
			
		tempfile base
		save `base'
		
	* ------- make panel A variables for rows -------
		
		* for CEO, can use hosp_has_ceo indicator
		
		* Define roles 
			* COO
			local role_coo_suffix        "coo"
			local role_coo_stdpattern    "COO:"
			local role_coo_titlepatterns ///
				`" "COO" "Chief Operating Officer" "chief operating officer" "'
				
			* CFO 
			local role_cfo_suffix        "cfo"
			local role_cfo_stdpattern    "CFO:"
			local role_cfo_titlepatterns ///
				`" "CFO" "Chief Financial Officer" "chief financial officer" "'

// 			* CMO - not really used for hospitals in HIMSS
// 			local role_cmo_suffix        "cmo"
// 			local role_cmo_stdpattern    "CMO:"
// 			local role_cmo_titlepatterns ///
// 				`" "CMO" "Chief Medical Officer" "chief medical officer" "'	
			
			* cco	
			local role_cco_suffix        "cco"
			local role_cco_stdpattern    "Chief Compliance Officer"
			local role_cco_titlepatterns ///
				`" "Chief Compliance Officer" "chief compliance officer" "'	

			* mcs	
			local role_mcs_suffix        "mcs"
			local role_mcs_stdpattern    "Medical Staff Chief"
			local role_mcs_titlepatterns ///
				`" "Medical Staff Chief" "Chief of Medical Staff" "Medical Chief of Staff" "'	
				
			* CNH	
			local role_cnh_suffix        "cnh"
			local role_cnh_stdpattern    "Chief Nursing Head"
			local role_cnh_titlepatterns ///
				`" "CNH" "Chief Nursing Head" "chief nursing head" "'	
				
		* Loop over roles
			foreach role in coo cfo /* cmo */ cco cnh mcs {

				local suffix        "`role_`role'_suffix'"
				local stdpattern    "`role_`role'_stdpattern'"
				local titlepatterns "`role_`role'_titlepatterns'"

				* Initial flag from title_standardized
				gen char_`suffix' = regexm(title_standardized, "`stdpattern'")

				* Check whether hospital already has role that year
				bysort entity_uniqueid year: egen temp_hosp_has_`suffix' = max(char_`suffix')

				* Fallback: search raw title only if none found yet
				foreach pat of local titlepatterns {
					replace char_`suffix' = 1 ///
						if regexm(title, "`pat'") & temp_hosp_has_`suffix' == 0
				}

				drop temp_hosp_has_`suffix'

				* Final hospital-year indicator
				bysort entity_uniqueid year: egen hosp_has_`suffix' = max(char_`suffix')
			}
			
		* custom for CIO/CTO
		* CIO/CTO
		gen char_cio = regexm(title_standardized, "CIO:  Chief Information Officer")
		bysort entity_uniqueid year: egen temp_hosp_has_cio = max(char_cio)
			* add other title_standardized
		replace char_cio = 1 if regexm(title_standardized, "CNIS:") & temp_hosp_has_cio == 0
		replace char_cio = 1 if regexm(title_standardized, "CSIO/IT Security Officer") & temp_hosp_has_cio == 0
		replace char_cio = 1 if regexm(title_standardized, "Chief Medical Information Officer") & temp_hosp_has_cio == 0
			* add other title
		drop temp_hosp_has_cio
		bysort entity_uniqueid year: egen temp_hosp_has_cio = max(char_cio)
		replace char_cio = 1 if regexm(title, "CTO|Chief Technology Officer|chief technology officer|Chief Information Officer") & temp_hosp_has_cio == 0
			* final version
		drop temp_hosp_has_cio
		bysort entity_uniqueid year: egen hosp_has_cio = max(char_cio)
		
		* make a combined variable for COO and CFO
		gen temp_hosp_has_coo_cfo = hosp_has_coo*hosp_has_cfo
		bysort entity_uniqueid year: egen hosp_has_coo_cfo = max(temp_hosp_has_coo_cfo)
		
		* make a combined "technical" csuite variable (excluding CMO)
		gen temp_hosp_has_technical = hosp_has_cnh*hosp_has_cio*hosp_has_mcs
		bysort entity_uniqueid year: egen hosp_has_technical = max(temp_hosp_has_technical)
		
		* number of distinct roles at hospital
			* ndistinct_std: number of unique values of title_standardized
			bysort entity_uniqueid year title_standardized: gen temp_ndistinct_std = 1 if _n == 1
			bysort entity_uniqueid year: egen ndistinct_std = total(temp_ndistinct_std)
			* ndistinct_raw: number of unique job role observations in the hospital-year
			bysort entity_uniqueid year: gen ndistinct_raw = _N
			* ndistinct_title
			bysort entity_uniqueid year title: gen temp_ndistinct_title = 1 if _n == 1
			bysort entity_uniqueid year: egen ndistinct_title = total(temp_ndistinct_title)
			* ndistinct_ind
			bysort entity_uniqueid year contact_uniqueid: gen temp_ndistinct_ind = 1 if _n == 1
			bysort entity_uniqueid year: egen ndistinct_ind = total(temp_ndistinct_ind)
			* investigate
// 			codebook ndistinct*
	
		* make a variable for a shared CEO
		bysort contact_uniqueid year: egen count_char_ceo = total(char_ceo)
		gen shared_ceo = (count_char_ceo > 1 & char_ceo == 1)
		bysort entity_uniqueid year: egen hosp_has_shared_ceo = max(shared_ceo)
		
		tempfile base_withvars
		save `base_withvars'
		
	* ------- keep year only -------
		keep if year == `tabyear'
		
	* ------- make panel A -------	
		
		bysort entity_uniqueid year: keep if _n ==1 // make unique by hospital	
		gen count = 1
		tempfile hosp_`tabyear'
		save `hosp_`tabyear''
		
		local collapsevarsA "hosp_has_ceo hosp_has_coo hosp_has_cfo hosp_has_coo_cfo hosp_has_cco hosp_has_cnh hosp_has_cio hosp_has_mcs hosp_has_technical hosp_has_shared_ceo ndistinct_raw ndistinct_std ndistinct_title ndistinct_ind (rawsum) count"
		
		* ALL
		collapse `collapsevarsA', by(year)
		gen type = "All"
		tempfile panelA_all
		save `panelA_all'
		
		use `hosp_`tabyear'', clear
		
		* FP/NFP
		collapse `collapsevarsA', by(year forprofit)
		drop if missing(forprofit)
		gen type = "For-Profit" if forprofit == 1
		replace type = "Non-Profit" if forprofit == 0
		egen temp_total = total(count)
		gen share = count/temp_total
		drop temp_total
		tempfile panelA_fp_nfp
		save `panelA_fp_nfp'
		
		use `hosp_`tabyear'', clear
		
		* SYS/NON-SYS
		collapse `collapsevarsA', by(year system_cat)
		drop if missing(system_cat)
		gen type = "Single" if system_cat == 1
		replace type = "Small System" if system_cat == 2
		replace type = "Large System" if system_cat == 3
		egen temp_total = total(count)
		gen share = count/temp_total
		drop temp_total
		tempfile panelA_sys
		save `panelA_sys'
		
		use `hosp_`tabyear'', clear
		
		* PE/NON-PE
		collapse `collapsevarsA', by(year pe_acq_by`tabyear')
		drop if missing(pe_acq_by`tabyear')
		gen type = "PE-Acquired" if pe_acq_by`tabyear' == 1
		replace type = "Non-PE" if pe_acq_by`tabyear' == 0
		egen temp_total = total(count)
		gen share = count/temp_total
		drop temp_total
		tempfile panelA_pe
		save `panelA_pe'
		
		* APPEND
		use `panelA_all', clear
		append using `panelA_fp_nfp'
		append using `panelA_sys'
		append using `panelA_pe'
		encode type, gen(type_id)
		drop type
		
		xpose, clear varname
		
		* clean
		rename v1 all
		rename v2 nfp
		rename v3 fp 
		rename v4 nonsys
		rename v5 sys_small
		rename v6 sys_big
		rename v7 nonpe
		rename v8 pe
		drop if _varname == "forprofit" |  _varname == "system_cat" |  _varname == "type_id" | _varname == "year" |  _varname == "ndistinct_std" | _varname == "pe_acq_by`tabyear'"
		order _varname all fp nfp nonsys sys_small sys_big pe nonpe
		
	* ------- export panel A -------	
		
		gen str60 rowlabel = _varname
		replace rowlabel = "CEO present (\%)"                    if _varname == "hosp_has_ceo"
		replace rowlabel = "COO present (\%)"                    if _varname == "hosp_has_coo"
		replace rowlabel = "CFO present (\%)"                    if _varname == "hosp_has_cfo"
		replace rowlabel = "COO and CFO present (\%)"             if _varname == "hosp_has_coo_cfo"
		replace rowlabel = "CCO present (\%)"     				if _varname == "hosp_has_cco"
		replace rowlabel = "CNH present (\%)"     				if _varname == "hosp_has_cnh"
		replace rowlabel = "CIO present (\%)"                    if _varname == "hosp_has_cio"
		replace rowlabel = "Medical Staff Chief present (\%)"     		if _varname == "hosp_has_mcs"
		replace rowlabel = "All technical execs present (\%)"         if _varname == "hosp_has_technical"
		replace rowlabel = "Shared CEO (\%)"             if _varname == "hosp_has_shared_ceo"
		replace rowlabel = "Distinct roles (count)"          if _varname == "ndistinct_raw"
		replace rowlabel = "Distinct titles (count)"          if _varname == "ndistinct_title"
		replace rowlabel = "Distinct individuals (count)"          if _varname == "ndistinct_ind"
		replace rowlabel = "Observations"			          if _varname == "count"
		replace rowlabel = "Share"			          			if _varname == "share"
		

		local fmt "%9.2f"
		local fmt_ct "%9.0fc"
		
		foreach v in all fp nfp sys_small sys_big nonsys pe nonpe {
			replace `v' = 100*`v' if inlist(_varname, ///
				"hosp_has_ceo","hosp_has_coo","hosp_has_cfo","hosp_has_coo_cfo", ///
				"hosp_has_cnh","hosp_has_cio","hosp_has_technical","hosp_has_shared_ceo","hosp_has_mcs")
			replace `v' = 100*`v' if inlist(_varname,"share","hosp_has_cco")
		}
		
		local outfile "${overleaf}/tables/outline_descriptives_table1_panelA_`tabyear'.tex"
		cap file close fh
		file open fh using "`outfile'", write replace text

		file write fh "\begin{table}[!htbp]" _n
		file write fh "\centering" _n
		file write fh "\caption{Hospital Leadership Roles, `tabyear' (Panel A)}" _n
		file write fh "\label{tab:outline_descriptives_table1_panelA_`tabyear'}" _n
		file write fh "\begin{tabular}{lcccccccc}" _n
		file write fh "\toprule" _n

		* Group header row
		file write fh " & \multicolumn{1}{c}{} & \multicolumn{2}{c}{Profit Status} & \multicolumn{3}{c}{System Affiliation} & \multicolumn{2}{c}{Private Equity} \\" _n
		file write fh "\cmidrule(lr){3-4}\cmidrule(lr){5-7}\cmidrule(lr){8-9}" _n
		file write fh " & \multicolumn{1}{c}{All} & \multicolumn{1}{c}{For-Profit} & \multicolumn{1}{c}{Non-Profit} & \multicolumn{1}{c}{Non-System} & \multicolumn{1}{c}{Small System} & \multicolumn{1}{c}{Large System} & \multicolumn{1}{c}{PE-Acquired} & \multicolumn{1}{c}{Non-PE} \\" _n
		file write fh "\midrule" _n

		* Body rows
		quietly {
			forvalues i = 1/`=_N' {
				if _varname[`i'] == "count" {
					file write fh "\midrule" _n
					local r = rowlabel[`i']

					local a  : display `fmt_ct' all[`i']
					local fp : display `fmt_ct' fp[`i']
					local nf : display `fmt_ct' nfp[`i']
					local sy_sm : display `fmt_ct' sys_small[`i']
					local sy_b : display `fmt_ct' sys_big[`i']
					local ns : display `fmt_ct' nonsys[`i']
					local pe : display `fmt_ct' pe[`i']
					local np : display `fmt_ct' nonpe[`i']

					* trim leading spaces from :display output
					local a  = strtrim("`a'")
					local fp = strtrim("`fp'")
					local nf = strtrim("`nf'")
					local sy_sm = strtrim("`sy_sm'")
					local sy_b = strtrim("`sy_b'")
					local ns = strtrim("`ns'")
					local pe = strtrim("`pe'")
					local np = strtrim("`np'")

					file write fh "`r' & `a' & `fp' & `nf' & `ns' & `sy_sm' & `sy_b' & `pe' & `np' \\" _n
				}
				else {
					local r = rowlabel[`i']

					local a  : display `fmt' all[`i']
					local fp : display `fmt' fp[`i']
					local nf : display `fmt' nfp[`i']
					local sy_sm : display `fmt' sys_small[`i']
					local sy_b : display `fmt' sys_big[`i']
					local ns : display `fmt' nonsys[`i']
					local pe : display `fmt' pe[`i']
					local np : display `fmt' nonpe[`i']

					* trim leading spaces from :display output
					local a  = strtrim("`a'")
					local fp = strtrim("`fp'")
					local nf = strtrim("`nf'")
					local sy_sm = strtrim("`sy_sm'")
					local sy_b = strtrim("`sy_b'")
					local ns = strtrim("`ns'")
					local pe = strtrim("`pe'")
					local np = strtrim("`np'")

					file write fh "`r' & `a' & `fp' & `nf' & `ns' & `sy_sm' & `sy_b' & `pe' & `np' \\" _n
				}
			
			}
		}

		file write fh "\bottomrule" _n
		file write fh "\end{tabular}" _n
		file write fh "\vspace{0.25em} \\" _n
		file write fh "\footnotesize Notes: Entries are means by subgroup." _n
		file write fh "\end{table}" _n

		file close fh

		di as result "Wrote LaTeX table to: `outfile'"
		
	* ------- make panel B variables -------	
	
		use `base_withvars', clear
		
		* make a previously CNH variable
		bysort contact_uniqueid year: egen char_cnh_yr = max(char_cnh)
		bysort contact_uniqueid (year): gen prev_cnh = char_cnh_yr if _n == 1
		bysort contact_uniqueid (year): replace prev_cnh = max(char_cnh_yr, prev_cnh[_n-1])
		
		* char_female
		* char_md 
		
		* clinical degree variable 
		gen char_clinical = char_md
		replace char_clinical = regexm(credentials,"MD|md|DO|PharmD|RN|PhD|PsyD")
		
		* for person-hospital combinations, count total number of years between 2009 and 2017
		drop if year > `tabyear'
		bysort contact_uniqueid entity_uniqueid year: gen year_at_hosp_0917 = 1 if _n == 1
		bysort contact_uniqueid entity_uniqueid: egen total_years_at_hosp_0917 = total(year_at_hosp_0917)
		
		* for person-hospital combinations, count total number of roles between 2009 and 2017
		bysort contact_uniqueid entity_uniqueid title_standardized: gen role_at_hosp_0917 = 1 if _n == 1
		bysort contact_uniqueid entity_uniqueid: egen total_roles_at_hosp_0917 = total(role_at_hosp_0917)
		
		* reduce sample to hospital CEOs only
		keep if hospital_ceo == 1 // same as char_ceo in this sample
		
		* make a CEO of own system indicator
		gen system_ceo_own = (contact_uniqueid == contact_uniqueid_parentceo & entity_type_parent != "Single Hospital Health System")
// 		replace system_ceo_own = . if entity_type_parent == "Single Hospital Health System" // Should we do this? 
		
		* count number of years per person (# CEO years)
		bysort contact_uniqueid year: gen year_as_ceo_0917 = 1 if _n == 1
		bysort contact_uniqueid: egen total_years_as_ceo_0917 = total(year_as_ceo_0917)
		
		* count number of facilities per person (# CEO facilities)
		bysort contact_uniqueid entity_uniqueid: gen hosp_as_ceo_0917 = 1 if _n == 1
		bysort contact_uniqueid: egen total_hosps_as_ceo_0917 = total(hosp_as_ceo_0917)
		
		* keep 2017 only
		keep if year == `tabyear'
		
	* ------- make panel B -------	
		
		gen count = 1
		tempfile hospceo_`tabyear'
		save `hospceo_`tabyear''
		
		local collapsevarsB "char_female char_clinical total_years_at_hosp_0917 total_years_as_ceo_0917 total_hosps_as_ceo_0917 total_roles_at_hosp_0917 system_ceo_thisyear system_ceo_own (rawsum) count"
		
		* ALL
		collapse `collapsevarsB', by(year)
		gen type = "All"
		tempfile panelB_all
		save `panelB_all'
		
		use `hospceo_`tabyear'', clear
		
		* FP/NFP
		collapse `collapsevarsB', by(year forprofit)
		drop if missing(forprofit)
		gen type = "For-Profit" if forprofit == 1
		replace type = "Non-Profit" if forprofit == 0
		egen temp_total = total(count)
		gen share = count/temp_total
		drop temp_total
		tempfile panelA_fp_nfp
		tempfile panelB_fp_nfp
		save `panelB_fp_nfp'
		
		use `hospceo_`tabyear'', clear
		
		* SYS/NON-SYS
		collapse `collapsevarsB', by(year system_cat)
		drop if missing(system_cat)
		gen type = "Single" if system_cat == 1
		replace type = "Small System" if system_cat == 2
		replace type = "Large System" if system_cat == 3
		egen temp_total = total(count)
		gen share = count/temp_total
		drop temp_total
		tempfile panelA_fp_nfp
		tempfile panelB_sys
		save `panelB_sys'
		
		use `hospceo_`tabyear'', clear
		
		* PE/NON-PE
		collapse `collapsevarsB', by(year pe_acq_by`tabyear')
		drop if missing(pe_acq_by`tabyear')
		gen type = "PE-Acquired" if pe_acq_by`tabyear' == 1
		replace type = "Non-PE" if pe_acq_by`tabyear' == 0
		egen temp_total = total(count)
		gen share = count/temp_total
		drop temp_total
		tempfile panelA_fp_nfp
		tempfile panelB_pe
		save `panelB_pe'
		
		* APPEND
		use `panelB_all', clear
		append using `panelB_fp_nfp'
		append using `panelB_sys'
		append using `panelB_pe'
		encode type, gen(type_id)
		drop type
		
		xpose, clear varname
		
		* clean
		rename v1 all
		rename v2 nfp
		rename v3 fp 
		rename v4 nonsys
		rename v5 sys_small
		rename v6 sys_big
		rename v7 nonpe
		rename v8 pe
		drop if _varname == "forprofit" |  _varname == "system_cat" |  _varname == "type_id" | _varname == "year" |  _varname == "ndistinct_std" | _varname == "pe_acq_by`tabyear'"
		order _varname all fp nfp nonsys sys_small sys_big pe nonpe
		
	* ------- export panel B -------	
		
		gen str60 rowlabel = _varname
		replace rowlabel = "Female (\%)"                    	if _varname == "char_female"
		replace rowlabel = "Clinical Degree (\%)"                    if _varname == "char_clinical"
		replace rowlabel = "Years at Hospital (Any Role)"                    if _varname == "total_years_at_hosp_0917"
		replace rowlabel = "Years as CEO (Any Hospital)"             if _varname == "total_years_as_ceo_0917"
		replace rowlabel = "Hospitals as CEO (Count)"             if _varname == "total_hosps_as_ceo_0917"
		replace rowlabel = "Unique Roles at Hospital (Count)"             if _varname == "total_roles_at_hosp_0917"
		replace rowlabel = "Also CEO of Any System (\%)"             if _varname == "system_ceo_thisyear"
		replace rowlabel = "Also CEO of Own System (\%)"             if _varname == "system_ceo_own"
		replace rowlabel = "Observations"			          	if _varname == "count"
		replace rowlabel = "Share"			          			if _varname == "share"

		local fmt "%9.2f"
		local fmt_ct "%9.0fc"
		
		foreach v in all fp nfp sys_small sys_big nonsys pe nonpe {
			replace `v' = 100*`v' if inlist(_varname, ///
				"char_female","char_clinical","system_ceo_thisyear","system_ceo_own","share")
		}
		
		local outfile "${overleaf}/tables/outline_descriptives_table1_panelB_`tabyear'.tex"
		cap file close fh
		file open fh using "`outfile'", write replace text

		file write fh "\begin{table}[!htbp]" _n
		file write fh "\centering" _n
		file write fh "\caption{Hospital CEO Characteristics, `tabyear' (Panel B)}" _n
		file write fh "\label{tab:outline_descriptives_table1_panelB_`tabyear'}" _n
		file write fh "\begin{tabular}{lcccccccc}" _n
		file write fh "\toprule" _n

		* Group header row
		file write fh " & \multicolumn{1}{c}{} & \multicolumn{2}{c}{Profit Status} & \multicolumn{3}{c}{System Affiliation} & \multicolumn{2}{c}{Private Equity} \\" _n
		file write fh "\cmidrule(lr){3-4}\cmidrule(lr){5-7}\cmidrule(lr){8-9}" _n
		file write fh " & \multicolumn{1}{c}{All} & \multicolumn{1}{c}{For-Profit} & \multicolumn{1}{c}{Non-Profit} & \multicolumn{1}{c}{Non-System} & \multicolumn{1}{c}{Small System} & \multicolumn{1}{c}{Large System} & \multicolumn{1}{c}{PE-Acquired} & \multicolumn{1}{c}{Non-PE} \\" _n
		file write fh "\midrule" _n

		* Body rows
		quietly {
			forvalues i = 1/`=_N' {
				if _varname[`i'] == "count" {
					file write fh "\midrule" _n
					local r = rowlabel[`i']

					local a  : display `fmt_ct' all[`i']
					local fp : display `fmt_ct' fp[`i']
					local nf : display `fmt_ct' nfp[`i']
					local sy_sm : display `fmt_ct' sys_small[`i']
					local sy_b : display `fmt_ct' sys_big[`i']
					local ns : display `fmt_ct' nonsys[`i']
					local pe : display `fmt_ct' pe[`i']
					local np : display `fmt_ct' nonpe[`i']

					* trim leading spaces from :display output
					local a  = strtrim("`a'")
					local fp = strtrim("`fp'")
					local nf = strtrim("`nf'")
					local sy_sm = strtrim("`sy_sm'")
					local sy_b = strtrim("`sy_b'")
					local ns = strtrim("`ns'")
					local pe = strtrim("`pe'")
					local np = strtrim("`np'")

					file write fh "`r' & `a' & `fp' & `nf' & `ns' & `sy_sm' & `sy_b' & `pe' & `np' \\" _n
				}
				else {
					local r = rowlabel[`i']

					local a  : display `fmt' all[`i']
					local fp : display `fmt' fp[`i']
					local nf : display `fmt' nfp[`i']
					local sy_sm : display `fmt' sys_small[`i']
					local sy_b : display `fmt' sys_big[`i']
					local ns : display `fmt' nonsys[`i']
					local pe : display `fmt' pe[`i']
					local np : display `fmt' nonpe[`i']

					* trim leading spaces from :display output
					local a  = strtrim("`a'")
					local fp = strtrim("`fp'")
					local nf = strtrim("`nf'")
					local sy_sm = strtrim("`sy_sm'")
					local sy_b = strtrim("`sy_b'")
					local ns = strtrim("`ns'")
					local pe = strtrim("`pe'")
					local np = strtrim("`np'")

					file write fh "`r' & `a' & `fp' & `nf' & `ns' & `sy_sm' & `sy_b' & `pe' & `np' \\" _n
				}
			
			}
		}

		file write fh "\bottomrule" _n
		file write fh "\end{tabular}" _n
		file write fh "\vspace{0.25em} \\" _n
		file write fh "\footnotesize Notes: Entries are means by subgroup." _n
		file write fh "\end{table}" _n

		file close fh

		di as result "Wrote LaTeX table to: `outfile'"
		
	* ------- make panel C variables -------	
	
		use `base_withvars', clear
	
		* keep categories of hospitals
		keep entity_uniqueid year forprofit system_cat pe_acq*
		collapse forprofit system_cat pe_acq*, by(entity_uniqueid year)
		tempfile hosp_cats
		save `hosp_cats'
	
		use "${dbdata}/derived/hospitals_with_turnover.dta", clear
		merge 1:1 entity_uniqueid year using `hosp_cats', gen(_merge_cats)
		
		* keep hospitals only -------
		keep if is_hospital == 1 & entity_type == "Hospital"
		restrict_hosp_sample // will keep only general and CAH
		
		* any turnover event
		bysort entity_uniqueid: egen ever_ceo_turnover = max(turnover_ceo)
		
		* average number of hospital CEO turnover events
		bysort entity_uniqueid: egen total_ceo_turnover = total(turnover_ceo)
		
		* turnover_ceo, turnover_cfo, turnover_coo
		
		*  turnover rate among technical c-suite (CMO/CNO/CTO/CIO)
		egen turnover_technical = rowmean(turnover_cmo turnover_cio turnover_cno) 
		
	* ------- collapse panel C variables -------	
		
		* keep year only
		keep if year == `tabyear'
		
		gen count = 1
		tempfile turnover_`tabyear'
		save `turnover_`tabyear''
		
		local collapsevarsC "turnover_ceo ever_ceo_turnover total_ceo_turnover turnover_cfo turnover_coo turnover_technical (rawsum) count"
		
		* ALL
		collapse `collapsevarsC', by(year)
		gen type = "All"
		tempfile panelC_all
		save `panelC_all'
		
		use `turnover_`tabyear'', clear
		
		* FP/NFP
		collapse `collapsevarsC', by(year forprofit)
		drop if missing(forprofit)
		gen type = "For-Profit" if forprofit == 1
		replace type = "Non-Profit" if forprofit == 0
		egen temp_total = total(count)
		gen share = count/temp_total
		drop temp_total
		tempfile panelC_fp_nfp
		save `panelC_fp_nfp'
		
		use `turnover_`tabyear'', clear
		
		* SYS/NON-SYS
		collapse `collapsevarsC', by(year system_cat)
		drop if missing(system_cat)
		gen type = "Single" if system_cat == 1
		replace type = "Small System" if system_cat == 2
		replace type = "Large System" if system_cat == 3
		egen temp_total = total(count)
		gen share = count/temp_total
		drop temp_total
		tempfile panelC_sys
		save `panelC_sys'
		
		use `turnover_`tabyear'', clear
		
		* PE/NON-PE
		collapse `collapsevarsC', by(year pe_acq_by`tabyear')
		drop if missing(pe_acq_by`tabyear')
		gen type = "PE-Acquired" if pe_acq_by`tabyear' == 1
		replace type = "Non-PE" if pe_acq_by`tabyear' == 0
		egen temp_total = total(count)
		gen share = count/temp_total
		drop temp_total
		tempfile panelC_pe
		save `panelC_pe'
		
		* APPEND
		use `panelC_all', clear
		append using `panelC_fp_nfp'
		append using `panelC_sys'
		append using `panelC_pe'
		encode type, gen(type_id)
		drop type
		
		xpose, clear varname
		
		* clean
		rename v1 all
		rename v2 nfp
		rename v3 fp 
		rename v4 nonsys
		rename v5 sys_small
		rename v6 sys_big
		rename v7 nonpe
		rename v8 pe
		drop if _varname == "forprofit" |  _varname == "system_cat" |  _varname == "type_id" | _varname == "year" |  _varname == "ndistinct_std" | _varname == "pe_acq_by`tabyear'"
		order _varname all fp nfp nonsys sys_small sys_big pe nonpe
		
	* ------- export panel B -------	
		
		gen str60 rowlabel = _varname
		replace rowlabel = "CEO Turnover Rate (\%)"                    	if _varname == "turnover_ceo"
		replace rowlabel = "Ever Have CEO Turnover (\%)"                    if _varname == "ever_ceo_turnover"
		replace rowlabel = "Number of Turnover Events"                    if _varname == "total_ceo_turnover"
		replace rowlabel = "CFO Turnover Rate (\%)"                    	if _varname == "turnover_cfo"
		replace rowlabel = "COO Turnover Rate (\%)"                    	if _varname == "turnover_coo"
		replace rowlabel = "Technical Turnover Rate (\%)"                    if _varname == "turnover_technical"
		replace rowlabel = "Observations"			          	if _varname == "count"
		replace rowlabel = "Share"			          			if _varname == "share"

		local fmt "%9.2f"
		local fmt_ct "%9.0fc"
		
		foreach v in all fp nfp sys_small sys_big nonsys pe nonpe {
			replace `v' = 100*`v' if inlist(_varname, ///
				"turnover_ceo","ever_ceo_turnover","turnover_cfo","turnover_coo","turnover_technical","share")
		}
		
		local outfile "${overleaf}/tables/outline_descriptives_table1_panelC_`tabyear'.tex"
		cap file close fh
		file open fh using "`outfile'", write replace text

		file write fh "\begin{table}[!htbp]" _n
		file write fh "\centering" _n
		file write fh "\caption{Hospital Turnover Characteristics, `tabyear' (Panel C)}" _n
		file write fh "\label{tab:outline_descriptives_table1_panelC_`tabyear'}" _n
		file write fh "\begin{tabular}{lcccccccc}" _n
		file write fh "\toprule" _n

		* Group header row
		file write fh " & \multicolumn{1}{c}{} & \multicolumn{2}{c}{Profit Status} & \multicolumn{3}{c}{System Affiliation} & \multicolumn{2}{c}{Private Equity} \\" _n
		file write fh "\cmidrule(lr){3-4}\cmidrule(lr){5-7}\cmidrule(lr){8-9}" _n
		file write fh " & \multicolumn{1}{c}{All} & \multicolumn{1}{c}{For-Profit} & \multicolumn{1}{c}{Non-Profit} & \multicolumn{1}{c}{Non-System} & \multicolumn{1}{c}{Small System} & \multicolumn{1}{c}{Large System} & \multicolumn{1}{c}{PE-Acquired} & \multicolumn{1}{c}{Non-PE} \\" _n
		file write fh "\midrule" _n

		* Body rows
		quietly {
			forvalues i = 1/`=_N' {
				if _varname[`i'] == "count" {
					file write fh "\midrule" _n
					local r = rowlabel[`i']

					local a  : display `fmt_ct' all[`i']
					local fp : display `fmt_ct' fp[`i']
					local nf : display `fmt_ct' nfp[`i']
					local sy_sm : display `fmt_ct' sys_small[`i']
					local sy_b : display `fmt_ct' sys_big[`i']
					local ns : display `fmt_ct' nonsys[`i']
					local pe : display `fmt_ct' pe[`i']
					local np : display `fmt_ct' nonpe[`i']

					* trim leading spaces from :display output
					local a  = strtrim("`a'")
					local fp = strtrim("`fp'")
					local nf = strtrim("`nf'")
					local sy_sm = strtrim("`sy_sm'")
					local sy_b = strtrim("`sy_b'")
					local ns = strtrim("`ns'")
					local pe = strtrim("`pe'")
					local np = strtrim("`np'")

					file write fh "`r' & `a' & `fp' & `nf' & `ns' & `sy_sm' & `sy_b' & `pe' & `np' \\" _n
				}
				else {
					local r = rowlabel[`i']

					local a  : display `fmt' all[`i']
					local fp : display `fmt' fp[`i']
					local nf : display `fmt' nfp[`i']
					local sy_sm : display `fmt' sys_small[`i']
					local sy_b : display `fmt' sys_big[`i']
					local ns : display `fmt' nonsys[`i']
					local pe : display `fmt' pe[`i']
					local np : display `fmt' nonpe[`i']

					* trim leading spaces from :display output
					local a  = strtrim("`a'")
					local fp = strtrim("`fp'")
					local nf = strtrim("`nf'")
					local sy_sm = strtrim("`sy_sm'")
					local sy_b = strtrim("`sy_b'")
					local ns = strtrim("`ns'")
					local pe = strtrim("`pe'")
					local np = strtrim("`np'")

					file write fh "`r' & `a' & `fp' & `nf' & `ns' & `sy_sm' & `sy_b' & `pe' & `np' \\" _n
				}
			
			}
		}

		file write fh "\bottomrule" _n
		file write fh "\end{tabular}" _n
		file write fh "\vspace{0.25em} \\" _n
		file write fh "\footnotesize Notes: Entries are means by subgroup." _n
		file write fh "\end{table}" _n

		file close fh

		di as result "Wrote LaTeX table to: `outfile'"
			
		
	* ------- make a COO over time -------	
		
		use `base_withvars', clear
		if `tabyear' == 2017 {
			bysort entity_uniqueid year: keep if _n ==1 // make unique by hospital-year
			
			gen hosp_has_nonshared_ceo = (hosp_has_ceo == 1 & hosp_has_shared_ceo == 0)
		
			tempfile hosp_all_coo
			save `hosp_all_coo'
			
			local collapsevarsCOO "hosp_has_ceo hosp_has_coo hosp_has_cfo hosp_has_coo_cfo hosp_has_cnh hosp_has_cio hosp_has_technical hosp_has_shared_ceo ndistinct_raw ndistinct_std hosp_has_nonshared_ceo"
			
			* ALL
			collapse `collapsevarsCOO', by(year)
			gen type = "All"
			twoway line hosp_has_nonshared_ceo year, ytitle("Share with Own CEO") ylabel(0(0.1)1)
				graph export "${overleaf}/tables/outline_descriptives_table2_CEOtimeseries.pdf", as(pdf) replace
			twoway line hosp_has_technical year, ytitle("All Technical Roles Filled") ylabel(0(0.1)1)
				graph export "${overleaf}/tables/outline_descriptives_table3_technicaltimeseries.pdf", as(pdf) replace
			tempfile panelCOO_all
			save `panelCOO_all'
			
			use `hosp_all_coo', clear
			
			* FP/NFP
			collapse `collapsevarsCOO', by(year forprofit)
			drop if missing(forprofit)
			gen type = "For-Profit" if forprofit == 1
			replace type = "Non-Profit" if forprofit == 0
			tempfile panelCOO_fp_nfp
			save `panelCOO_fp_nfp'
			
			use `hosp_all_coo', clear
			
			* SYS/NON-SYS
			collapse `collapsevarsCOO', by(year system_ind_cleaned_3min)
			drop if missing(system_ind_cleaned_3min)
			gen type = "System" if system_ind_cleaned_3min == 1
			replace type = "Non-System" if system_ind_cleaned_3min == 0
			tempfile panelCOO_sys
			save `panelCOO_sys'
			
			use `hosp_all_coo', clear
			
			* PE/NON-PE
			collapse `collapsevarsCOO', by(year pe_acq_by2017)
			drop if missing(pe_acq_by2017)
			gen type = "PE-Acquired" if pe_acq_by2017 == 1
			replace type = "Non-PE" if pe_acq_by2017 == 0
			tempfile panelCOO_pe
			save `panelCOO_pe'
			
			* APPEND
			use `panelCOO_all', clear
			append using `panelCOO_fp_nfp'
			append using `panelCOO_sys'
			append using `panelCOO_pe'
			encode type, gen(type_id)
			drop type
			
			twoway ///
				(line hosp_has_coo year if type_id==1, sort lcolor(black)) ///
				(line hosp_has_coo year if type_id==2, sort lcolor(navy)) ///
				(line hosp_has_coo year if type_id==4, sort lcolor (blue)) ///
				(line hosp_has_coo year if type_id==7, sort lcolor (maroon)) ///
				(line hosp_has_coo year if type_id==5, sort lcolor(red)) ///
				(line hosp_has_coo year if type_id==6, sort lcolor(dkorange)) ///
				(line hosp_has_coo year if type_id==3, sort lcolor(orange)), ///
				legend(order(1 "All" 2 "For-Profit" 3 "Non-Profit" 4 "System" 5 "Non-System" 6 "PE-Acquired" 7 "Non-PE")) ///
				ytitle("Share with COO")
			graph export "${overleaf}/tables/outline_descriptives_table1_COOtimeseries.pdf", as(pdf) replace
		}
		
		
	restore

end

// descriptives_table, tabyear(2017)
descriptives_table, tabyear(2015)
descriptives_table, tabyear(2010)

* step-up graphs? EXPERIMENTING

	* distinguish between system roles at a SHHS or IDS/RHA
	* take max by year?
	
	gen step = .
	
	* NON HOSPITAL, NON-CEO
	replace step = 1 if char_ceo == 0 /// not ceo
		& is_hospital == 0 /// not a hospital 
		& !inlist(entity_type,"Single Hospital Health System","IDS/RHA")
		
	* NON HOSPITAL, CEO
	replace step = 2 if char_ceo == 1 /// is ceo
		& is_hospital == 0 /// not a hospital 
		& !inlist(entity_type,"Single Hospital Health System","IDS/RHA")
		
	* HOSPITAL, NON-CEO
	replace step = 3 if char_ceo == 0 /// not ceo
		& (is_hospital == 1 | inlist(entity_type,"Single Hospital Health System")) /// is a hospital 
		& !inlist(entity_type,"IDS/RHA")
		
	* HOSPITAL, CEO
	replace step = 4 if char_ceo == 1 /// is ceo
		& (is_hospital == 1 | inlist(entity_type,"Single Hospital Health System")) /// is a hospital 
		& !inlist(entity_type,"IDS/RHA")
		
	* HOSPITAL SYSTEM, NON-CEO
	replace step = 5 if char_ceo == 0 /// not ceo
		& is_hospital == 0 /// is a hospital 
		& inlist(entity_type,"IDS/RHA")
		
	* HOSPITAL SYSTEM, CEO 
	replace step = 6 if char_ceo == 1 /// is ceo
		& is_hospital == 0 /// is a hospital 
		& inlist(entity_type,"IDS/RHA")
	
	* ALL
	preserve
		collapse (max) step char_ceo char_female char_md, by(contact_uniqueid year)
		xtset contact_uniqueid year // panel dataset
		tsfill // fill missing years
		
		bysort contact_uniqueid (year): gen year_obs = _n
		
		twoway (line step year_obs if contact_uniqueid == 74169) ///
			(line step year_obs if contact_uniqueid == 74279)
		
		
		collapse step, by(year_obs char_female)
		
		twoway (line step year_obs if char_female == 0) ///
				(line step year_obs if char_female == 1)
		
	restore
	
	* KEEP IF BECOMES HOSP SYS CEO
	preserve
		
		keep if ever_sys_ceo
		
		collapse (max) step char_ceo char_female char_md, by(contact_uniqueid year)
		xtset contact_uniqueid year // panel dataset
		tsfill // fill missing years
		
		bysort contact_uniqueid (year): gen year_obs = _n
		
		collapse step, by(year_obs char_female)
		
		twoway (line step year_obs if char_female == 0) ///
				(line step year_obs if char_female == 1)
		
	restore
	
	* SEPARATELY BY #YR-COHORT
	preserve
	
		keep if ever_hospital_ceo
		
		collapse (max) step char_ceo char_female char_md, by(contact_uniqueid year)
		xtset contact_uniqueid year // panel dataset
		tsfill // fill missing years
		
		bysort contact_uniqueid (year): gen year_obs = _n
		
		bysort contact_uniqueid: egen tot_years = max(year_obs)
		
		collapse step, by(year_obs tot_years char_female)
		
		twoway (line step year_obs if tot_years == 3 & char_female == 1) ///
				(line step year_obs if tot_years == 4 & char_female == 1) ///
				(line step year_obs if tot_years == 5 & char_female == 1) ///
				(line step year_obs if tot_years == 6 & char_female == 1) ///
				(line step year_obs if tot_years == 7 & char_female == 1) ///
				(line step year_obs if tot_years == 8 & char_female == 1) ///
				(line step year_obs if tot_years == 9 & char_female == 1)
		
		
		twoway (line step year_obs if tot_years == 3 & char_female == 0) ///
				(line step year_obs if tot_years == 4 & char_female == 0) ///
				(line step year_obs if tot_years == 5 & char_female == 0) ///
				(line step year_obs if tot_years == 6 & char_female == 0) ///
				(line step year_obs if tot_years == 7 & char_female == 0) ///
				(line step year_obs if tot_years == 8 & char_female == 0) ///
				(line step year_obs if tot_years == 9 & char_female == 0)
				
	restore
	
	* SEPARATELY BY #YR-COHORT and initial step
	preserve
	
// 		keep if ever_hospital_ceo // compatible with steps?
		
		collapse (max) step char_ceo char_female char_md, by(contact_uniqueid year)
		xtset contact_uniqueid year // panel dataset
		tsfill // fill missing years
		
		bysort contact_uniqueid (year): gen year_obs = _n
		bysort contact_uniqueid (year): gen init_step_yr1 = step if _n == 1
		bysort contact_uniqueid: egen init_step = max(init_step_yr1)
		bysort contact_uniqueid: egen tot_years = max(year_obs)
		
		collapse step, by(year_obs tot_years char_female init_step)
		
		forval i = 1/6 {
			twoway (line step year_obs if tot_years == 3 & char_female == 1 & init_step == `i') ///
				(line step year_obs if tot_years == 4 & char_female == 1 & init_step == `i') ///
				(line step year_obs if tot_years == 5 & char_female == 1 & init_step == `i') ///
				(line step year_obs if tot_years == 6 & char_female == 1 & init_step == `i') ///
				(line step year_obs if tot_years == 7 & char_female == 1 & init_step == `i') ///
				(line step year_obs if tot_years == 8 & char_female == 1 & init_step == `i') ///
				(line step year_obs if tot_years == 9 & char_female == 1 & init_step == `i'), ///
			   title("Female") ///
			   name(female_plot, replace)
		
			twoway (line step year_obs if tot_years == 3 & char_female == 0 & init_step == `i') ///
					(line step year_obs if tot_years == 4 & char_female == 0 & init_step == `i') ///
					(line step year_obs if tot_years == 5 & char_female == 0 & init_step == `i') ///
					(line step year_obs if tot_years == 6 & char_female == 0 & init_step == `i') ///
					(line step year_obs if tot_years == 7 & char_female == 0 & init_step == `i') ///
					(line step year_obs if tot_years == 8 & char_female == 0 & init_step == `i') ///
					(line step year_obs if tot_years == 9 & char_female == 0 & init_step == `i'), ///
				   title("Male") ///
				   name(male_plot, replace)
				   
			* Combine vertically
			graph combine female_plot male_plot, col(1) ///
				title("Initial Step `i': Career Progression by Gender") ///
				iscale(1) ///
				ycommon ///
				name(step`i', replace)

			* Export
			graph export "${overleaf}/notes/CEO Descriptives/figures/steps_ever_hospital_ceo_initstep`i'.pdf", as(pdf) name(step`i') replace
		}
		
	restore
	

* how common is it for there to be a woman who is second in commmand to a man?
	preserve
		restrict_hosp_sample
		
		gen maleceo = hospital_ceo == 1 & char_female == 0
		bysort entity_uniqueid year: egen hosp_has_maleceo = max(maleceo)
		
		tab hosp_has_maleceo
		* COO
		tab char_female if hosp_has_maleceo ==1 & regexm(title_standardized,"Chief Operating Officer")
		tab char_female if regexm(title_standardized,"Chief Operating Officer")
		* CFO
		tab char_female if hosp_has_maleceo ==1 & regexm(title_standardized,"Chief Financial Officer")
		tab char_female if regexm(title_standardized,"Chief Financial Officer")
		* slightly less likely to have women in these roles if CEO is male
	restore
	
* share of CEOs who are female by facility type
	preserve 
		replace entity_type= "SHHS" if entity_type == "Single Hospital Health System"
		display "Female Share"
		* female CEO share by entity_type
		tab entity_type if char_ceo ==1, sum(char_female)
		graph bar char_female if char_ceo ==1, over(entity_type) /// frequencies 
			title("Female CEO Share by Entity Type") ///
			ytitle("Female Share")
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceosharefemale_entity_type.pdf", as(pdf) name("Graph") replace
		graph bar (count) char_female if char_ceo ==1, over(entity_type) /// counts 
			title("Count of CEO Observations by Entity Type") ///
			ytitle("Count")
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceocount_entity_type.pdf", as(pdf) name("Graph") replace
		* detailed hospital type
		tab type if char_ceo ==1 & entity_type == "Hospital", sum(char_female)
		graph hbar char_female if char_ceo ==1 & entity_type == "Hospital", ///
			over(type, sort(1) descending label(angle(0))) ///
			ytitle("Female Share") ///
			title("Female CEO Share by Detailed Hospital Type") 
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceosharefemale_hospital_type_detail.pdf", as(pdf) name("Graph") replace
		graph hbar (count) char_female if char_ceo ==1 & entity_type == "Hospital", ///
			over(type) ///
			ytitle("Count") ///
			title("Count of CEO Observations by Detailed Hospital Type") 
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceocount_hospital_type_detail.pdf", as(pdf) name("Graph") replace
		
		display "MD Share"
		tab entity_type if char_ceo ==1, sum(char_md)
		graph bar char_md if char_ceo ==1, over(entity_type) /// frequencies 
			title("MD CEO Share by Entity Type") ///
			ytitle("MD Share")
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceosharemd_entity_type.pdf", as(pdf) name("Graph") replace
		tab type if char_ceo ==1 & entity_type == "Hospital", sum(char_md)
		graph hbar char_md if char_ceo ==1 & entity_type == "Hospital", ///
			over(type, sort(1) descending label(angle(0))) ///
			ytitle("MD Share") ///
			title("MD CEO Share by Detailed Hospital Type") 
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceosharemd_hospital_type_detail.pdf", as(pdf) name("Graph") replace
	
	restore


* most common jobs right before becoming hospital ceo?
	gen title_collapsed = "CEO" if char_ceo == 1
		replace title_collapsed = "COO" if regexm(title_standardized, "COO:")
		replace title_collapsed = "CFO" if regexm(title_standardized, "CFO:")
		replace title_collapsed = "CMO" if regexm(title_standardized, "Chief Medical Officer")
		replace title_collapsed = "CNH" if regexm(title_standardized, "Chief Nursing Head")
		replace title_collapsed = "Other C-Suite" if regexm(title_standardized, "Chief") &!regexm(title_standardized, "Medical Staff Chief") & missing(title_collapsed)
		replace title_collapsed = "Director" if regexm(title_standardized, "Director") & missing(title_collapsed)
		replace title_collapsed = "Head" if regexm(title_standardized, "Head|Medical Staff Chief|Pathology Chief") & !regexm(title_standardized, "Head of Facility") & missing(title_collapsed)
		replace title_collapsed = "Head of Non-Hospital Facility" if regexm(title_standardized, "Head of Facility") & missing(title_collapsed)
		
	* by gender
	preserve
		gen yeargained = year if gained_hosp_ceo == 1
		gen yeargained_minus = yeargained -1
		bysort contact_uniqueid: egen yearbeforeceo = max(yeargained_minus)
		
		tab title_standardized if year == yearbeforeceo
		tab title_standardized if year == yearbeforeceo & char_female == 1 // far more likely to be CNO
		tab title_standardized if year == yearbeforeceo & char_female == 0 // far more likely to be CFO
			
		gen count = 1
		collapse (count) count if year == yearbeforeceo, by(title_collapsed char_female)
		bysort char_female: egen total = total(count)
		gen share = count/total
		bysort char_female (share): egen rank = rank(-share)
		gen keep = rank <= 10 
		bysort title_collapsed : egen keep_role = max(keep)
		keep if keep_role & !missing(char_female)
		
		* graph bar
		gen share_pct = 100*share
		graph hbar (mean) share_pct, ///
			over(title_collapsed, sort(1) descending label(labsize(small))) ///
			by(char_female, cols(2) note("") title("Prior role before CEO")) ///
			blabel(bar, format(%3.1f)) ///
			ytitle("Share of CEOs (%)")
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_rolebeforeceo_female.pdf", as(pdf) name("Graph") replace
	restore
	* by profit
	preserve
		gen yeargained = year if gained_hosp_ceo == 1
		gen yeargained_minus = yeargained -1
		bysort contact_uniqueid: egen yearbeforeceo = max(yeargained_minus)
		
		tab title_collapsed if year == yearbeforeceo
		tab title_collapsed if year == yearbeforeceo & forprofit == 0
		tab title_collapsed if year == yearbeforeceo & forprofit == 1
		
		gen count = 1
		collapse (count) count if year == yearbeforeceo, by(title_collapsed forprofit)
		bysort forprofit: egen total = total(count)
		gen share = count/total
		bysort forprofit (share): egen rank = rank(-share)
		gen keep = rank <= 10 
		bysort title_collapsed : egen keep_role = max(keep)
		keep if keep_role & !missing(forprofit)
		
		* graph bar
		gen share_pct = 100*share
		graph hbar (mean) share_pct, ///
			over(title_collapsed, sort(1) descending label(labsize(small))) ///
			by(forprofit, cols(2) note("") title("Prior role before CEO")) ///
			blabel(bar, format(%3.1f)) ///
			ytitle("Share of CEOs (%)")
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_rolebeforeceo_forprofit.pdf", as(pdf) name("Graph") replace
	restore
	* by gender and profit
	preserve
		gen yeargained = year if gained_hosp_ceo == 1
		gen yeargained_minus = yeargained -1
		bysort contact_uniqueid: egen yearbeforeceo = max(yeargained_minus)
		
		gen count = 1
		collapse (count) count if year == yearbeforeceo, by(title_collapsed forprofit char_female)
		bysort forprofit char_female: egen total = total(count)
		gen share = count/total
		bysort forprofit char_female (share): egen rank = rank(-share)
		gen keep = rank <= 10 
		bysort title_collapsed : egen keep_role = max(keep)
		keep if keep_role & !missing(forprofit) & !missing(char_female)
		
		* graph bar
		gen share_pct = 100*share
		graph hbar (mean) share_pct if char_female == 1, ///
			over(title_collapsed, sort(1) descending label(labsize(small))) ///
			by(forprofit char_female, cols(2) note("") title("Prior role before CEO")) ///
			blabel(bar, format(%3.1f)) ///
			ytitle("Share of CEOs (%)") 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_rolebeforeceo_forprofit_female.pdf", as(pdf) name("Graph") replace
		graph hbar (mean) share_pct if char_female == 0, ///
			over(title_collapsed, sort(1) descending label(labsize(small))) ///
			by(forprofit char_female, cols(2) note("") title("Prior role before CEO")) ///
			blabel(bar, format(%3.1f)) ///
			ytitle("Share of CEOs (%)") 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_rolebeforeceo_forprofit_male.pdf", as(pdf) name("Graph") replace
	restore


* CEO MD and female shares (ownership variable: gov_priv_type)
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
		
		* combine fed, state and local into one government category
		replace gov_priv_type = 1 if gov_priv_type==2
	
		* make summary stats by hospital time
		collapse char_md char_female count_ceo_roles* (rawsum) count, by(year gov_priv_type)
		drop if missing(gov_priv_type)
		
		* make MD line graph over time		
		twoway line char_md year if gov_priv_type == 1, lcolor(orange) ///
		|| line char_md year if gov_priv_type == 3, lcolor(green) ///
		|| line char_md year if gov_priv_type == 4, lcolor(blue) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP")) ///
		title("Share of Hospitals with MD CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.025)0.1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_md_gov_priv_type.pdf", as(pdf) name("Graph") replace
		
		* make gender line graph over time
		twoway line char_female year if gov_priv_type == 1, lcolor(orange) ///
		|| line char_female year if gov_priv_type == 3, lcolor(green) ///
		|| line char_female year if gov_priv_type == 4, lcolor(blue) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP")) ///
		title("Share of Hospitals with Female CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_female_gov_priv_type.pdf", as(pdf) name("Graph") replace
		
		* make line graph for CEO # roles
		twoway ///
			line count_ceo_roles_1 year if gov_priv_type == 4, lcolor("198 219 239") || ///
			line count_ceo_roles_2 year if gov_priv_type == 4, lcolor("107 174 214") || ///
			line count_ceo_roles_3 year if gov_priv_type == 4, lcolor("8 81 156") || ///
			line count_ceo_roles_1 year if gov_priv_type == 3, lcolor("199 233 192") || ///
			line count_ceo_roles_2 year if gov_priv_type == 3, lcolor("116 196 118") || ///
			line count_ceo_roles_3 year if gov_priv_type == 3, lcolor("35 139 69") || ///
			line count_ceo_roles_1 year if gov_priv_type == 1, lcolor("253 208 162") || ///
			line count_ceo_roles_2 year if gov_priv_type == 1, lcolor("253 141 60") || ///
			line count_ceo_roles_3 year if gov_priv_type == 1, lcolor("217 72 16") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles" ///
						 7 "Govt, 1 Role" ///
						 8 "Govt, 2 Roles" ///
						 9 "Govt, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_roles1_gov_priv_type.pdf", ///
			as(pdf) name("Graph") replace	
	restore

	
* CEO MD and female shares (ownership variable: forprofit)
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hospitals that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		collapse char_md char_female count_ceo_roles* (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make MD line graph over time		
		twoway line char_md year if forprofit == 0, lcolor(blue) ///
		|| line char_md year if forprofit == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share of Hospitals with MD CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.025)0.1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_md_forprofit.pdf", as(pdf) name("Graph") replace
		
		* make gender line graph over time
		twoway line char_female year if forprofit == 0, lcolor(blue) ///
		|| line char_female year if forprofit == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share of Hospitals with Female CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_female_forprofit.pdf", as(pdf) name("Graph") replace
		
		* make line graph for CEO # roles
		twoway ///
			line count_ceo_roles_1 year if forprofit == 0, lcolor("198 219 239") || ///
			line count_ceo_roles_2 year if forprofit == 0, lcolor("107 174 214") || ///
			line count_ceo_roles_3 year if forprofit == 0, lcolor("8 81 156") || ///
			line count_ceo_roles_1 year if forprofit == 1, lcolor("199 233 192") || ///
			line count_ceo_roles_2 year if forprofit == 1, lcolor("116 196 118") || ///
			line count_ceo_roles_3 year if forprofit == 1, lcolor("35 139 69") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" /// 
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_roles1_forprofit.pdf", ///
			as(pdf) name("Graph") replace
	restore
	
* female and MD stats for hosps vs systems
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		tab total_dup
// 		assert total_dup == 2 // 2 hospitals that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* how many are we working with here?
		tab forprofit if entity_type=="IDS/RHA"
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		gen category = "Hospital" if is_hospital == 1
			replace category = "IDS/RHA" if entity_type=="IDS/RHA"
		collapse char_md char_female count_ceo_roles* (rawsum) count, by(year forprofit category)
		drop if missing(forprofit)
		
		* make MD line graph over time		
		twoway line char_md year if forprofit == 0 & category == "Hospital", lcolor("8 81 156") ///
		|| line char_md year if forprofit == 0 & category == "IDS/RHA", lcolor("107 174 214") ///
		|| line char_md year if forprofit == 1 & category == "Hospital", lcolor("35 139 69") ///
		|| line char_md year if forprofit == 1 & category == "IDS/RHA", lcolor("116 196 118") ///
		legend(order(1 "Non-Profit Hospital" 2 "Non-Profit System" 3 "For-Profit Hospital" 4 "For-Profit System")) ///
		title("Share with MD CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_md_forprofit_combsys.pdf", as(pdf) name("Graph") replace
		
		* make gender line graph over time
		twoway line char_female year if forprofit == 0 & category == "Hospital", lcolor("8 81 156") ///
		|| line char_female year if forprofit == 0 & category=="IDS/RHA", lcolor("107 174 214") ///
		|| line char_female year if forprofit == 1 & category == "Hospital", lcolor("35 139 69") ///
		|| line char_female year if forprofit == 1 & category=="IDS/RHA", lcolor("116 196 118") ///
		legend(order(1 "Non-Profit Hospital" 2 "Non-Profit System" 3 "For-Profit Hospital" 4 "For-Profit System")) ///
		title("Share with Female CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_female_forprofit_combsys.pdf", as(pdf) name("Graph") replace
		
	restore
	

	
* Count of CEO jobs using AHAID instead of HIMSS
	* gov_priv_type
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
		
		* make unique by person-AHAnum-year
		bysort contact_uniqueid ahanumber year: keep if _n == 1
		
		* combine fed, state and local into one government category
		replace gov_priv_type = 1 if gov_priv_type==2
	
		* make summary stats by hospital time
		collapse count_ceo_roles_aha* (rawsum) count, by(year gov_priv_type)
		drop if missing(gov_priv_type)
	
		* make line graph for CEO # roles
		twoway ///
			line count_ceo_roles_aha_1 year if gov_priv_type == 4, lcolor("198 219 239") || ///
			line count_ceo_roles_aha_2 year if gov_priv_type == 4, lcolor("107 174 214") || ///
			line count_ceo_roles_aha_3 year if gov_priv_type == 4, lcolor("8 81 156") || ///
			line count_ceo_roles_aha_1 year if gov_priv_type == 3, lcolor("199 233 192") || ///
			line count_ceo_roles_aha_2 year if gov_priv_type == 3, lcolor("116 196 118") || ///
			line count_ceo_roles_aha_3 year if gov_priv_type == 3, lcolor("35 139 69") || ///
			line count_ceo_roles_aha_1 year if gov_priv_type == 1, lcolor("253 208 162") || ///
			line count_ceo_roles_aha_2 year if gov_priv_type == 1, lcolor("253 141 60") || ///
			line count_ceo_roles_aha_3 year if gov_priv_type == 1, lcolor("217 72 16") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles" ///
						 7 "Govt, 1 Role" ///
						 8 "Govt, 2 Roles" ///
						 9 "Govt, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			subtitle("Facility ID variable: AHA Number") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_roles1_gov_priv_type_aha.pdf", ///
			as(pdf) name("Graph") replace
	restore
	* FP/NFP
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
		
		* make unique by person-AHAnum-year
		bysort contact_uniqueid ahanumber year: keep if _n == 1
	
		* make summary stats by hospital time
		collapse count_ceo_roles_aha* (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make line graph for CEO # roles
		twoway ///
			line count_ceo_roles_aha_1 year if forprofit == 0, lcolor("198 219 239") || ///
			line count_ceo_roles_aha_2 year if forprofit == 0, lcolor("107 174 214") || ///
			line count_ceo_roles_aha_3 year if forprofit == 0, lcolor("8 81 156") || ///
			line count_ceo_roles_aha_1 year if forprofit == 1, lcolor("199 233 192") || ///
			line count_ceo_roles_aha_2 year if forprofit == 1, lcolor("116 196 118") || ///
			line count_ceo_roles_aha_3 year if forprofit == 1, lcolor("35 139 69") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			subtitle("Facility ID variable: AHA Number") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_roles1_forprofit_aha.pdf", ///
			as(pdf) name("Graph") replace
	restore
	
* Count of CEOs with other c-suite jobs
	* gov_priv_type
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
		
		* combine fed, state and local into one government category
		replace gov_priv_type = 1 if gov_priv_type==2
	
		* make summary stats by hospital time
		collapse count_csuite_roles_* (rawsum) count, by(year gov_priv_type)
		drop if missing(gov_priv_type)
	
		* make line graph for CEO # roles
		twoway ///
			line count_csuite_roles_1 year if gov_priv_type == 4, lcolor("198 219 239") || ///
			line count_csuite_roles_2 year if gov_priv_type == 4, lcolor("107 174 214") || ///
			line count_csuite_roles_3 year if gov_priv_type == 4, lcolor("8 81 156") || ///
			line count_csuite_roles_1 year if gov_priv_type == 3, lcolor("199 233 192") || ///
			line count_csuite_roles_2 year if gov_priv_type == 3, lcolor("116 196 118") || ///
			line count_csuite_roles_3 year if gov_priv_type == 3, lcolor("35 139 69") || ///
			line count_csuite_roles_1 year if gov_priv_type == 1, lcolor("253 208 162") || ///
			line count_csuite_roles_2 year if gov_priv_type == 1, lcolor("253 141 60") || ///
			line count_csuite_roles_3 year if gov_priv_type == 1, lcolor("217 72 16") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles" ///
						 7 "Govt, 1 Role" ///
						 8 "Govt, 2 Roles" ///
						 9 "Govt, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			subtitle("Includes any C-Suite Role") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_csuite_roles1_gov_priv_type.pdf", ///
			as(pdf) name("Graph") replace
	restore
	* FP/NFP
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		collapse count_csuite_roles* (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make line graph for CEO # roles
		twoway ///
			line count_csuite_roles_1 year if forprofit == 0, lcolor("198 219 239") || ///
			line count_csuite_roles_2 year if forprofit == 0, lcolor("107 174 214") || ///
			line count_csuite_roles_3 year if forprofit == 0, lcolor("8 81 156") || ///
			line count_csuite_roles_1 year if forprofit == 1, lcolor("199 233 192") || ///
			line count_csuite_roles_2 year if forprofit == 1, lcolor("116 196 118") || ///
			line count_csuite_roles_3 year if forprofit == 1, lcolor("35 139 69") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			subtitle("Includes any C-Suite Role") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_csuite_roles1_forprofit.pdf", ///
			as(pdf) name("Graph") replace
	restore

* Count of CEO jobs across systems instead of HIMSS entity
	* gov_priv_type
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
		
		* make unique by person-system-year
		bysort contact_uniqueid parent_system year: keep if _n == 1
		
		* combine fed, state and local into one government category
		replace gov_priv_type = 1 if gov_priv_type==2
	
		* make summary stats by hospital time
		collapse count_system_roles* (rawsum) count, by(year gov_priv_type)
		drop if missing(gov_priv_type)
	
		* make line graph for CEO # roles
		twoway ///
			line count_system_roles_1 year if gov_priv_type == 4, lcolor("198 219 239") || ///
			line count_system_roles_2 year if gov_priv_type == 4, lcolor("107 174 214") || ///
			line count_system_roles_3 year if gov_priv_type == 4, lcolor("8 81 156") || ///
			line count_system_roles_1 year if gov_priv_type == 3, lcolor("199 233 192") || ///
			line count_system_roles_2 year if gov_priv_type == 3, lcolor("116 196 118") || ///
			line count_system_roles_3 year if gov_priv_type == 3, lcolor("35 139 69") || ///
			line count_system_roles_1 year if gov_priv_type == 1, lcolor("253 208 162") || ///
			line count_system_roles_2 year if gov_priv_type == 1, lcolor("253 141 60") || ///
			line count_system_roles_3 year if gov_priv_type == 1, lcolor("217 72 16") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles" ///
						 7 "Govt, 1 Role" ///
						 8 "Govt, 2 Roles" ///
						 9 "Govt, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			subtitle("Facility Systems") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_roles1_gov_priv_type_system.pdf", ///
			as(pdf) name("Graph") replace
	restore
	* FP/NFP
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* make a count variable
		gen count = 1
		
		* make unique by person-AHAnum-year
		bysort contact_uniqueid ahanumber year: keep if _n == 1
	
		* make summary stats by hospital time
		collapse count_system_roles* (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make line graph for CEO # roles
		twoway ///
			line count_system_roles_1 year if forprofit == 0, lcolor("198 219 239") || ///
			line count_system_roles_2 year if forprofit == 0, lcolor("107 174 214") || ///
			line count_system_roles_3 year if forprofit == 0, lcolor("8 81 156") || ///
			line count_system_roles_1 year if forprofit == 1, lcolor("199 233 192") || ///
			line count_system_roles_2 year if forprofit == 1, lcolor("116 196 118") || ///
			line count_system_roles_3 year if forprofit == 1, lcolor("35 139 69") ///
			legend(order(1 "Non-Profit, 1 Role" ///
						 2 "Non-Profit, 2 Roles" ///
						 3 "Non-Profit, 3+ Roles" ///
						 4 "For-Profit, 1 Role" ///
						 5 "For-Profit, 2 Roles" ///
						 6 "For-Profit, 3+ Roles")) ///
			title("Share of Hospitals by Profit Status & CEO's Number of Roles") ///
			subtitle("Facility Systems") ///
			xtitle("Year") ///
			ytitle("Share") ///
			ylabel(0(0.1)1)
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_roles1_forprofit_system.pdf", ///
			as(pdf) name("Graph") replace
	restore	
	
	
* turnover, share of hosps with CEO (ownership variable: gov_priv_type)
	preserve
		
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* keep one observation per hospital-year
		bysort entity_uniqueid year: keep if _n == 1
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		collapse hosp_has_ceo ceo_turnover1 (rawsum) count, by(year gov_priv_type)
		drop if missing(gov_priv_type)
		
		* make "has CEO" line graph over time
		twoway line hosp_has_ceo year if gov_priv_type == 1, lcolor(orange) ///
		|| line hosp_has_ceo year if gov_priv_type == 3, lcolor(green) ///
		|| line hosp_has_ceo year if gov_priv_type == 4, lcolor(blue) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP")) ///
		title("Share of Hospitals With CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0.5(0.1)1) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_has_ceo_gov_priv_type.pdf", as(pdf) name("Graph") replace
		
		* make CEO turnover line graph over time
		merge m:1 year using "${dbdata}/derived/temp/ceoturnover_all"
		twoway line ceo_turnover1 year if gov_priv_type == 1, lcolor(orange) ///
		|| line ceo_turnover1 year if gov_priv_type == 3, lcolor(green) ///
		|| line ceo_turnover1 year if gov_priv_type == 4, lcolor(blue) ///
		|| line ceo_turnover_govpriv year, lcolor(gray) ///
		legend(order(1 "Government" 2 "Private FP" 3 "Private NFP" 4 "Aggregate")) ///
		title("Share of Hospitals With CEO Turnover") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.1)0.6) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_ceoturnover_gov_priv_type.pdf", as(pdf) name("Graph") replace
	
	restore	
	
* turnover, share of hosps with CEO (ownership variable: forprofit)
	preserve
	
		* keep only hospital observations
		keep if is_hospital ==1 
	
		* keep one observation per hospital-year
		bysort entity_uniqueid year: keep if _n == 1
		
		* make a count variable
		gen count = 1
	
		* make summary stats by hospital time
		collapse hosp_has_ceo ceo_turnover1 (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make "has CEO" line graph over time
		twoway line hosp_has_ceo year if forprofit == 0, lcolor(blue) ///
		|| line hosp_has_ceo year if forprofit == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share of Hospitals With CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0.5(0.1)1) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_has_ceo_forprofit.pdf", as(pdf) name("Graph") replace
		
		* make CEO turnover line graph over time
		merge m:1 year using "${dbdata}/derived/temp/ceoturnover_all"
		twoway line ceo_turnover1 year if forprofit == 0, lcolor(blue) ///
		|| line ceo_turnover1 year if forprofit == 1, lcolor(green) ///
		|| line ceo_turnover_forprofit year, lcolor(gray) ///
		legend(order(1 "Non-Profit" 2 "For-Profit" 3 "Aggregate")) ///
		title("Share of Hospitals With CEO Turnover") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_hosp_ceoturnover_forprofit.pdf", as(pdf) name("Graph") replace
	
	restore
	
* share of CEO turnover episodes where one member of the c-suite comes with them
* using c_suite flag
* identify CEOs that switch jobs
	preserve
		restrict_hosp_sample
		keep if char_ceo ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
// 		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* for each CEO, get the observation before turnover. Add the destination entity. 
		bysort entity_uniqueid (year): gen pre_turnover = ceo_turnover1[_n+1]
		gen turnover_year = year if ceo_turnover1 == 1
		
		tempfile orig
		save `orig'
		
		* find destination(s)
		keep if ceo_turnover1 == 1
		keep added_hosp_ceo_entities contact_uniqueid year char_female
		replace year = year-1
		rename added_hosp_ceo_entities ceo_destination1
		rename char_female ceo_char_female
		* dedup
		bysort contact_uniqueid year: keep if _n ==1
		tempfile ceodestinations
		save `ceodestinations'
		
		use `orig', clear
		merge m:1 contact_uniqueid year using `ceodestinations', keep(1 3) gen(_merge_destination)
		
		* descriptives
		* how many CEOs have turnover? 
		tab ceo_turnover1 // we have already restricted to CEOs
		tab pre_turnover
		* how many have a destination as a hospital CEO? 
		tab _merge_destination if pre_turnover == 1
		count if pre_turnover == 1 & !missing(ceo_destination1)
		
		* cases when CEO leaves and then we see them as CEO at another hospital:
			* pre_turnover == 1
			* ceo_destination1 is not missing
		
		keep pre_turnover entity_uniqueid year contact_uniqueid turnover_year ceo_destination1 ceo_char_female
		
		keep if pre_turnover == 1
		rename contact_uniqueid ceoturnover_contact_uniqueid  
		
		tempfile pre_turnover_data
		save `pre_turnover_data'
		
	restore
	*preserve
		restrict_hosp_sample
		
		merge m:1 entity_uniqueid year using `pre_turnover_data', keep(1 3) nogen
		
		* identify members of c-suite before the CEO switches jobs
		gen pre_turnover_csuite = (c_suite == 1) & (pre_turnover == 1) & char_ceo == 0
		bysort contact_uniqueid: egen ever_pre_turnover_csuite = max(pre_turnover_csuite)
		* each turnover event will have an observation with the turnover year, ceo ID, entity ID, and a list of the whole csuite IDs
		* get list of their entities next year and year after
		gen year1 = ceo_turnover1 == 1
		
		tempfile orig2
		save `orig2'
		
		* get a list of all places each person works in each year
		keep if ever_pre_turnover_csuite == 1
		keep contact_uniqueid year entity_uniqueid 
		bysort contact_uniqueid year entity_uniqueid: keep if _n == 1
		bysort contact_uniqueid year: gen num = _n
		rename entity_uniqueid entity_uniqueid_num
		reshape wide entity_uniqueid_num, i(contact_uniqueid year) j(num)
		gen year_prev = year-1 // so that I can merge next year's entities onto this year's
			* these are the next-year obs
		
		tempfile rolesbyyear
		save `rolesbyyear'
		
		* merge roles by year with main data on prior year
		use `orig2', clear
		gen year_prev = year
		merge m:1 contact_uniqueid year_prev using `rolesbyyear', gen(_merge_roles) keep(1 3)
		
		* Check if destination matches
		gen dest_match1 = 0
		* Loop over all entity_uniqueid* variables
		foreach v of varlist entity_uniqueid_num* {
			replace dest_match1 = 1 if dest_match1 == 0 & strpos(ceo_destination1, string(`v'))>0
		}
		
		* DESCRIPTIVES
		tab pre_turnover if char_ceo == 1 // how many CEOs turn over?
		tab pre_turnover
		tab pre_turnover if !missing(ceo_destination1)
		tab dest_match1 if !missing(ceo_destination1) & pre_turnover == 1
		gen missing_ceo_dest = missing(ceo_destination1)
		
		* collapse to each turnover episode (can add descriptives)
		keep if pre_turnover_csuite == 1
		collapse pre_turnover dest_match1 missing_ceo_dest ceo_char_female forprofit, by(entity_uniqueid year)
		tab dest_match1 if missing_ceo_dest == 0
		gen matches = dest_match1 > 0 if !missing(dest_match1)
		tab ceo_char_female if missing_ceo_dest == 0, sum(matches)
		tab forprofit if missing_ceo_dest == 0, sum(matches)
		
	
	restore
	
* share of hospital SYSTEMS with CEO
	preserve
	
		* keep only hospital observations
		keep if entity_type =="IDS/RHA"
	
		* keep one observation per hospital-year
		bysort entity_uniqueid year: keep if _n == 1
		
		* make a count variable
		gen count = 1
		
		* make summary stats by hospital time
		collapse hosp_has_ceo ceo_turnover1 (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make "has CEO" line graph over time
		twoway line hosp_has_ceo year if forprofit == 0, lcolor(blue) ///
		|| line hosp_has_ceo year if forprofit == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share of Hospital Systems With CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0.5(0.1)1) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_sys_has_ceo_forprofit.pdf", as(pdf) name("Graph") replace
		
		* make CEO turnover line graph over time
		merge m:1 year using "${dbdata}/derived/temp/ceoturnover_all"
		twoway line ceo_turnover1 year if forprofit == 0, lcolor(blue) ///
		|| line ceo_turnover1 year if forprofit == 1, lcolor(green) /// || line ceo_turnover_forprofit year, lcolor(gray) ///
		legend(order(1 "Non-Profit" 2 "For-Profit" /* 3 "Aggregate"*/)) ///
		title("Share of Hospital Systems With CEO Turnover") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0(0.05)0.35) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_sys_ceoturnover_forprofit.pdf", as(pdf) name("Graph") replace
	
	restore
	
* share with CEO at either hosp or system 
	preserve
	
		* keep only hospital observations
		keep if is_hospital==1
	
		* make a system CEO variable
		gen comb_ceo_prelim = 1 if !missing(contact_uniqueid_parentceo)
		
		* max by year
		bysort entity_uniqueid year: egen sys_ceo = max(comb_ceo_prelim)
	
		* keep one observation per hospital-year
		bysort entity_uniqueid year: keep if _n == 1
		
		* make combined variable
		gen comb_ceo = hosp_has_ceo
		replace comb_ceo = 1 if sys_ceo == 1
		
		* make a count variable
		gen count = 1
		
		* make summary stats by hospital time
		collapse comb_ceo ceo_turnover1 (rawsum) count, by(year forprofit)
		drop if missing(forprofit)
		
		* make "has CEO" line graph over time
		twoway line comb_ceo year if forprofit == 0, lcolor(blue) ///
		|| line comb_ceo year if forprofit == 1, lcolor(green) ///
		legend(order(1 "Non-Profit" 2 "For-Profit")) ///
		title("Share of Hospitals With Facility or System CEO") ///
		xtitle("Year") ytitle("Share") ///
		ylabel(0.5(0.1)1) 
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_comb_has_ceo_forprofit.pdf", as(pdf) name("Graph") replace
	
	restore
	
* episode length descriptives by FP/NFP
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 

		* summarize episode_length
		bysort entity_uniqueid episode_num: keep if _n == 1
		gen count = 1
		collapse episode_length (rawsum) count, by(forprofit episode_type)
		keep if !missing(forprofit)
		
		label define profit_lbl 0 "Non-Profit" 1 "For-Profit"
		label values forprofit profit_lbl
		
		graph bar episode_length, over(episode_type) over(forprofit) ///
			ytitle("Mean Episode Length (Years)") ///
			title("Episode Length by Profit Status and Episode Type") ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_avgepisodelength_forprofit.pdf", as(pdf) name("Graph") replace
			
		graph bar count, over(episode_type) over(forprofit) ///
			ytitle("Count of Episodes") ///
			title("Count of Episodes by Profit Status and Episode Type") ///
			blabel(bar, format(%3.0f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_countepisodetype_forprofit.pdf", as(pdf) name("Graph") replace
		
	restore
	
* episode length descriptives by CEO gender
	preserve
		* keep CEOs only
		keep if char_ceo == 1
		* keep only hospital observations
		keep if is_hospital ==1 
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 

		* summarize episode_length
		bysort entity_uniqueid episode_num: keep if _n == 1
		gen count = 1
		collapse episode_length (rawsum) count, by(char_female episode_type)
		keep if !missing(char_female)
				
		graph bar episode_length, over(episode_type) over(char_female) ///
			ytitle("Mean Episode Length (Years)") ///
			title("Episode Length by CEO Gender and Episode Type") ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_avgepisodelength_female.pdf", as(pdf) name("Graph") replace
			
		graph bar count, over(episode_type) over(char_female) ///
			ytitle("Count of Episodes") ///
			title("Count of Episodes by CEO Gender and Episode Type") ///
			blabel(bar, format(%3.0f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_countepisodetype_female.pdf", as(pdf) name("Graph") replace
		
	restore
	
* do career transition types vary over time?
preserve 
	tab hosp_transition_type if is_hospital ==1, gen(ct)
	collapse (sum) ct*, by(year)
	drop if year <= 2009
	
	twoway                                                         ///
    (line ct1 year, lp(solid)     lw(medthick))                ///
    (line ct2 year, lp(dash)      lw(medthick))                ///
    (line ct3 year, lp(dot)       lw(medthick))                ///
    (line ct4 year, lp(shortdash) lw(medthick))                ///
    (line ct5 year, lp(longdash)  lw(medthick)),              	///
    legend(order(1 "External move" 2 "External promotion"        ///
                 3 "Internal move" 4 "Internal promotion"        ///
                 5 "Lateral move") cols(1) position(6))         ///
    ytitle("Count of transitions")                              ///
    xtitle("Year")                                              ///
    title("Career Transition Distribution by Year")             ///
    graphregion(color(white))
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_byyear_hosp.pdf", as(pdf) name("Graph") replace	
restore
preserve 
	tab sys_transition_type if entity_type == "IDS/RHA", gen(ct)
	collapse (sum) ct*, by(year)
	drop if year <= 2009
	
	twoway                                                         ///
    (line ct1 year, lp(solid)     lw(medthick))                ///
    (line ct2 year, lp(dash)      lw(medthick))                ///
    (line ct3 year, lp(dot)       lw(medthick))                ///
    (line ct4 year, lp(shortdash) lw(medthick))                ///
    (line ct5 year, lp(longdash)  lw(medthick)),              	///
    legend(order(1 "External move" 2 "External promotion"        ///
                 3 "Internal move" 4 "Internal promotion"        ///
                 5 "Lateral move") cols(1) position(6))         ///
    ytitle("Count of transitions")                              ///
    xtitle("Year")                                              ///
    title("Career Transition Distribution by Year")             ///
    graphregion(color(white))
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_byyear_sys.pdf", as(pdf) name("Graph") replace	
restore

* break out by within/across systems
preserve
gen within_system = regexm(hosp_transition_type_det,"within system")
replace within_system = . if !regexm(hosp_transition_type_det,"system")

label define withacrsys 0 "Across Systems" 1 "Within System"
label values within_system withacrsys

	tab hosp_transition_type if is_hospital ==1, gen(ct)
	collapse (sum) ct*, by(within_system)
	
	graph bar ct*, asyvars stack over(within_system) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distributions Within vs Across Systems")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_systemwithinacross.pdf", as(pdf) name("Graph") replace
restore

* do diff ownership types have different career transition types?
preserve
	tab hosp_transition_type if is_hospital ==1, gen(ct)
	collapse (sum) ct*, by(forprofit)
	
	graph bar ct*, asyvars stack over(forprofit) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distribution by Hospital Ownership")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_forprofit_hosp.pdf", as(pdf) name("Graph") replace
restore
preserve
	tab sys_transition_type if entity_type == "IDS/RHA", gen(ct)
	collapse (sum) ct*, by(forprofit)
	
	graph bar ct*, asyvars stack over(forprofit) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distribution by Hospital Ownership")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_forprofit_sys.pdf", as(pdf) name("Graph") replace
restore

* career transition types by profit AND gender
preserve
	tab hosp_transition_type if is_hospital ==1, gen(ct)
	collapse (sum) ct*, by(forprofit char_female)
	
	graph bar ct* if char_female == 0, asyvars stack over(forprofit) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distribution by Hospital Ownership: Men Only")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_forprofit_men.pdf", as(pdf) name("Graph") replace
	
	graph bar ct* if char_female == 1, asyvars stack over(forprofit) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distribution by Hospital Ownership: Women Only")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_forprofit_women.pdf", as(pdf) name("Graph")replace
restore

* do female and male CEOs have different career transition types?
preserve
	tab hosp_transition_type if is_hospital ==1, gen(ct)
	collapse (sum) ct*, by(char_female)
	
	graph bar ct*, asyvars stack over(char_female) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distribution by CEO Gender")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_bygender.pdf", as(pdf) name("Graph") replace
restore

* do MD and non-MD CEOs have different career transition types?
preserve
	tab hosp_transition_type if is_hospital ==1, gen(ct)
	collapse (sum) ct*, by(char_md)
	
	graph bar ct*, asyvars stack over(char_md) percent  		///
		legend(order(1 "External move" 2 "External promotion"       ///
                 3 "Internal move" 4 "Internal promotion"          	///
                 5 "Lateral move") cols(1) position(6))           	///
		ytitle("Share of transitions")                              ///
		title("Career Transition Distribution by CEO MD Status")
	graph export "${overleaf}/notes/CEO Descriptives/figures/descr_transitions_bymd.pdf", as(pdf) name("Graph") replace
restore

* are men and women CEOs of different size hospitals? 
	cap destring hospbd, replace
	cap destring bdtot, replace
	* these seem pretty much the same
* bdtot
preserve
	* keep CEOs only
		keep if char_ceo == 1
		keep if is_hospital == 1
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 facilities that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
	* collapse 	
		collapse bdtot, by(char_female forprofit)
		graph bar bdtot, over(char_female) over(forprofit) ///
			ytitle("Count") ///
			title("Mean Bed Count by CEO Gender and Ownership") ///
			subtitle("AHA Variable") ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_bdtot_female_forprofit.pdf", as(pdf) name("Graph") replace
restore
* entity_bedsize
preserve
	* keep CEOs only
		keep if char_ceo == 1
		keep if is_hospital == 1
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 facilities that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
	* collapse 	
		collapse entity_bedsize, by(char_female forprofit)
		graph bar entity_bedsize, over(char_female) over(forprofit) ///
			ytitle("Count") ///
			title("Mean Bed Count by CEO Gender and Ownership") ///
			subtitle("HIMSS Variable") ///
			blabel(bar, format(%3.2f))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_entity_bedsize_female_forprofit.pdf", as(pdf) name("Graph") replace
restore

* TO-DO: redo first taking the sum by contact_uniqueid and then collapsing
	* take sum of each kind of ownership within a contact_uniqueid
	* graph over gender stacked bars of each 

* how many places are people CEOs at in a given year?
preserve
		* keep CEOs only
		keep if char_ceo == 1
		keep if is_hospital ==1
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 facilities that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
		* how many in any year
		gen ct_fac_yr = 1
		collapse (rawsum) ct_fac_yr (max) char_md, by(contact_uniqueid year char_female)
		
		sum ct_fac_yr
		tab char_md, sum(ct_fac_yr )
		tab char_female, sum(ct_fac_yr)
		
restore 

* ceo shares by ownership and gender and year
preserve
	* keep CEOs only
		keep if char_ceo == 1
		restrict_hosp_sample
		
		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		assert total_dup == 2 // 2 hosps that end up with two CEOs in the same year
		drop if duplicates == 1
		drop total_dup duplicates 
		
	* what share of CEOs at each type?
		sum forprofit 
		gen nonprofit_ps = 1- forprofit
		
	* find the hospitals that joined the sample in 2012
		bysort entity_uniqueid: egen firstyearinsample = min(year)
		bysort entity_uniqueid: egen lastyearinsample = max(year)
		
		tab forprofit if firstyearinsample==2012, m 
		
		* save to come back to old data
		tempfile before
		save `before'
		
		* collapse list of new entities
		keep entity_uniqueid entity_name year firstyearinsample entity_name_parent entity_uniqueid_parent forprofit 
		bysort entity_uniqueid year: keep if _n ==1
		keep if firstyearinsample == 2012
		save "${dbdata}/derived/temp/newfacilities_2012.dta", replace
		* this might be a slight overcount since they need to have a CEO to be included in this list, so maybe if they added their first CEO in 2012 then they would be here. But mostly these are facilities that are actually not in the individual file until 2012 (I checked separately in that file, but didn't use it since it doesn't have the parent info merged in as cleanly)
		
	* share of CEOs of each type
	use `before', clear
	gen count =1 
	collapse forprofit (rawsum) count, by(year char_female)
	
	drop if year <= 2009
	
	* graph bar over gender/year
	twoway ///
    (line forprofit year if char_female == 0, lcolor(blue) lpattern(solid)) ///
    (line forprofit year if char_female == 1, lcolor(red) lpattern(dash)) ///
    , ///
    ytitle("Share") ///
    title("Share of CEOs at For-Profit Facilities by Gender") ///
    legend(order(1 "Male" 2 "Female"))
		graph export "${overleaf}/notes/CEO Descriptives/figures/descr_ceo_forprofit_bygender.pdf", as(pdf) name("Graph") replace
	
restore
	
* different CEO ecosystems? ____________________________________________________ 

program define ceo_ecosystem

	syntax, file_prefix(str) vsn(str)
	
	preserve
	
		* sample restrictions as necessary
		if "`vsn'" == "base" {
			* restrict sample 
				restrict_hosp_sample
			* keep CEOs only
				keep if char_ceo == 1
		}
		else if "`vsn'" == "ceo_2017" {
			* identify CEOs in 2017
			restrict_hosp_sample
			gen is_ceo_2017 = year == 2017 & char_ceo == 1
			bysort contact_uniqueid: egen was_ceo_2017 = max(is_ceo_2017)
			keep if was_ceo_2017
			drop is_ceo_2017 was_ceo_2017
			
			keep if year >= 2013
			
			* keep CEOs only
			keep if char_ceo == 1
		}
		else if "`vsn'" == "ceo_2017_switch" {
			* identify CEOs in 2017
			restrict_hosp_sample
			gen is_ceo_2017 = year == 2017 & char_ceo == 1
			bysort contact_uniqueid: egen was_ceo_2017 = max(is_ceo_2017)
			keep if was_ceo_2017
			drop is_ceo_2017 was_ceo_2017
			
			keep if year >= 2013
			
			* keep CEOs only
			keep if char_ceo == 1
			
			* keep only CEOs who worked at multiple hospitals in the sample period and add at least one new one
				* multiple hosps
			bysort contact_uniqueid entity_uniqueid: gen count_ceo_fac = 1 if _n == 1
			bysort contact_uniqueid: egen tot_ceo_fac = total(count_ceo_fac)
				* added hosps
			gen added_hosp = !missing(added_hosp_ceo_entities)
			bysort contact_uniqueid: egen ever_added_hosp = max(added_hosp)
			keep if tot_ceo_fac > 1 & added_hosp ==1 
			* could do by tagging the first obs of an entity_uniqueid for the CEOs list that isn't in 2013
			
			
		}

		* make sure unique
		bysort contact_uniqueid entity_uniqueid year: gen duplicates = 1 if _n > 1
		egen total_dup = total(duplicates)
		tab total_dup
		drop if duplicates == 1
		drop total_dup duplicates 
			
		* make CEO fp/nfp variables
		gen ceo_nfp = char_ceo == 1 & forprofit == 0
			replace ceo_nfp = . if missing(forprofit)
		gen ceo_forprofit = char_ceo == 1 & forprofit == 1
			replace ceo_forprofit = . if missing(forprofit)
		
		* make EVER variables
		bysort contact_uniqueid: egen ever_ceo_nfp = max(ceo_nfp)
		bysort contact_uniqueid: egen ever_ceo_forprofit = max(ceo_forprofit)
		
		* make years variable
		gen roles = 1
		
		collapse *ceo_nfp *ceo_forprofit (rawsum) roles (max) char_md, by(contact_uniqueid char_female)
		* TEMP: keep most common
		bysort contact_uniqueid (roles): keep if _n == _N
		
		sum ever_ceo_forprofit if ever_ceo_nfp == 1
		sum ever_ceo_nfp if ever_ceo_forprofit ==1
		
		sum ever_ceo_forprofit if ever_ceo_nfp == 1 & char_female ==1
		sum ever_ceo_forprofit if ever_ceo_nfp == 1 & char_female ==0
		sum ever_ceo_nfp if ever_ceo_forprofit ==1 & char_female ==1
		sum ever_ceo_nfp if ever_ceo_forprofit ==1 & char_female ==0
		
		* make variables for graphing
		gen only_nonprofit_ceo = ever_ceo_forprofit == 0 & ever_ceo_nfp == 1
			replace only_nonprofit_ceo = . if missing(ever_ceo_forprofit) | missing(ever_ceo_nfp) 
		gen only_forprofit_ceo = ever_ceo_forprofit == 1 & ever_ceo_nfp == 0
			replace only_forprofit_ceo = . if missing(ever_ceo_forprofit) | missing(ever_ceo_nfp) 
		gen ever_both_ceo = ever_ceo_forprofit == 1 & ever_ceo_nfp == 1
			replace ever_both_ceo = . if missing(ever_ceo_forprofit) | missing(ever_ceo_nfp) 
			
		tempfile base
		save `base'
		
		* ALL
		collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_nfp
		gen type = "NFP"
		tempfile nfp
		save `nfp'
		
		use `base', clear
		collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_forprofit
		gen type = "For-Profit"
		tempfile forprofit 
		save `forprofit'

		use `base', clear
		collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo
		gen type = "All"
		append using `nfp'
		append using `forprofit'
		
		graph bar only_nonprofit_ceo only_forprofit_ceo ever_both_ceo, over(type) stack ///
			ytitle("Share") ///
			title("Has the CEO Ever Worked in Each Ownership Type?") ///
			legend(order(1 "Only Non-Profit" 2 "Only For-Profit" 3 "Both")) ///
			blabel(bar, format(%3.2f))
			graph export "${overleaf}/notes/CEO Descriptives/figures/`file_prefix'.pdf", as(pdf) name("Graph") replace
		
		* BY GENDER
		use `base', clear
		collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_nfp, by(char_female)
		gen type = "NFP"
		tempfile nfp_female
		save `nfp_female'
		
		use `base', clear
		collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_forprofit, by(char_female)
		gen type = "For-Profit"
		tempfile forprofit_female 
		save `forprofit_female'

		use `base', clear
		collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo, by(char_female)
		gen type = "All"
		append using `nfp_female'
		append using `forprofit_female'
		
		graph bar only_nonprofit_ceo only_forprofit_ceo ever_both_ceo, over(char_female) over(type) stack ///
			ytitle("Share") ///
			title("Has the CEO Ever Worked in Each Ownership Type, by Gender") ///
			legend(order(1 "Only Non-Profit" 2 "Only For-Profit" 3 "Both")) ///
			blabel(bar, format(%3.2f))
			graph export "${overleaf}/notes/CEO Descriptives/figures/`file_prefix'_bygender.pdf", as(pdf) name("Graph") replace
			
		* BY MD
		use `base', clear
		collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_nfp, by(char_md)
		gen type = "NFP"
		tempfile nfp_md
		save `nfp_md'
		
		use `base', clear
		collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo if ever_ceo_forprofit, by(char_md)
		gen type = "For-Profit"
		tempfile forprofit_md 
		save `forprofit_md'

		use `base', clear
		collapse only_nonprofit_ceo only_forprofit_ceo ever_both_ceo, by(char_md)
		gen type = "All"
		append using `nfp_md'
		append using `forprofit_md'
		
		cap label define degree_md 0 "No MD" 1 "Has MD"
		cap label values char_md degree_md
		
		graph bar only_nonprofit_ceo only_forprofit_ceo ever_both_ceo, over(char_md) over(type) stack ///
			ytitle("Share") ///
			title("Has the CEO Ever Worked in Each Ownership Type, by CEO MD") ///
			legend(order(1 "Only Non-Profit" 2 "Only For-Profit" 3 "Both")) ///
			blabel(bar, format(%3.2f))
			graph export "${overleaf}/notes/CEO Descriptives/figures/`file_prefix'_bymd.pdf", as(pdf) name("Graph") replace
		
	restore
end

* run CEO ecosystem descriptives 
	* for all CEOs
	ceo_ecosystem, vsn(base) file_prefix(descr_everceo_forprofit)
	
	* for all people who were CEOs in 2017, 5-year lookback
	ceo_ecosystem, vsn(ceo_2017) file_prefix(descr_ceo_2017_forprofit)
		
	* for all people who were CEOs in 2017, 5-year lookback, who switched CEO roles
		* look at people who have been CEOs at two or more facilities in the window
		* optionally can look at people who also have added a CEO job in the window using added_hosp_ceo_entities
	ceo_ecosystem, vsn(ceo_2017_switch) file_prefix(descr_ceo_2017_switch_forprofit)
	

* TESTING HOSP ECOSYSTEM SUFF. STAT
	* get the shares of FP/NFP for each year
	preserve
		restrict_hosp_sample
		keep if char_ceo == 1
		
		* number of HRRs by year
		bysort contact_uniqueid year hrrnum: gen dup = _n == 1
		by contact_uniqueid year: egen n_unique_hrr = total(dup)
 		gen multiple_hrr = n_unique_hrr > 1

		* Bed count: above or below median 
		destring bdtot, replace
		bysort year: egen med_bed = median(bdtot)
		gen bed_high = bdtot > med_bed
		
		* System owned 
		gen sys_owned = entity_type_parent == "IDS/RHA" // no missing values
		
		* collapse 
		collapse mean_forprofit=forprofit mean_sys_owned=sys_owned mean_bed_high=bed_high mean_multiple_hrr=multiple_hrr, by(year)
		tempfile annual_profitshares
		save `annual_profitshares'
	restore
	*preserve
		restrict_hosp_sample
		* keep CEOs only
		keep if char_ceo == 1
		
		* keep only people who were CEOs in 2017
		gen is_ceo_2017 = year == 2017 & char_ceo == 1
		bysort contact_uniqueid: egen was_ceo_2017 = max(is_ceo_2017)
		keep if was_ceo_2017
		drop is_ceo_2017 was_ceo_2017
		
		* number of HRRs by year
		bysort contact_uniqueid year hrrnum: gen dup = _n == 1
		by contact_uniqueid year: egen n_unique_hrr = total(dup)
 		gen multiple_hrr = n_unique_hrr > 1

		* Bed count: above or below median 
		destring bdtot, replace
		bysort year: egen med_bed = median(bdtot)
		gen bed_high = bdtot > med_bed
		
		* system ownership variable
		gen sys_owned = entity_type_parent == "IDS/RHA"
		tab sys_owned
		
		* 5 year lookback + 1 for lag
		keep if year >= 2012
		
		* identify added observations
		* could do by tagging the first obs of an entity_uniqueid for the CEOs list that isn't in 2013
		bysort contact_uniqueid entity_uniqueid (year): gen added_ceo = (_n == 1 & year > 2012)
		
		* 5 year lookback
		keep if year >= 2013
		
		* keep only CEOs who worked at multiple hospitals in the sample period and add at least one new one
			* multiple hosps
		bysort contact_uniqueid entity_uniqueid: gen count_ceo_fac = 1 if _n == 1
		bysort contact_uniqueid: egen tot_ceo_fac = total(count_ceo_fac)
			* added hosps
		gen added_hosp = !missing(added_hosp_ceo_entities)
		bysort contact_uniqueid: egen ever_added_hosp = max(added_hosp)
		keep if tot_ceo_fac > 1 & added_hosp ==1 
		
		* pull in annual profit shares and system ownership
		merge m:1 year using `annual_profitshares', keep(1 3) nogen
		
		* collapse to n moves and average profit shares 
		collapse forprofit sys_owned mean_forprofit mean_sys_owned bed_high mean_bed_high multiple_hrr mean_multiple_hrr (rawsum) added_ceo, by(contact_uniqueid)
		
		* make indicator for having worked in both
		gen both_fp = (forprofit > 0 & forprofit < 1) if !missing(forprofit)
		gen both_sys = (sys_owned > 0 & sys_owned <1) if !missing(sys_owned)
		gen both_bed = (bed_high > 0 & bed_high < 1) if !missing(bed_high)
		gen both_hrr = (multiple_hrr > 0) if !missing(multiple_hrr)
		
		sum both_fp
		sum both_sys 
		
		* gen simulated outcome 
		egen all_N = total(added_ceo)
		gen weight = added_ceo / all_N
		
		* = (share currently in FP) * (1 - prob. their previous hospitals were all FP) + (share currently in NFP) * (1 - prob. their previous hospitals were all NFP)
		gen churn_fp = mean_forprofit * (1 - mean_forprofit^(added_ceo)) + (1-mean_forprofit)*(1 - (1-mean_forprofit)^(added_ceo))
		gen weighted_churn_fp = churn_fp*weight
		sum churn_fp [iw=added_ceo]
		
		* for systems
		gen churn_sys = mean_sys_owned * (1 - mean_sys_owned^(added_ceo)) + (1-mean_sys_owned)*(1 - (1-mean_sys_owned)^(added_ceo))
		gen weighted_churn_sys = churn_sys*weight
		sum churn_sys [iw=added_ceo]
		
		* for beds
		gen churn_bed = mean_bed_high * (1 - mean_bed_high^(added_ceo)) + (1-mean_bed_high)*(1 - (1-mean_bed_high)^(added_ceo))
		gen weighted_churn_bed = churn_bed*weight
		sum churn_bed [iw=added_ceo]
		
		* for multiple HRRs
		gen churn_hrr = mean_multiple_hrr * (1 - mean_multiple_hrr^(added_ceo)) + (1-mean_multiple_hrr)*(1 - (1-mean_multiple_hrr)^(added_ceo))
		gen weighted_churn_hrr = churn_hrr*weight
		sum churn_hrr [iw=added_ceo]
		
		* collapse
		collapse both_fp both_sys both_bed both_hrr (rawsum) weighted_churn_fp weighted_churn_sys weighted_churn_bed weighted_churn_hrr
		
		label var both_fp "Observed"
		label var weighted_churn_fp "Random Churn"
		
		xpose , clear varname
		gen category = "Observed" if _varname == "both_fp"
		replace category = "Random Churn" if _varname == "weighted_churn_fp"
		
		graph bar v1 if !missing(category), over(category) title("Share of CEOs with Experience at FP and NFP") subtitle("2017 5-year lookback") blabel(bar) ytitle("Percent of 2017 CEOs")
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceomarkets_fp.pdf", as(pdf) name("Graph") replace
		
		replace category = ""
		replace category = "Observed" if _varname == "both_sys"
		replace category = "Random Churn" if _varname == "weighted_churn_sys"
		
		graph bar v1 if !missing(category), over(category) title("% CEOs with Experience at System-Owned & Independent Hosps") subtitle("2017 5-year lookback") blabel(bar) ytitle("Percent of 2017 CEOs")
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceomarkets_sys.pdf", as(pdf) name("Graph") replace
		
		replace category = ""
		replace category = "Observed" if _varname == "both_bed"
		replace category = "Random Churn" if _varname == "weighted_churn_bed"
		
		graph bar v1 if !missing(category), over(category) title("% CEOs with Experience at Large and Small Hosps") subtitle("2017 5-year lookback") blabel(bar) ytitle("Percent of 2017 CEOs")
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceomarkets_bed.pdf", as(pdf) name("Graph") replace

		replace category = ""
		replace category = "Observed" if _varname == "both_hrr"
		replace category = "Random Churn" if _varname == "weighted_churn_hrr"
		
		graph bar v1 if !missing(category), over(category) title("% CEOs with Experience at Multiple HRRs") subtitle("2017 5-year lookback") blabel(bar) ytitle("Percent of 2017 CEOs")
		graph export "${overleaf}/notes/CEO Descriptives/figures/ceomarkets_hrr.pdf", as(pdf) name("Graph") replace

		
	restore
	