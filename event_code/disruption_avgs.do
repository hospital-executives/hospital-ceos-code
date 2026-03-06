/* EVENT_STUDY_STATS *************************************************************

Program name: 	non_ceo_turnover.do
Programmer: 	Katherine Papen

Goal: 			Generate event study plots for non-CEO turnover measures

*******************************************************************************/

* check setup
	check_setup

*----------------------------------------------------------
* Load data and restrict to correct sample
*----------------------------------------------------------
	use "${dbdata}/derived/temp/merged_ma_sysid_xwalk.dta", clear
	
* merge in type 
preserve
	use "${dbdata}/derived/temp/merged_ma_nonharmonized.dta", clear
	keep entity_uniqueid year type
	
	tempfile himss_type_xwalk
	save `himss_type_xwalk'
restore

merge 1:1 entity_uniqueid year using `himss_type_xwalk', assert(master match) keep(match) nogen

* restrict to hospital sample
restrict_hosp_sample

* will need to revisit for the cases where we're dropping observations still
merge 1:1 entity_uniqueid year using "${dbdata}/derived/positions_by_tier.dta", keep(match) nogen

make_target_sample

* ── 0. Define title groups ───────────────────────────────────────────────────
local t1b ceo cfo coo
local t1c medical_staff_chief chief_nursing_head
local t1i cio chief_compliance_officer csio_it_security_officer
local t2b business_office_head marketing_head purchasing_head patient_accounting
local t2c quality_head ob_head cardiology_head er_director or_head ///
          ambulatory_care_head patient_safety_head pathology_chief ///
          laboratory_director pharmacy_head radiology_med_dir
local t2i it_director hr_head him_director facility_management_head ///
          director_of_technology clinical_systems_director

local all_titles  `t1b' `t1c' `t1i' `t2b' `t2c' `t2i'
local business    `t1b' `t2b'   // tier 1 + tier 2 business
local clinical    `t1c' `t2c'   // tier 1 + tier 2 clinical
local admin       `t1i' `t2i'   // tier 1 + tier 2 IT/legal/HR
local upper       `t1b' `t1c' `t1i'   // tier 1 only
local middle      `t2b' `t2c' `t2i'   // tier 2 only

* ── 1. xtset (entity_uniqueid must be numeric) ───────────────────────────────
* if entity_uniqueid is a string, run: egen id = group(entity_uniqueid)
xtset entity_uniqueid tar_reltime

* ── 2. Generate per-role elimination indicator ───────────────────────────────
* eliminated: role existed (dne==0) last period → DNE (dne==1) this period
foreach title of local all_titles {
    gen elim_`title' = (L.`title'_dne == 0) & (`title'_dne == 1)
    replace elim_`title' = . if missing(L.`title'_dne) | missing(`title'_dne)
}

* ── 3. Compute group-level rates and indicators ──────────────────────────────
foreach grp in overall business clinical admin upper middle {

    if "`grp'" == "overall"   local grp_titles `all_titles'
    if "`grp'" == "business"  local grp_titles `business'
    if "`grp'" == "clinical"  local grp_titles `clinical'
    if "`grp'" == "admin"     local grp_titles `admin'
    if "`grp'" == "upper"     local grp_titles `upper'
    if "`grp'" == "middle"    local grp_titles `middle'

    * running sum and non-missing count across titles in group
    gen _elim_sum = 0
    gen _elim_n   = 0
    foreach title of local grp_titles {
        replace _elim_sum = _elim_sum + elim_`title'  if !missing(elim_`title')
        replace _elim_n   = _elim_n   + 1             if !missing(elim_`title')
    }

    * rate: share of roles eliminated (missing if no non-missing roles)
    gen elim_rate_`grp' = _elim_sum / _elim_n if _elim_n > 0

    * indicator: any role eliminated
    gen elim_any_`grp'  = (_elim_sum > 0)     if _elim_n > 0

    drop _elim_sum _elim_n
}

* ── 4. Label variables ───────────────────────────────────────────────────────
label var elim_rate_overall   "Elim rate: all roles"
label var elim_rate_business  "Elim rate: business (T1+T2)"
label var elim_rate_clinical  "Elim rate: clinical (T1+T2)"
label var elim_rate_admin     "Elim rate: IT/legal/HR (T1+T2)"
label var elim_rate_upper     "Elim rate: upper mgmt (T1)"
label var elim_rate_middle    "Elim rate: middle mgmt (T2)"

label var elim_any_overall    "Any elim: all roles"
label var elim_any_business   "Any elim: business (T1+T2)"
label var elim_any_clinical   "Any elim: clinical (T1+T2)"
label var elim_any_admin      "Any elim: IT/legal/HR (T1+T2)"
label var elim_any_upper      "Any elim: upper mgmt (T1)"
label var elim_any_middle     "Any elim: middle mgmt (T2)"