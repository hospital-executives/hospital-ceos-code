******** Summary Stats ********

***Before you start:
*1: open terminal
*2: navigate to your local version of the repo
*3: run "make compile_himss"
*In doing so, this will generate a custom configuration file with shortcuts


* Include the custom configuration file
*do "config_stata.do"
do "/Users/ambarlaforgia/hospital-ceos-code/analysis_code/config_stata.do"

* Now you have the following globals to use as shortcuts to the DropBox:
* DERIVED_DATA,RAW_DATA, SUPPLEMENTAL_DATA, AUXILIARY_DATA, NICKNAMES_DICTIONARIES 

use "$DERIVED_DATA/final_confirmed_new.dta", clear
