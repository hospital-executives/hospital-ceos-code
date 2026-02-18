args target_script

if "`target_script'" == "" {
    di as error "Missing target script argument."
    exit 198
}

local script_dir = c(pwd)

do "`script_dir'/00_master.do"
do "`script_dir'/`target_script'"
