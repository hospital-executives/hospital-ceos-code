
import pandas as pd
from collections import defaultdict
import sys
import os

## load data
if '--from_r' in sys.argv:
    data_path = sys.argv[sys.argv.index('--from_r') + 1]
else:
    print("WARNING - NOT USING R INPUT")
    data_path = "/Users/loaner/Dropbox/hospital_ceos/_data"

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from helper_scripts import xwalk_helper as hlp

# load data
haentity_path = os.path.join(data_path, "derived/auxiliary/haentity.feather")
haentity = pd.read_feather(haentity_path)
haentity['mcrnum'] = haentity['medicarenumber'].apply(hlp.clean_and_convert)
xwalk_path = os.path.join(data_path, "supplemental/hospital_ownership.dta")
x_walk_2_data = pd.read_stata(xwalk_path)

# CLEAN DFS
df1, hospitals = hlp.process_dfs(x_walk_2_data, haentity)
ref_df = df1.dropna(subset=['address', 'ahaid', 'year', 'mcrnum', 'zipcode'])

# DIRECT MATCH ON MCR:AHA USING XWALK THEN MCR:AHA IF ADDRESSES MATCH 
# make  basic medicare to aha crosswalk 
x_walk_2_mcr = defaultdict(set)
for _, row in x_walk_2_data.iterrows():
    key = (hlp.clean_and_convert(row['mcrnum']), row['year'])
    x_walk_2_mcr[key].add(hlp.clean_and_convert(row['ahaid_noletter']))
medicare_to_aha = {k: v for k, v in x_walk_2_mcr.items() if len(v) == 1}
mcr_mismatch = {k: v for k, v in x_walk_2_mcr.items() if len(v) > 1}

# make address xwalk
address_matches = hlp.mcr_to_address(haentity, mcr_mismatch, df1, 
                                     "entity_address", "address")

filled_values = []
for _, row in hospitals.iterrows():
    key1 = (row['cleaned_mcr'], row['year'])
    if key1 in medicare_to_aha:
        filled_values.append(next(iter(medicare_to_aha[key1])))
    elif key1 in address_matches:
        filled_values.append(address_matches[key1])
    else:
        filled_values.append(None)

hospitals['filled_aha'] = filled_values

# SUBSET BY ZIP CODE AND MATCH BY SIMILAR ADDRESSSES
# for each year, zip find all addresses, ahaid and pick the aha with the most
# similar address
match_with_mcr, match_wo_mcr = hlp.year_zip_add(df1, "address_clean", hospitals, "address_clean")
hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        match_with_mcr[(row['year'], row['zip'], row['mcrnum'], row['address_clean'])]
        if pd.isna(row['filled_aha']) and 
        (row['year'], row['zip'], row['mcrnum'], row['address_clean']) in 
        match_with_mcr
        else row['filled_aha']
    ),
    axis=1
)
hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        match_wo_mcr[(row['year'], row['zip'],  row['address_clean'])]
        if pd.isna(row['filled_aha']) and 
        (row['year'], row['zip'],  row['address_clean']) in 
        match_wo_mcr
        else row['filled_aha']
    ),
    axis=1
)
## 6364 obs missing AHA of which 2220 have valid medicare numbers

## MATCH ON NAME & ADDRESS
cleaned_aha_name_address_dict = hlp.name_add(hospitals, x_walk_2_data, "address")

hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        cleaned_aha_name_address_dict.get(
            (str(row['entity_name']).lower().strip(), str(row['entity_address']).lower().strip())
        )
        if pd.isna(row['filled_aha']) and 
           (str(row['entity_name']).lower().strip(), str(row['entity_address']).lower().strip()) in cleaned_aha_name_address_dict
        else row['filled_aha']
    ),
    axis=1
)

## (ZIP, NAME) : AHA
zip_name_aha = hlp.zip_name(df1)

hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        zip_name_aha.get(
            (str(row['zip']), str(row['entity_name']).lower().strip(), int(row['year']))
        )
        if pd.isna(row['filled_aha']) and 
           (str(row['zip']), str(row['entity_name']).lower().strip(), int(row['year'])) in zip_name_aha
        else row['filled_aha']
    ),
    axis=1
)

## USE NAME TO FIND CLOSE MATCH ON ADDRESS
name_to_addresses = defaultdict(list)
for _, row in df1.iterrows():
    name_to_addresses[row['clean_name']].append((row['address_clean'], row['ahaid']))
hospitals['filled_aha'] = hospitals.apply(
    lambda row: hlp.get_aha_from_address(row, name_to_addresses),
    axis=1
)

## USE JUST ADDRESS TO FIND AHA
add_aha = hlp.add_only(df1,"address_clean")
hospitals['filled_aha'] = hospitals.apply(
    lambda row: hlp.fill_from_add_aha(row, add_aha) if pd.isna(row['filled_aha']) else row['filled_aha'],
    axis=1
)

# 1996 OBS REMAINING / 3675 ROWS HERE
### PART TWO WITH SUPER CLEANED ADDRESS
match_with_mcr, match_wo_mcr = hlp.year_zip_add(df1, "address_clean_std", hospitals, "address_clean_std")
hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        match_with_mcr[(row['year'], row['zip'], row['mcrnum'], row['address_clean_std'])]
        if pd.isna(row['filled_aha']) and 
        (row['year'], row['zip'], row['mcrnum'], row['address_clean_std']) in 
        match_with_mcr
        else row['filled_aha']
    ),
    axis=1
)
hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        match_wo_mcr[(row['year'], row['zip'],  row['address_clean_std'])]
        if pd.isna(row['filled_aha']) and 
        (row['year'], row['zip'],  row['address_clean_std']) in 
        match_wo_mcr
        else row['filled_aha']
    ),
    axis=1
)
## MATCH ON NAME & ADDRESS
cleaned_aha_name_address_dict = hlp.name_add(hospitals, x_walk_2_data, "address_clean_std")

hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        cleaned_aha_name_address_dict.get(
            (str(row['entity_name']).lower().strip(), str(row['address_clean_std']).lower().strip())
        )
        if pd.isna(row['filled_aha']) and 
           (str(row['entity_name']).lower().strip(), str(row['address_clean_std']).lower().strip()) in cleaned_aha_name_address_dict
        else row['filled_aha']
    ),
    axis=1
)

## USE NAME TO FIND CLOSE MATCH ON ADDRESS
name_to_addresses = defaultdict(list)
for _, row in df1.iterrows():
    name_to_addresses[row['clean_name']].append((row['address_clean_std'], row['ahaid']))
hospitals['filled_aha'] = hospitals.apply(
    lambda row: hlp.get_aha_from_address(row, name_to_addresses),
    axis=1
)

## USE JUST ADDRESS TO FIND AHA
add_aha = hlp.add_only(df1,"address_clean_std")
hospitals['filled_aha'] = hospitals.apply(
    lambda row: hlp.fill_from_add_aha(row, add_aha) if pd.isna(row['filled_aha']) else row['filled_aha'],
    axis=1
)

## USE ZIP CODE DISTANCES
filled_na = hlp.zip_distance(hospitals, x_walk_2_data)
pre_filled = hospitals[hospitals['filled_aha'].notna()]


combined_df = pd.concat([pre_filled, filled_na], ignore_index=True)
combined_df['missing_aha'] = combined_df['filled_aha'].isna().astype(int)

export_path = os.path.join(data_path, "derived/auxiliary/xwalk.csv")
combined_df.to_csv(export_path)
