
import pandas as pd
from collections import defaultdict
import sys
import os
import numpy as np

## load data
### FOR INTEGRATION UPSTREAM - INCOMPLETE:
#if '--from_r' in sys.argv:
    #data_path = sys.argv[sys.argv.index('--from_r') + 1]
#else:
    #print("WARNING - NOT USING R INPUT")
    #data_path = "/Users/katherinepapen/Library/CloudStorage/Dropbox/hospital_ceos/_data"

### FOR INTEGRATION DOWNSTREAM:
if len(sys.argv) == 3:
    data_path = sys.argv[1]
    aha_output = sys.argv[2]

else: 
    data_path = "/Users/katherinepapen/Library/CloudStorage/Dropbox/hospital_ceos/_data"
    aha_output = os.path.join(data_path, "derived/auxiliary/aha_himss_xwalk.csv")
    print('WARNING: not using Makefile inputs')

key_path = data_path[:-5] + "api_keys/geocoding_key.txt"

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from helper_scripts import xwalk_helper as hlp

# load data
haentity_path = os.path.join(data_path, "derived/auxiliary/haentity.feather")
haentity = pd.read_feather(haentity_path)
haentity['mcrnum'] = haentity['medicarenumber'].apply(hlp.clean_and_convert)

haentity = (
    haentity.sort_values(['entity_uniqueid', 'year']).reset_index(drop=True)
      .assign(
          mcr_before=lambda d: d.groupby('entity_uniqueid', sort=False)['medicarenumber'].ffill(),
          mcr_after =lambda d: d.groupby('entity_uniqueid', sort=False)['medicarenumber'].bfill(),
          mcr_filled=lambda d: np.select(
              [
                  d['mcrnum'].isna() & d['mcr_before'].notna() & d['mcr_after'].notna() & (d['mcr_before'] == d['mcr_after']),
                  d['mcrnum'].isna() & d['mcr_before'].notna() & d['mcr_after'].isna(),
                  d['mcrnum'].isna() & d['mcr_before'].isna() & d['mcr_after'].notna(),
              ],
              [d['mcr_after'], d['mcr_before'], d['mcr_after']],
              default=d['mcrnum']
          )
      )
)

haentity['mcrnum'] = haentity['mcr_filled']

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

# only keep cases where mcrnum has one corresponding entry and also
# mcrnum only has one ahaid
hosp_counts = (
    hospitals.groupby(['mcrnum', 'year'])['entity_uniqueid']
    .nunique()
)

mcr_with_multiple_aha = (
    x_walk_2_data.groupby('mcrnum')['ahaid_noletter']
    .nunique()
    .loc[lambda x: x > 1]
    .index
    .tolist()
)

medicare_to_aha = {
    k: v
    for k, v in x_walk_2_mcr.items()  # assume k = (mcrnum, year)
    if len(v) == 1
       and hosp_counts.get((k[0], k[1]), 0) == 1
       and str(int(k[0])) not in mcr_with_multiple_aha
    # and str(next(iter(v))) not in aha_with_multiple_mcr
}

mcr_mismatch = {k: v for k, v in x_walk_2_mcr.items() 
                if k not in medicare_to_aha.keys()}

# make address xwalk
address_matches = hlp.mcr_to_address(haentity, mcr_mismatch, df1, 
                                     "entity_address", "address")
cleaned_add_matches = {k:v for k, v in address_matches.items()
                       if k[0] not in mcr_with_multiple_aha}

filled_values = []
fuzzy_flags = []

for _, row in hospitals.iterrows():
    key1 = (row['cleaned_mcr'], row['year'])
    if key1 in medicare_to_aha:
        filled_values.append(next(iter(medicare_to_aha[key1])))
        fuzzy_flags.append(0)
    elif key1 in address_matches:
        filled_values.append(cleaned_add_matches[key1])
        fuzzy_flags.append(1)
    else:
        filled_values.append(None)
        fuzzy_flags.append(None)


hospitals['filled_aha'] = filled_values
hospitals['fuzzy_flag'] = fuzzy_flags
hospitals['exact_match'] = fuzzy_flags

# SUBSET BY ZIP CODE AND MATCH BY SIMILAR ADDRESSSES
# for each year, zip find all addresses, ahaid and pick the aha with the most
# similar address
match_with_mcr, match_wo_mcr = hlp.year_zip_add(df1, "address_clean", hospitals, 
                                                "address_clean")

## removed match_with_mcr since it added no additional cleaned entries

hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        match_wo_mcr[(row['year'], row['zip'],  row['address_clean'])][0]
        if pd.isna(row['filled_aha']) and 
        (row['year'], row['zip'],  row['address_clean']) in 
        match_wo_mcr
        else row['filled_aha']
    ),
    axis=1
)

hospitals['fuzzy_flag'] = hospitals.apply(
    lambda row: (
        match_wo_mcr[(row['year'], row['zip'],  row['address_clean'])][1]
        if pd.isna(row['fuzzy_flag']) and 
        (row['year'], row['zip'],  row['address_clean']) in 
        match_wo_mcr
        else row['fuzzy_flag']
    ),
    axis=1
)

## 6364 obs missing AHA of which 2220 have valid medicare numbers

## MATCH ON NAME & ADDRESS
cleaned_aha_name_address_dict = hlp.name_add(hospitals, x_walk_2_data, 
                                             "address")

hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        cleaned_aha_name_address_dict.get(
            (str(row['entity_name']).lower().strip(), str(row['entity_address']
                                                          ).lower().strip())
        )
        if pd.isna(row['filled_aha']) and 
           (str(row['entity_name']).lower().strip(), 
            str(row['entity_address']).lower().strip()) 
            in cleaned_aha_name_address_dict
        else row['filled_aha']
    ),
    axis=1
)

hospitals.loc[
    hospitals['filled_aha'].notna() & hospitals['fuzzy_flag'].isna(),
    'fuzzy_flag'
] = 0

## (ZIP, NAME) : AHA - removed; did not add filled_ahas

## USE NAME TO FIND CLOSE MATCH ON ADDRESS
name_to_addresses = defaultdict(list)
for _, row in df1.iterrows():
    name_to_addresses[row['clean_name']].append((row['address_clean'], 
                                                 row['ahaid']))
    
hospitals[['filled_aha', 'fuzzy_flag']] = hospitals.apply(
    lambda row: hlp.get_aha_from_address(row, name_to_addresses),
    axis=1,
    result_type='expand'
)

## USE JUST ADDRESS TO FIND AHA
add_aha = hlp.add_only(df1,"address_clean")
hospitals[['filled_aha', 'fuzzy_flag']] = hospitals.apply(
    lambda row: (
        hlp.fill_from_add_aha(row, add_aha, "address_clean") 
        if pd.isna(row['filled_aha']) else (row['filled_aha'], row['fuzzy_flag'])
    ),
    axis=1,
    result_type='expand'
)


# 1996 OBS REMAINING / 3675 ROWS HERE
### PART TWO WITH SUPER CLEANED ADDRESS
match_with_mcr, match_wo_mcr = hlp.year_zip_add(df1,
                                                 "address_clean_std",
                                                   hospitals, 
                                                   "address_clean_std")
hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        match_with_mcr[(row['year'], row['zip'], row['mcrnum'], 
                        row['address_clean_std'])][0]
        if pd.isna(row['filled_aha']) and 
        (row['year'], row['zip'], row['mcrnum'], row['address_clean_std']) in 
        match_with_mcr
        else row['filled_aha']
    ),
    axis=1
)


hospitals['fuzzy_flag'] = hospitals.apply(
    lambda row: (
        match_with_mcr[(row['year'], row['zip'], row['mcrnum'], row['address_clean_std'])][1]
        if pd.isna(row['fuzzy_flag']) and 
        (row['year'], row['zip'], row['mcrnum'],  row['address_clean_std']) in 
        match_with_mcr
        else row['fuzzy_flag']
    ),
    axis=1
)

hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        match_wo_mcr[(row['year'], row['zip'],  row['address_clean_std'])][0]
        if pd.isna(row['filled_aha']) and 
        (row['year'], row['zip'],  row['address_clean_std']) in 
        match_wo_mcr
        else row['filled_aha']
    ),
    axis=1
)


hospitals['fuzzy_flag'] = hospitals.apply(
    lambda row: (
        match_wo_mcr[(row['year'], row['zip'], row['address_clean_std'])][1]
        if pd.isna(row['fuzzy_flag']) and 
        (row['year'], row['zip'], row['address_clean_std']) in 
        match_wo_mcr
        else row['fuzzy_flag']
    ),
    axis=1
)

## MATCH ON NAME & ADDRESS
cleaned_aha_name_address_dict = hlp.name_add(hospitals, x_walk_2_data, 
                                             "address_clean_std")

hospitals['filled_aha'] = hospitals.apply(
    lambda row: (
        cleaned_aha_name_address_dict.get(
            (str(row['entity_name']).lower().strip(), 
             str(row['address_clean_std']).lower().strip())
        )
        if pd.isna(row['filled_aha']) and 
           (str(row['entity_name']).lower().strip(), 
            str(row['address_clean_std']).lower().strip()) 
            in cleaned_aha_name_address_dict
        else row['filled_aha']
    ),
    axis=1
)

## USE NAME TO FIND CLOSE MATCH ON ADDRESS
name_to_addresses = defaultdict(list)
for _, row in df1.iterrows():
    name_to_addresses[row['clean_name']].append((row['address_clean_std'], 
                                                 row['ahaid']))
hospitals[['filled_aha', 'fuzzy_flag']] = hospitals.apply(
    lambda row: hlp.get_aha_from_address(row, name_to_addresses),
    axis=1,
    result_type='expand'
)


## USE JUST ADDRESS TO FIND AHA
add_aha = hlp.add_only(df1,"address_clean_std")
hospitals[['filled_aha', 'fuzzy_flag']] = hospitals.apply(
    lambda row: (
        hlp.fill_from_add_aha(row, add_aha, 'address_clean_std') 
        if pd.isna(row['filled_aha']) else (row['filled_aha'], row['fuzzy_flag'])
    ),
    axis=1,
    result_type='expand'
)


## USE ZIP CODE DISTANCES
missing_loc_df = hospitals[(hospitals['filled_aha'].isna()) & 
          (~hospitals['latitude'].notna() | ~hospitals['longitude'].notna())]
missing_adds = missing_loc_df[['entity_address', 'entity_zip', 
                               'entity_zip_five', 'entity_city', 
                               'entity_state', 'address_clean', 
                               'address_clean_std']].drop_duplicates()

###### LOAD GEOCODED DFS
cleaned_census = hlp.load_census(data_path)
cleaned_api = hlp.load_neonatim(data_path)

filled_census, unfilled_census = hlp.updated_match_census_adds(missing_adds, data_path)
filled_api, unfilled_api = hlp.match_api_adds(unfilled_census, cleaned_api)

###### GEOCODE REMAINING MISSING LOCATIONS
with open(key_path, 'r') as file:
    api_key = file.read()
filled_google = hlp.geocode_addresses(unfilled_api, data_path, api_key)

### create list of missing aha numbers and their coordinates
locations = pd.concat([filled_census, filled_api,filled_google], ignore_index=True)

hospitals['_row_id'] = range(len(hospitals))

## merge with hospitals data frame
hospitals_merged = hospitals.merge(
    locations[['entity_address', 'entity_city', 'entity_state', 'latitude', 
               'longitude']].drop_duplicates(),
    on=['entity_address', 'entity_city', 'entity_state'],
    how='left',
    suffixes=('', '_new')  # prevent immediate overwriting
)

# clean up latitude and longitude
hospitals_merged['latitude'] = hospitals_merged['latitude'].\
    combine_first(hospitals_merged['latitude_new'])
hospitals_merged['longitude'] = hospitals_merged['longitude'].\
    combine_first(hospitals_merged['longitude_new'])
hospitals_merged = hospitals_merged.drop(columns=['latitude_new', 
                                                  'longitude_new'])

# combine dfs with updated aha

filled_na = hlp.impute_aha_with_loc(hospitals_merged, x_walk_2_data, 1/3)
filled_na.loc[
    filled_na['filled_aha'].notna() & filled_na['fuzzy_flag'].isna(),
    'fuzzy_flag'
] = 0
pre_filled = hospitals[hospitals['filled_aha'].notna()]

# indicator for geocoding
pre_filled['geocoded'] = False
filled_na['geocoded'] = True

combined_df = pd.concat([pre_filled, filled_na], ignore_index=True)\
.drop_duplicates()
combined_df['missing_aha'] = combined_df['filled_aha'].isna().astype(int)

#### finalize latitude and longitude numbers

## get lat/lon from aha
aha_coords = x_walk_2_data[
    x_walk_2_data['lat'].notna() & x_walk_2_data['lon'].notna() &
    (x_walk_2_data['lat'] != 0) & (x_walk_2_data['lon'] != 0)
]

# Drop duplicates to keep the first valid (lat, lon) per ahanumber
first_latlon = aha_coords.drop_duplicates(subset='ahaid_noletter', keep='first')
ahanumber_to_latlon = dict(zip(first_latlon['ahaid_noletter'], 
                               zip(first_latlon['lat'], first_latlon['lon'])))

# map with AHA first
mapped_coords = combined_df['filled_aha'].map(
    lambda x: ahanumber_to_latlon.get(str(int(x))) if pd.notna(x) else pd.NA
)
combined_df['latitude'] = np.where(
    combined_df['latitude'].isna(),
    mapped_coords.map(lambda x: x[0] if pd.notna(x) else pd.NA),
    combined_df['latitude']
)

combined_df['longitude'] = np.where(
    combined_df['longitude'].isna(),
    mapped_coords.map(lambda x: x[1] if pd.notna(x) else pd.NA),
    combined_df['longitude']
)

# manually add coords if possible
address_to_coords = {
    "930 chestnut ridge road": (39.654877, -79.95564),
    "718 glenview avenue":(42.19076,-87.805744),
    "2051 hamill road":(35.12474,-85.237919),
    "400 old river road":(35.351425,-119.112538),
    "4601 corbett drive":(40.522013,-105.02846),
    "1501 hartford street":(40.427303, -86.880745)
}

combined_df['latitude'] = np.where(
    combined_df['latitude'].isna(),
    combined_df['entity_address'].map(lambda addr: address_to_coords.get(addr, (pd.NA, pd.NA))[0]),
    combined_df['latitude']
)

combined_df['longitude'] = np.where(
    combined_df['longitude'].isna(),
    combined_df['entity_address'].map(lambda addr: address_to_coords.get(addr, (pd.NA, pd.NA))[1]),
    combined_df['longitude']
)

# export aha numbers
combined_df.to_csv(aha_output)
