## this code contains the helper functions for the crosswalk
import re
import pandas as pd
from collections import defaultdict
from rapidfuzz import process, fuzz
from rapidfuzz.fuzz import token_sort_ratio
import string
import ssl
import certifi
import geopy.geocoders
from geopy.exc import GeocoderTimedOut
import time
from tqdm import tqdm
from sklearn.neighbors import BallTree
import numpy as np
import requests
import json

## define helpful functions
def clean_and_convert(val):
    """Try to convert a string to a number. If not, strip letters and retry."""
    if val == "" or pd.isna(val):
        return None

    try:
        return float(val)
    except:
        # Strip letters and non-digits, keep only numbers and periods
        cleaned = re.sub(r"[^\d.]", "", val)
        try:
            return float(cleaned) if cleaned else None
        except:
            print(val)

def clean_name(name):
        return ''.join(c for c in str(name).lower() if c not in string.punctuation).strip()

def standardize_address_column(series: pd.Series) -> pd.Series:
    direction_map = {
        r'\bnorth\b': 'n',
        r'\bsouth\b': 's',
        r'\beast\b': 'e',
        r'\bwest\b': 'w'
    }

    suffix_map = {
        r'\bstreet\b': 'st',
        r'\bavenue\b': 'ave',
        r'\bdrive\b': 'dr',
        r'\broad\b': 'rd',
        r'\bboulevard\b': 'blvd',
        r'\blane\b': 'ln',
        r'\bplace\b': 'pl',
        r'\btrail\b': 'trl',
        r'\bparkway\b': 'pkwy',
        r'\bcourt\b': 'ct',
        r'\bsquare\b': 'sq',
        r'\bterrace\b': 'ter',
        r'\bcircle\b': 'cir'
    }

    number_map = {
        r'\bfirst\b': '1st',
        r'\bsecond\b': '2nd',
        r'\bthird\b': '3rd',
        r'\bfourth\b': '4th',
        r'\bfifth\b': '5th',
        r'\bsixth\b': '6th',
        r'\bseventh\b': '7th',
        r'\beighth\b': '8th',
        r'\bninth\b': '9th',
        r'\btenth\b': '10th'
    }

    def clean_address(addr):
        if pd.isna(addr):
            return addr
        addr = str(addr).lower()
        addr = addr.translate(str.maketrans('', '', string.punctuation))  # remove punctuation

        # Apply replacements
        for pattern, repl in direction_map.items():
            addr = re.sub(pattern, repl, addr)
        for pattern, repl in suffix_map.items():
            addr = re.sub(pattern, repl, addr)
        for pattern, repl in number_map.items():
            addr = re.sub(pattern, repl, addr)

        return re.sub(r'\s+', ' ', addr).strip()  # Normalize whitespace

    return series.apply(clean_address)

def process_dfs(x_walk_2_data, haentity):

    df1 = x_walk_2_data[['address', 'ahaid_noletter', 'year','mcrnum', 'zipcode', 'name']].rename(
    columns={
        'address': 'address',
        'ahaid_noletter': 'ahaid', 
        'year': 'year',
        'mcrnum': 'mcrnum',
        'zipcode': 'zipcode',
        'name' : 'name'
        }
    )
    df1['ahaid'] = df1['ahaid'].apply(clean_and_convert)
    df1['mcrnum'] = df1['mcrnum'].apply(clean_and_convert)
    df1['zipcode'] = df1['zipcode'].astype(str)
    df1['zipcode'] = df1['zipcode'].str[:5]
    df1['zipcode'] = df1['zipcode'].str.zfill(5)
    df1['address_clean'] = df1['address'].astype(str).str.lower().str.strip()
    df1['clean_name'] = df1['name'].apply(clean_name)

    hospitals = haentity #[haentity['haentitytypeid'] == "1"]
    hospitals['cleaned_mcr'] = hospitals['medicarenumber'].apply(clean_and_convert)
    hospitals['year'] = hospitals['year'].astype(int)
    hospitals['address_clean'] = hospitals['entity_address'].astype(str).str.lower().str.strip()
    hospitals['zip'] = hospitals['entity_zip'].astype(str)
    hospitals['zip'] = hospitals['zip'].str[:5]
    hospitals['zip'] = hospitals['zip'].str.zfill(5)
    hospitals['clean_name'] = hospitals['entity_name'].apply(clean_name)
          
    hospitals['address_clean_std'] = standardize_address_column(hospitals['address_clean'])
    df1['address_clean_std'] = standardize_address_column(df1['address_clean'])

    return df1, hospitals

def mcr_to_address(haentity, mcr_mismatch, ref_df, ha_address, ref_address):
    
    address_counts = (
        haentity
        .groupby(['mcrnum', 'year'])[ha_address]
        .nunique()
        .reset_index(name='n_address')
    )
    valid_keys = address_counts[address_counts['n_address'] == 1][['mcrnum', 'year']]

    filtered_df = haentity.merge(valid_keys, on=['mcrnum', 'year'], how='inner')

    mcr_to_real_address = {
        (row['mcrnum'], int(row['year'])): row[ha_address]
        for _, row in filtered_df.iterrows()
    }
    # get address match where each mcr has one address
    address_matches = {}
    for (mcrnum, year) in mcr_mismatch:

        key = (mcrnum, year)
        if key in mcr_to_real_address:
            real_address = mcr_to_real_address[(mcrnum, year)]

            # Filter ref_df for the same (mcrnum, year)
            subset = ref_df[(ref_df['mcrnum'] == mcrnum) & 
                                (ref_df['year'] == year)]
                
            if subset.empty:
                address_matches[(mcrnum, year)] = None
                continue

            # Get list of addresses from this subset
            addresses = subset[ref_address].tolist()

            # Perform fuzzy matching
            match_result = process.extractOne(real_address, addresses, 
                                                scorer=fuzz.token_sort_ratio)

            if match_result is None:
                address_matches[(mcrnum, year)] = None
                continue

            matched_address, score, idx = match_result
            if score >= 80:
                # Get all rows in subset that match the matched_address
                matches_df = subset[subset[ref_address] == matched_address]
                unique_ahaids = matches_df['ahaid'].unique()

                if len(unique_ahaids) == 1:
                    address_matches[(mcrnum, year)] = unique_ahaids[0]
                else:
                    # Conflict: same address matches multiple different ahaids
                    address_matches[(mcrnum, year)] = None
            else:
                address_matches[(mcrnum, year)] = None
        
    return address_matches

def year_zip_add(df1, ref_address, hospitals, ha_address):

    unique_rows = df1[['year', 'zipcode', ref_address, 'ahaid']].drop_duplicates()
    #unique_rows['zip_numeric'] = pd.to_numeric(unique_rows['zipcode'])
    address_map = defaultdict(set)
    for _, row in unique_rows.iterrows():
        key = (row['year'], row['zipcode'], row[ref_address])
        address_map[key].add(row['ahaid'])

    year_zip_to_addresses = defaultdict(list)
    for (year, zip_, address), ahaids in address_map.items():
        for ahaid in ahaids:
            year_zip_to_addresses[(year, zip_)].append((address, ahaid))

    filtered_df = hospitals[hospitals['filled_aha'].isna()]
    match_with_mcr = {}  # Will store (year, zip, mcrnum, row_address) -> matched_aha
    match_wo_mcr = {}

    for _, row in filtered_df.iterrows():
        year = row['year']
        zip = (row['zip'])
        candidates = year_zip_to_addresses.get((year,zip), [])

        # Get just the list of addresses to match against
        candidate_addresses = [addr for addr, _ in candidates]

        if not candidate_addresses:
            continue

        # Fuzzy match the current row's address to candidate addresses
        match_result = process.extractOne(
                row[ha_address], 
                candidate_addresses, 
                scorer=fuzz.token_sort_ratio
            )

        if match_result:
            matched_address, score, idx = match_result
            matched_aha = candidates[idx][1]

                # Check if the score is high OR if one address is a substring of the other
        if (
            score >= 80 or 
            matched_address in row[ha_address] or 
            row[ha_address] in matched_address
            ):
            
            if score == 100:
                if not pd.isna(row['mcrnum']):
                    match_with_mcr[(row['year'], (row['zip']), row['mcrnum'], 
                                row[ha_address])] = (matched_aha, 0)
                else:
                    match_wo_mcr[(row['year'], (row['zip']), 
                                    row[ha_address])] = (matched_aha, 0)
            else:
                if not pd.isna(row['mcrnum']):
                    match_with_mcr[(row['year'], (row['zip']), row['mcrnum'], 
                                row[ha_address])] = (matched_aha, 1)
                else:
                    match_wo_mcr[(row['year'], (row['zip']), 
                                    row[ha_address])] = (matched_aha, 1)
            
    return match_with_mcr, match_wo_mcr

def name_add(hospitals, x_walk_2_data, ref_address):
    na = hospitals[hospitals['filled_aha'].isna()]
    na['year'].value_counts().sort_index()

    x_walk_2_data['address_clean'] = \
        x_walk_2_data['address'].astype(str).str.lower().str.strip()
    x_walk_2_data['address_clean_std'] = standardize_address_column(
        x_walk_2_data['address_clean'])

    ## MATCH ON ENTITY NAME AND ENTITY ADDRESS
    aha_name_address_dict =  defaultdict(set)
    for _, row in x_walk_2_data.iterrows():
        key = (
            str(row['name']).lower().strip(),
            str(row[ref_address]).lower().strip()
        )    
        aha_name_address_dict[key].add(clean_and_convert(row['ahaid_noletter']))

    cleaned_aha_name_address_dict = {
        k: next(iter(v)) for k, v in aha_name_address_dict.items() if len(v) == 1
    }

    return cleaned_aha_name_address_dict
    
def zip_name(df1):
    # get cases where each address has one aha
    address_counts = (
        df1
        .groupby(['zipcode', 'name', 'year'])['ahaid']
        .nunique()
        .reset_index(name='n_id')
    )

    # Keep only (address, year) with a single unique ahaid
    valid_keys = address_counts[address_counts['n_id'] == 1][['zipcode', 'name', 'year']]

    # Filter df1 to those keys
    filtered_df = df1.merge(valid_keys, on=['zipcode', 'name', 'year'], how='inner')

    zip_name_aha = {
        (row['zipcode'], row['name'], int(row['year'])): row['ahaid']
        for _, row in filtered_df.iterrows()
    }

    return zip_name_aha

def get_aha_from_address(row, name_to_addresses):
    if pd.notna(row['filled_aha']):
        return (row['filled_aha'], row['fuzzy_flag'])  # already filled

    name_key = row['clean_name']
    address = str(row['address_clean'])

    if name_key not in name_to_addresses:
        return (None, None)

    # Get candidate addresses
    candidates = name_to_addresses[name_key]
    candidate_addresses = [addr for addr, _ in candidates]

    # Fuzzy match
    match_result = process.extractOne(
        address, 
        candidate_addresses, 
        scorer=fuzz.token_sort_ratio
    )

    if match_result:
        matched_address, score, idx = match_result
        if score == 100:
            return (candidates[idx][1],0)  # ahaid
        elif score >= 85:
            return (candidates[idx][1],1)
    return (None, None)

def add_only(df1, ref_address):
    address_counts = (
        df1
        .groupby([ref_address, 'year'])['ahaid']
        .nunique()
        .reset_index(name='n_id')
    )

    # Keep only (address, year) with a single unique ahaid
    valid_keys = address_counts[address_counts['n_id'] == 1][[ref_address, 'year']]

    # Filter df1 to those keys
    filtered_df = df1.merge(valid_keys, on=[ref_address, 'year'], how='inner')

    add_aha = {
        (row[ref_address], row['year']): (row['name'], row['ahaid'])
        for _, row in filtered_df.iterrows()
    }

    return add_aha

def fill_from_add_aha(row, add_aha, add_var):
    if pd.notna(row['filled_aha']):
        return (row['filled_aha'], row['fuzzy_flag'])
    
    key = (row[add_var], row['year'])
    if key not in add_aha:
        return (None, None)

    ref_name, ahaid = add_aha[key]
    
    score = token_sort_ratio(
        str(row['entity_name']).lower().strip(),
        str(ref_name).lower().strip()
    )

    if score == 100:
        return (ahaid, 0)
    elif score >= 85: 
        return (ahaid, 1)
    return (None, None)

def get_lat_lon(address, zip_code, geolocator, retry=3):
    full_address = f"{address}, {zip_code}"
    try:
        location = geolocator.geocode(full_address, timeout=10)
        if location:
            return (location.latitude, location.longitude)
        else:
            return (None, None)
    except GeocoderTimedOut:
        if retry > 0:
            time.sleep(1)
            return get_lat_lon(address, zip_code, geolocator, retry=retry-1)
        return (None, None)
    
def zip_distance(hospitals, x_walk_2_data):

    ## set up geolocator code
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    geolocator = geopy.geocoders.Nominatim(
        user_agent="hospital_locator",
        ssl_context=ssl_context
    )

    ## subset hospitals
    missing_lat_long = set(zip(
        hospitals.loc[
            hospitals['filled_aha'].isna() & 
            (hospitals['longitude'].isna() | hospitals['latitude'].isna()),
            'address_clean'
        ],
        hospitals.loc[
            hospitals['filled_aha'].isna() & 
            (hospitals['longitude'].isna() | hospitals['latitude'].isna()),
            'zip'
        ]
    ))

    # get lat / long coords
    results = []

    for address, zip_code in tqdm(missing_lat_long, desc="Geocoding addresses"):
        lat, lon = get_lat_lon(address, zip_code, geolocator)
        results.append({
            'address_clean': address,
            'zip': zip_code,
            'latitude': lat,
            'longitude': lon
        })

    # change to df and create dicts
    geocoded_df = pd.DataFrame(results)

    lat_dict = {
        (row['address_clean'], str(row['zip'])): row['latitude']
        for _, row in geocoded_df.iterrows()
    }
    lon_dict = {
        (row['address_clean'], str(row['zip'])): row['longitude']
        for _, row in geocoded_df.iterrows()
    }

    # Ensure zip is string for consistent keys
    hospitals['zip'] = hospitals['zip'].astype(str)

    # Fill only if lat/lon is missing
    hospitals['latitude'] = hospitals.apply(
        lambda row: lat_dict.get((row['address_clean'], row['zip']), 
                                 row['latitude'])
        if pd.isna(row['latitude']) else row['latitude'],
        axis=1
    )

    hospitals['longitude'] = hospitals.apply(
        lambda row: lon_dict.get((row['address_clean'], row['zip']), 
                                 row['longitude'])
        if pd.isna(row['longitude']) else row['longitude'],
        axis=1
    )

    target_df = hospitals[hospitals['filled_aha'].isna()]

    target_df['latitude'] = pd.to_numeric(target_df['latitude'], errors='coerce')
    target_df['longitude'] = pd.to_numeric(target_df['longitude'], errors='coerce')

    missing_lat_long = target_df[target_df[['latitude', 'longitude']].isna().any(axis=1)]
    target_df = target_df.dropna(subset=['latitude', 'longitude'])
    target_coords = np.radians(target_df[['latitude', 'longitude']].to_numpy())


    ref_df = x_walk_2_data[[ 'ahaid_noletter', 'lat', 'lon']].rename(
        columns={
            'ahaid_noletter': 'ahaid', 
            'lat': 'latitude',
            'lon': 'longitude'
            }
        )
    ref_df['ahaid'] = ref_df['ahaid'].apply(clean_and_convert)

    ref_df['latitude'] = pd.to_numeric(ref_df['latitude'], errors='coerce')
    ref_df['longitude'] = pd.to_numeric(ref_df['longitude'], errors='coerce')
    ref_df = ref_df.dropna(subset=['latitude', 'longitude'])

    ref_coords = np.radians(ref_df[['latitude', 'longitude']].to_numpy())

    # Build BallTree with reference points
    tree = BallTree(ref_coords, metric='haversine')

    # Query nearest neighbor (returns distance in radians)
    distances, indices = tree.query(target_coords, k=1)

    # Convert radians to km (Earth radius ~6371 km)
    dist_km = distances.flatten() * 6371  # Earth radius in km
    nearest_indices = indices.flatten()

    # Define a match threshold (e.g., 0.5 km = 500 meters)
    match_threshold_km = 0.5

    # Create a copy to avoid modifying original
    target_df = target_df.copy()

    target_df = target_df.reset_index(drop=True)

    # Fill in ahaid where within distance and aha is missing
    for i, (dist, idx) in enumerate(zip(dist_km, nearest_indices)):
        if dist <= match_threshold_km and pd.isna(target_df.loc[i, 'filled_aha']):
            target_df.loc[i, 'filled_aha'] = ref_df.iloc[idx]['ahaid']

    result = pd.concat([target_df, missing_lat_long], ignore_index=True)

    return result

def zip_distance_updated(hospitals, x_walk_2_data, data_path):

    census = set(zip(
        hospitals.loc[
            hospitals['filled_aha'].isna() & 
            (hospitals['longitude'].isna() | hospitals['latitude'].isna()),
            'address_clean'
        ],
        hospitals.loc[
            hospitals['filled_aha'].isna() & 
            (hospitals['longitude'].isna() | hospitals['latitude'].isna()),
            'entity_city'
        ],
         hospitals.loc[
            hospitals['filled_aha'].isna() & 
            (hospitals['longitude'].isna() | hospitals['latitude'].isna()),
            'entity_state'
        ],
        hospitals.loc[
            hospitals['filled_aha'].isna() & 
            (hospitals['longitude'].isna() | hospitals['latitude'].isna()),
            'zip'
        ]
    ))
    census_df = pd.DataFrame(list(census), columns=["address_clean", 
                                                    "entity_city", 
                                                    "entity_state", "zip"])

    dfs = []

    for i in range(1, 6):
        df = pd.read_csv(
        f"{data_path}/supplemental/census_adds/census_results_{i}.csv",
        header=None,
        engine="python",
        on_bad_lines="skip"
        )
    # df = df.reindex(columns=range(5))  # force 5 columns
        dfs.append(df)

    combined_census = pd.concat(dfs, ignore_index=True)

    cleaned = combined_census[(combined_census[2] == "Match")  &
                            ((combined_census[3] == "Exact") | 
                            (combined_census[3] == "Non_Exact"))]
    
    ## create address to lat/long merge
    df_full_renamed = cleaned.rename(columns={0: "id", 5: "latitude", 6: "longitude"})

    df_merged = census_df.merge(df_full_renamed[['id', 'latitude', 'longitude']],
                                on='id', how='left')

    # Step 1: Merge lat/lon from df_merged
    hospitals_merged = hospitals.merge(
        df_merged[['address_clean', 'entity_city', 'entity_state', 'zip', 
                   'latitude', 'longitude']],
        on=['address_clean', 'entity_city', 'entity_state', 'zip'],
        how='left',
        suffixes=('', '_new')  # prevent overwriting existing lat/lon columns immediately
    )

    # Step 2: Fill in only where current lat/lon is missing
    hospitals_merged['latitude'] = hospitals_merged['latitude'].combine_first(
        hospitals_merged['latitude_new'])
    hospitals_merged['longitude'] = hospitals_merged['longitude'].combine_first(
        hospitals_merged['longitude_new'])

    # Step 3: Drop temporary columns
    hospitals_merged = hospitals_merged.drop(columns=['latitude_new', 
                                                      'longitude_new'])

    target_df = hospitals_merged[hospitals_merged['filled_aha'].isna()]

    target_df['latitude'] = pd.to_numeric(target_df['latitude'], errors='coerce')
    target_df['longitude'] = pd.to_numeric(target_df['longitude'], errors='coerce')

    missing_lat_long = target_df[target_df[['latitude', 'longitude']].isna().any(axis=1)]
    target_df = target_df.dropna(subset=['latitude', 'longitude'])
    target_coords = np.radians(target_df[['latitude', 'longitude']].to_numpy())

    ref_df = x_walk_2_data[[ 'ahaid_noletter', 'lat', 'lon']].rename(
            columns={
                'ahaid_noletter': 'ahaid', 
                'lat': 'latitude',
                'lon': 'longitude'
                }
            )
    ref_df['ahaid'] = ref_df['ahaid'].apply(clean_and_convert)

    ref_df['latitude'] = pd.to_numeric(ref_df['latitude'], errors='coerce')
    ref_df['longitude'] = pd.to_numeric(ref_df['longitude'], errors='coerce')
    ref_df = ref_df.dropna(subset=['latitude', 'longitude'])

    ref_coords = np.radians(ref_df[['latitude', 'longitude']].to_numpy())

    tree = BallTree(ref_coords, metric='haversine')

        # Query nearest neighbor (returns distance in radians)
    distances, indices = tree.query(target_coords, k=1)

        # Convert radians to km (Earth radius ~6371 km)
    dist_km = distances.flatten() * 6371  # Earth radius in km
    nearest_indices = indices.flatten()

        # Define a match threshold (e.g., 0.5 km = 500 meters)
    match_threshold_km = 0.5

        # Create a copy to avoid modifying original
    target_df = target_df.copy()

    target_df = target_df.reset_index(drop=True)

        # Fill in ahaid where within distance and aha is missing
    for i, (dist, idx) in enumerate(zip(dist_km, nearest_indices)):
        if dist <= match_threshold_km and pd.isna(target_df.loc[i, 'filled_aha']):
            target_df.loc[i, 'filled_aha'] = ref_df.iloc[idx]['ahaid']

    result = pd.concat([target_df, missing_lat_long], ignore_index=True)

    return result

def load_census(data_path):

    ## load census data
    dfs = []

    for i in range(1, 6):
        df = pd.read_csv(
        f"{data_path}/supplemental/census_adds/census_results_{i}.csv",
        header=None,
        engine="python",
        on_bad_lines="skip"
        )
    # df = df.reindex(columns=range(5))  # force 5 columns
        dfs.append(df)

    ## format census data
    combined_census = pd.concat(dfs, ignore_index=True)
    df_clean = combined_census.dropna(subset=[4, 5])
    df_clean = df_clean[(df_clean[4].str.strip() != '') & (df_clean[5].str.strip() != '')]

    # Split column 4 into address, city, state, zip
    address_parts = df_clean[4].str.split(',', n=3, expand=True)
    address_parts.columns = ['address', 'city', 'state', 'zip']
    address_parts = address_parts.apply(lambda col: col.str.strip())

    # Split column 5 into latitude and longitude
    latlon_parts = df_clean[5].str.split(',', n=1, expand=True)
    latlon_parts.columns = ['latitude', 'longitude']
    latlon_parts = latlon_parts.apply(lambda col: col.str.strip())  # clean quotes/spaces

    # Convert latitude and longitude to float (safely)
    latlon_parts = latlon_parts.apply(pd.to_numeric, errors='coerce')

    # Drop any rows where conversion failed
    latlon_parts = latlon_parts.dropna()

    # Align the address parts to match the cleaned latlon index
    address_parts = address_parts.loc[latlon_parts.index]

    # Combine into final DataFrame
    coord_df = pd.concat([address_parts, latlon_parts], axis=1)

    return coord_df

def load_neonatim(data_path):
    raw_file = pd.read_csv(
        f'{data_path}/derived/auxiliary/geocoded_addresses_census.csv')
    
    df_renamed = raw_file.rename(columns={"0": 'address', "1": 'zipcode', 
                                          "2": 'latitude', "3": 'longitude'})

    # Step 2: Convert latitude and longitude to numeric (coerce errors to NaN)
    df_renamed['latitude'] = pd.to_numeric(df_renamed['latitude'], 
                                           errors='coerce')
    df_renamed['longitude'] = pd.to_numeric(df_renamed['longitude'], 
                                            errors='coerce')

    # Step 3: Drop rows where latitude or longitude is NaN
    cleaned_df = df_renamed.dropna(subset=['latitude', 'longitude'])
    cleaned_df['address_clean'] = cleaned_df['address'].astype(str).str.lower().str.strip()

    result = cleaned_df[['address', 'zipcode', 'latitude', 'longitude']].drop_duplicates()

    return result

def match_census_adds(missing_adds, cleaned_census):
    missing_adds = missing_adds.copy()
    cleaned_census = cleaned_census.copy()

    # Step 2: Uppercase address fields
    missing_adds['address_clean_upper'] = missing_adds['address_clean'].str.upper()
    missing_adds['address_clean_std_upper'] = missing_adds['address_clean_std'].str.upper()
    cleaned_census['address_upper'] = cleaned_census['address'].str.upper()

    # Step 3: First attempt — match on address_clean_upper and entity_zip_five
    merge1 = missing_adds.merge(
        cleaned_census[['address_upper', 'zip', 'latitude', 'longitude']],
        how='left',
        left_on=['address_clean_upper', 'entity_zip_five'],
        right_on=['address_upper', 'zip']
    ).drop_duplicates()

    # Step 4: Find unmatched rows and try address_clean_std_upper
    unmatched = merge1[merge1['latitude'].isna()].copy()

    # Second merge on standardized address
    merge2 = unmatched.drop(columns=['latitude', 'longitude', 'address_upper']).merge(
        cleaned_census[['address_upper', 'zip', 'latitude', 'longitude']],
        how='left',
        left_on=['address_clean_std_upper', 'entity_zip_five'],
        right_on=['address_upper', 'zip']
    ).drop_duplicates()

    # Step 5: Fill missing values from second match
    final = merge1.copy()
    final.loc[final['latitude'].isna(), 'latitude'] = merge2['latitude'].values
    final.loc[final['longitude'].isna(), 'longitude'] = merge2['longitude'].values

    # Step 6: Drop helper columns if desired
    final = final.drop(columns=['address_clean_upper', 'address_clean_std_upper', 'address_upper'])

    filled = final[final['latitude'].notna() & final['longitude'].notna()]
    unfilled = final[final['latitude'].isna() | final['longitude'].isna()]

    return filled,unfilled

def updated_match_census_adds(missing_adds, data_path):
    
    ## load census addresses:geocoding
    census_path = f"{data_path}/supplemental/census_adds/combined_census.csv"
    census_coords = pd.read_csv(census_path)
    census_coords['latitude'] = census_coords['latitude'].round(5)
    census_coords['longitude'] = census_coords['longitude'].round(5)

    ## remove dupes 
    dupes = census_coords.duplicated(subset=['address_clean', 'entity_city', 
                                             'entity_state'], keep=False)
    dupe_coords = census_coords[dupes]
    not_dupes = census_coords[~dupes]

    dupe_coords = dupe_coords[['address_clean', 'entity_city', 
                               'entity_state', 'latitude', 'longitude']]
    df_clean = dupe_coords.copy()
    df_clean['latitude'] = df_clean['latitude'].round(5)
    df_clean['longitude'] = df_clean['longitude'].round(5)

    # Step 2: Force string normalization for text fields
    text_cols = ['address_clean', 'entity_city', 'entity_state']
    for col in text_cols:
        df_clean[col] = df_clean[col].str.strip().str.upper()

    # Step 3: Sort and drop duplicates across all columns
    df_deduped = df_clean.sort_values(by=text_cols + 
                                      ['latitude', 'longitude']
                                      ).drop_duplicates()
    
    ## combine into final coords
    final_coords = pd.concat([not_dupes,df_deduped])

    ## merge with missing addresses
    final = missing_adds.merge(final_coords[['address_clean', 'entity_city',
                                                  'entity_state', 
                                                  'latitude', 'longitude']],
                                on=['address_clean', 'entity_city',
                                    'entity_state'], 
                                    how='left').drop_duplicates()
    
    filled = final[final['latitude'].notna() & final['longitude'].notna()]
    unfilled = final[final['latitude'].isna() | final['longitude'].isna()]

    return filled,unfilled

def match_api_adds(unfilled_census, cleaned_api):

    cleaned_api_dedup = cleaned_api.drop_duplicates(subset=['address', 'zipcode'])

    merged = unfilled_census.merge(
    cleaned_api_dedup[['address', 'zipcode', 'latitude', 'longitude']],
    how='left',
    left_on=['address_clean', 'entity_zip'],
    right_on=['address', 'zipcode'],
    suffixes=('', '_api')
    ).drop(columns=['address', 'zipcode'])

    # Step 2: Fill missing lat/lon from cleaned_api
    merged['latitude'] = merged['latitude'].combine_first(merged['latitude_api'])
    merged['longitude'] = merged['longitude'].combine_first(merged['longitude_api'])

    # Step 3: Drop helper columns from API if desired
    merged = merged.drop(columns=['latitude_api', 
                                  'longitude_api'])
    
    filled_result = merged[merged['latitude'].notna() & merged['longitude'].notna()].drop_duplicates()
    unfilled_result = merged[merged['latitude'].isna() | merged['longitude'].isna()].drop_duplicates()
    
    return filled_result,unfilled_result
 
def geocode_addresses(unfilled_api, data_path, API_KEY):
    cached_file = f"{data_path}/supplemental/google_geocoded.json"

    unfilled_api['full_address'] = unfilled_api['entity_address'].astype(str) \
        + ', ' + unfilled_api['entity_city'].astype(str) + ', ' + \
            unfilled_api['entity_state'].astype(str)
    
    unique_addresses = unfilled_api[['entity_address', 'entity_city',
                                     'entity_state', 'full_address']].\
                                        drop_duplicates()


    try:
        with open(cached_file, 'r') as f:
            cache = json.load(f)
    except FileNotFoundError:
        cache = {}

    def geocode(address):
        if address in cache:
            return cache[address]
        
        #else:
            #return None
        # removed to prevent overuse of API key
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        params = {"address": address, "key": API_KEY}
        response = requests.get(url, params=params)
        result = response.json()
        
        if result['status'] == 'OK':
            location = result['results'][0]['geometry']['location']
            lat_lng = (location['lat'], location['lng'])
            cache[address] = lat_lng
            time.sleep(0.1)  # pause to respect rate limits
            return lat_lng
        else:
            print(f"Failed to geocode: {address}, Status: {result['status']}")
            cache[address] = (None, None)
            return (None, None)
        
    results = []
    for _, row in tqdm(unique_addresses.iterrows(), 
                       total=len(unique_addresses), desc="Geocoding"):
        lat_lng = geocode(row['full_address'])
        results.append(lat_lng)

    unique_addresses[['latitude', 'longitude']] = \
        pd.DataFrame(results, index=unique_addresses.index)

    # Save updated cache
    with open(cached_file, 'w') as f:
            json.dump(cache, f, indent=2)
    
    unfilled_api = unfilled_api[['entity_address', 'entity_zip',
                                 'entity_zip_five', 'entity_city',
                                 'entity_state', 'address_clean',
                                 'address_clean_std']].merge(
    unique_addresses[['entity_address', 'entity_city', 'entity_state', 'latitude', 'longitude']],
    on=['entity_address', 'entity_city', 'entity_state'],
    how='left'
    )

    return unfilled_api

def impute_aha_with_loc(hospitals_merged, x_walk_2_data, input_threshold = 0.5):
    target_df = hospitals_merged[hospitals_merged['filled_aha'].isna()]

    target_df['latitude'] = pd.to_numeric(target_df['latitude'], 
                                          errors='coerce')
    target_df['longitude'] = pd.to_numeric(target_df['longitude'],
                                            errors='coerce')

    missing_lat_long = target_df[target_df[['latitude', 'longitude']
                                           ].isna().any(axis=1)]
    target_df = target_df.dropna(subset=['latitude', 'longitude'])
    target_coords = np.radians(target_df[['latitude', 'longitude']
                                         ].to_numpy())

    ref_df = x_walk_2_data[[ 'ahaid_noletter', 'lat', 'lon']].rename(
            columns={
                'ahaid_noletter': 'ahaid', 
                'lat': 'latitude',
                'lon': 'longitude'
                }
            )
    ref_df['ahaid'] = ref_df['ahaid'].apply(clean_and_convert)

    ref_df['latitude'] = pd.to_numeric(ref_df['latitude'], errors='coerce')
    ref_df['longitude'] = pd.to_numeric(ref_df['longitude'], errors='coerce')
    ref_df = ref_df.dropna(subset=['latitude', 'longitude'])

    ref_coords = np.radians(ref_df[['latitude', 'longitude']].to_numpy())

    tree = BallTree(ref_coords, metric='haversine')

        # Query nearest neighbor (returns distance in radians)
    distances, indices = tree.query(target_coords, k=1)

        # Convert radians to km (Earth radius ~6371 km)
    dist_km = distances.flatten() * 6371  # Earth radius in km
    nearest_indices = indices.flatten()

        # Define a match threshold (e.g., 0.5 km = 500 meters)
    match_threshold_km = input_threshold

        # Create a copy to avoid modifying original
    target_df = target_df.copy()

    target_df = target_df.reset_index(drop=True)

        # Fill in ahaid where within distance and aha is missing
    for i, (dist, idx) in enumerate(zip(dist_km, nearest_indices)):
        if dist <= match_threshold_km and pd.isna(target_df.loc[i, 'filled_aha']):
            target_df.loc[i, 'filled_aha'] = ref_df.iloc[idx]['ahaid']

    result = pd.concat([target_df, missing_lat_long], 
                       ignore_index=True).drop_duplicates()
    
    return result