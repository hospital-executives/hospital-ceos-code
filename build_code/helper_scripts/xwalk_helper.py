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

    hospitals = haentity[haentity['haentitytypeid'] == "1"]
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
            match_with_mcr[(row['year'], (row['zip']), row['mcrnum'], 
                            row[ha_address])] = matched_aha
            match_wo_mcr[(row['year'], (row['zip']), 
                                row[ha_address])] = matched_aha
            
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
        return row['filled_aha']  # already filled

    name_key = row['clean_name']
    address = str(row['address_clean'])

    if name_key not in name_to_addresses:
        return None

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
        if score >= 85:
            return candidates[idx][1]  # ahaid
    return None

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

def fill_from_add_aha(row, add_aha):
    if pd.notna(row['filled_aha']):
        return row['filled_aha']
    
    key = (row['address_clean'], row['year'])
    if key not in add_aha:
        return None

    ref_name, ahaid = add_aha[key]
    
    score = token_sort_ratio(
        str(row['entity_name']).lower().strip(),
        str(ref_name).lower().strip()
    )

    return ahaid if score >= 85 else None

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