
import pandas as pd
import re
from collections import defaultdict
from rapidfuzz import process, fuzz

# load data
x_walk_1_data = pd.read_stata('/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/supplemental/aha-id-medicare-id-crosswalk.dta')
x_walk_2_data = pd.read_stata('/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/supplemental/hospital_ownership.dta')
aha_data_csv = pd.read_csv('/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/supplemental/AHA_2004_2017.csv', encoding='latin1')

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
    
x_walk_2_mcr = defaultdict(set)
for _, row in x_walk_2_data.iterrows():
    key = (clean_and_convert(row['mcrnum']), row['year'])
    x_walk_2_mcr[key].add(clean_and_convert(row['ahaid_noletter']))


medicare_to_aha = {k: v for k, v in x_walk_2_mcr.items() if len(v) == 1}
mcr_mismatch = {k: v for k, v in x_walk_2_mcr.items() if len(v) > 1}

# save 
crosswalk_df = pd.DataFrame([
    {'mcrnum': k[0], 'year': k[1], 'aha_xwalk': list(v)[0]}
    for k, v in medicare_to_aha.items()
])

# Save to CSV
crosswalk_df.to_csv("/Users/loaner/Desktop/github-archive/mcr_aha.csv", index=False)

# this crosswalk has 22 mismatched obs / 18 mismatches
# 3 are in medicare_to_aha
# the remaining 15 are due to one medicare number having multiple aha numbers
# in a given year


# look into entity name/address match
ha_with_mcr = pd.read_csv('/Users/loaner/Desktop/github-archive/haentity_with_mcr.csv')
# ha_with_mcr = ha_raw[ha_raw['haentitytypeid'] == 1]
ha_with_mcr['clean_mcr'] = ha_with_mcr['r_medicare'].apply(clean_and_convert)

# to determine
# if ha_with_mcr medicare number is NA 
# or if ha_with_mcr medicare number is not NA and medicare,year in mcr_mismatch

mcr_year_pairs = list(zip(ha_with_mcr['clean_mcr'], ha_with_mcr['year']))
undetermined_pairs = mcr_year_pairs & mcr_mismatch.keys()

no_medicare = ha_with_mcr[ha_with_mcr['r_medicare'].isna()][[
    'entity_name', 'year', 'entity_address', 'entity_zip'
]]


## build dfs to match on address
df1 = x_walk_2_data[['address', 'ahaid_noletter', 'year','mcrnum', 'zipcode']].rename(
    columns={
        'address': 'address',
        'ahaid_noletter': 'ahaid', 
        'year': 'year',
        'mcrnum': 'mcrnum',
        'zipcode': 'zipcode'
    }
)
df1['ahaid'] = df1['ahaid'].apply(clean_and_convert)
df1['mcrnum'] = df1['mcrnum'].apply(clean_and_convert)

ref_df = df1.dropna(subset=['address', 'ahaid', 'year', 'mcrnum', 'zipcode'])

# only get cases where each mcr has one address
address_counts = (
    ha_with_mcr
    .groupby(['clean_mcr', 'year'])['entity_address']
    .nunique()
    .reset_index(name='n_address')
)
valid_keys = address_counts[address_counts['n_address'] == 1][['clean_mcr', 'year']]
filtered_df = ha_with_mcr.merge(valid_keys, on=['clean_mcr', 'year'])

mcr_to_real_address = {
    (row['clean_mcr'], row['year']): row['entity_address']
    for _, row in filtered_df.iterrows()
}
# get address match

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
        addresses = subset['address'].tolist()

        if mcrnum == 452039 and year == 2007:
            print(addresses)

        # Perform fuzzy matching
        match_result = process.extractOne(real_address, addresses, 
                                            scorer=fuzz.token_sort_ratio)

        if match_result is None:
            address_matches[(mcrnum, year)] = None
            continue

        matched_address, score, idx = match_result
        if score >= 85:
            # Get all rows in subset that match the matched_address
            matches_df = subset[subset['address'] == matched_address]
            unique_ahaids = matches_df['ahaid'].unique()

            if len(unique_ahaids) == 1:
                address_matches[(mcrnum, year)] = unique_ahaids[0]
            else:
                # Conflict: same address matches multiple different ahaids
                address_matches[(mcrnum, year)] = None
        else:
            address_matches[(mcrnum, year)] = None


final_x_walk = {}

# Process dict1
for (mcr, year), value_set in medicare_to_aha.items():
    if mcr is None:
        continue
    if isinstance(value_set, set) and len(value_set) == 1:
        final_x_walk[(mcr, year)] = float(next(iter(value_set)))

# Process dict2
for (mcr, year), value in address_matches.items():
    if mcr is None:
        continue
    if value is None:
        continue
    final_x_walk[(mcr, year)] = float(value)  # overwrites if key overlaps with dict1

# save 
crosswalk_df = pd.DataFrame([
    {'mcrnum': k[0], 'year': k[1], 'aha_xwalk': v}
    for k, v in final_x_walk.items()
])

# Save to CSV
crosswalk_df.to_csv("/Users/loaner/Desktop/github-archive/mcr_year_to_aha.csv", index=False)

# 40 mismatched pairs
# 3 are still due to the original issue
# pinnacle health confirmed
# Mercy Medical Center - Des Moines confirmed
# Parkview Orthopedic Hospital
# Kindred Hospital - Oklahoma City - confirmed
# Community Hospital - East - confirmed
# Midland Memorial Hospital - confirmed
# The Regional Medical Center of Acadiana - confirmed
# north shore confirmed
# Promise Hospital Baton Rouge - confirmed
# Kindred Hospital - Houston - confirmed
# missing - 7600 Beechnut Street - confirmed

# Desert View Regional Medical Center - 1:1 mcr to aha

# ?? Mercy West Lakes & Mercy Medical Center - West Lakes : zip codes not in 
#  i think this is because of how you're filling in medicare numbers
# , Mercy Capitol - wrong ;


# check mismatch between julia and my code
pairs = pd.read_csv("/Users/loaner/Desktop/github-archive/mismatch_pairs.csv")
tuple_list = list(zip(pairs['r_medicare'], pairs['year']))
tuple_set = set(tuple_list)

verified = tuple_set & medicare_to_aha.keys() # 3

unaccounted = tuple_set - medicare_to_aha.keys() # 37

updated = tuple_set & address_matches.keys()

not_matched = tuple_set - address_matches.keys() - medicare_to_aha.keys()
# 7 don't have an mcr to address in haentity

missing_key = 0
for mcr, year in unaccounted:
    key = mcr, year
    if key not in mcr_to_real_address:
        missing_key += 1
        print(f"Key not in dict: {key}")
    else: 
        real_address = mcr_to_real_address[(mcr, year)]
        print(real_address)

        subset = ref_df[(ref_df['mcrnum'] == mcr) & 
                                (ref_df['year'] == year)]
                

        # Get list of addresses from this subset
        addresses = subset['address'].tolist()

        print(addresses)

## next want to create a crosswalk between zip code and address with aha


from collections import defaultdict

# Ensure address is lowercase and stripped before dropping duplicates
ref_df['address_clean'] = ref_df['address'].astype(str).str.lower().str.strip()

# Drop duplicates on just the key columns and ahanumber
unique_rows = ref_df[['year', 'zipcode', 'address_clean', 'ahaid']].drop_duplicates()
unique_rows['zip_numeric'] = pd.to_numeric(unique_rows['zipcode'])

# Build the dictionary
address_map = defaultdict(set)
for _, row in unique_rows.iterrows():
    key = (row['year'], row['zip_numeric'], row['address_clean'])
    address_map[key].add(row['ahaid'])


## still unmatched
remaining = mcr_mismatch - address_matches.keys()

mult_adds = address_counts[address_counts['n_address'] > 1][['clean_mcr', 'year']]
filtered_df = ha_with_mcr.merge(mult_adds, on=['clean_mcr', 'year'])
filtered_df['zip'] = filtered_df['entity_zip'].astype(str)
filtered_df['zip'] = filtered_df['zip'].str[:5]
filtered_df['zip'] = filtered_df['zip'].str.zfill(5)
filtered_df['address_clean'] = filtered_df['entity_address'].astype(str).str.lower().str.strip()


# Flatten address_map to group addresses by (year, zip)
year_zip_to_addresses = defaultdict(list)

for (year, zip_, address), ahaids in address_map.items():
    for ahaid in ahaids:
        year_zip_to_addresses[(year, zip_)].append((address, ahaid))


match_dict = {}  # Will store (year, zip, mcrnum, row_address) -> matched_aha

for _, row in filtered_df.iterrows():
    year = row['year']
    zip = int(row['zip'])
    candidates = year_zip_to_addresses.get((year,zip), [])

    # Get just the list of addresses to match against
    candidate_addresses = [addr for addr, _ in candidates]

    if not candidate_addresses:
        continue

    # Fuzzy match the current row's address to candidate addresses
    match_result = process.extractOne(
        row['address_clean'], 
        candidate_addresses, 
        scorer=fuzz.token_sort_ratio
    )

    if match_result:
        matched_address, score, idx = match_result
        matched_aha = candidates[idx][1]

        # Check if the score is high OR if one address is a substring of the other
        if (
            score >= 85 or 
            matched_address in row['address_clean'] or 
            row['address_clean'] in matched_address
        ):
            match_dict[(row['year'], (row['zip']), row['clean_mcr'], row['address_clean'])] = matched_aha



## leftover part 2
matches = {
    (year, zipcode, mcr, add): aha
    for (year, zipcode, mcr, add), aha in match_dict.items()
    if (year == 2006 and mcr == 160083) or 
    (year == 2007 and mcr == 160083) or 
    (year == 2008 and mcr == 160083) or
    (year == 2009 and mcr == 160083) or 
    (year == 2006 and mcr == 450184) or
    (year == 2007 and mcr == 450184)
    #if aha == 6620003 or aha == 6629025
    #if mcr == 34020.0
}


# desert and mercy west lakes already good from step 1
# pinnacle, parkview, and midland are good from step 2
# mercy capitol and mercy des moines now good from step 3

# only question left is Memorial Hermann Southwest Hospital
# but need to do it systematically so you can be sure matches are right