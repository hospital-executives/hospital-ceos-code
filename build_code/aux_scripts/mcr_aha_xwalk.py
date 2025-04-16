
import re
import pandas as pd
from collections import defaultdict
from rapidfuzz import process, fuzz

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

## load data
haentity = pd.read_feather("/Users/loaner/Desktop/github-archive/haentity2.feather")
haentity['mcrnum'] = haentity['medicarenumber'].apply(clean_and_convert)

x_walk_2_data = pd.read_stata('/Users/loaner/Downloads/_data/supplemental/hospital_ownership.dta')

# basic medicare to aha crosswalk 
x_walk_2_mcr = defaultdict(set)
for _, row in x_walk_2_data.iterrows():
    key = (clean_and_convert(row['mcrnum']), row['year'])
    x_walk_2_mcr[key].add(clean_and_convert(row['ahaid_noletter']))
medicare_to_aha = {k: v for k, v in x_walk_2_mcr.items() if len(v) == 1}
mcr_mismatch = {k: v for k, v in x_walk_2_mcr.items() if len(v) > 1}

# dfs to match on address
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
df1['zipcode'] = df1['zipcode'].astype(str)
df1['zipcode'] = df1['zipcode'].str[:5]
df1['zipcode'] = df1['zipcode'].str.zfill(5)

ref_df = df1.dropna(subset=['address', 'ahaid', 'year', 'mcrnum', 'zipcode'])

# only get cases where each mcr has one address
address_counts = (
    haentity
    .groupby(['mcrnum', 'year'])['entity_address']
    .nunique()
    .reset_index(name='n_address')
)
valid_keys = address_counts[address_counts['n_address'] == 1][['mcrnum', 'year']]

filtered_df = haentity.merge(valid_keys, on=['mcrnum', 'year'], how='inner')

mcr_to_real_address = {
    (row['mcrnum'], int(row['year'])): row['entity_address']
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
        addresses = subset['address'].tolist()

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

# address : aha match prep
df1['address_clean'] = df1['address'].astype(str).str.lower().str.strip()
unique_rows = df1[['year', 'zipcode', 'address_clean', 'ahaid']].drop_duplicates()
#unique_rows['zip_numeric'] = pd.to_numeric(unique_rows['zipcode'])
address_map = defaultdict(set)
for _, row in unique_rows.iterrows():
    key = (row['year'], row['zipcode'], row['address_clean'])
    address_map[key].add(row['ahaid'])

year_zip_to_addresses = defaultdict(list)
for (year, zip_, address), ahaids in address_map.items():
    for ahaid in ahaids:
        year_zip_to_addresses[(year, zip_)].append((address, ahaid))


## begin match process

# format data for matching
hospitals = haentity[haentity['haentitytypeid'] == "1"]
hospitals['cleaned_mcr'] = hospitals['medicarenumber'].apply(clean_and_convert)
hospitals['year'] = hospitals['year'].astype(int)
hospitals['address_clean'] = hospitals['entity_address'].astype(str).str.lower().str.strip()
hospitals['zip'] = hospitals['entity_zip'].astype(str)
hospitals['zip'] = hospitals['zip'].str[:5]
hospitals['zip'] = hospitals['zip'].str.zfill(5)

# match on mcr:aha and mcr:1 address
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

# get 

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
            match_with_mcr[(row['year'], (row['zip']), row['mcrnum'], 
                        row['address_clean'])] = matched_aha
            match_wo_mcr[(row['year'], (row['zip']), 
                        row['address_clean'])] = matched_aha


# 3733 hospitals unaccounted for after medicare_to_aha
# 3653 unaccounted after address matches
# 3607 after address round 2 

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

## 8485 obs missing AHA of which 2220 have valid medicare numbers

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

hospitals.to_csv('/Users/loaner/Desktop/github-archive/py_aha.csv')



## check unmatched medicare numbers
unmatched_mcr = set(hospitals[hospitals['filled_aha'].isna()]['cleaned_mcr'].dropna())

in_crosswalk = unmatched_mcr & set(df1['mcrnum']) 
# 749 mcrnum are in crosswalk, 105 re not


matches = {
    (year, zipcode, add): aha
    for (year, zipcode, add), aha in address_map.items()
    if  (zip == "76104")
        }
print(matches)


## look into na hospitals
na = hospitals[hospitals['filled_aha'].isna()]
na['year'].value_counts().sort_index()
