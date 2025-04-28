
import pandas as pd
import re
from collections import defaultdict
from rapidfuzz import process, fuzz

# load data
x_walk_1_data = pd.read_stata('/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/supplemental/aha-id-medicare-id-crosswalk.dta')
x_walk_2_data = pd.read_stata('/Users/loaner/BFI Dropbox/Katherine Papen/hospital_ceos/_data/supplemental/hospital_ownership.dta')
aha_data_csv = pd.read_csv('/Users/loaner/Downloads/_data/supplemental/AHA_2004_2017.csv', encoding='latin1')

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
    
    
# create (aha,year) to medicare crosswalks
x_walk_1_aha = defaultdict(set)
for _, row in x_walk_1_data.iterrows():
    key = (clean_and_convert(row['id']), row['year'])
    x_walk_1_aha[key].add(clean_and_convert(row['pn']))

x_walk_2_aha = defaultdict(set)
for _, row in x_walk_2_data.iterrows():
    key = (clean_and_convert(row['ahaid_noletter']), row['year'])
    x_walk_2_aha[key].add(clean_and_convert(row['mcrnum']))

x_walk_3_aha = defaultdict(set)
for _, row in aha_data_csv.iterrows():
    key = (clean_and_convert(row['ID']), row['YEAR'])
    x_walk_3_aha[key].add(clean_and_convert(row['MCRNUM']))

# create master
aha = defaultdict(set)

for d in [x_walk_1_aha, x_walk_2_aha, x_walk_3_aha]:
    for key, values in d.items():
        for v in list(values):
            try:
                number = clean_and_convert(v)
                if number is not None:
                    aha[key].add(number)
            except:
                continue

aha_to_medicare = {k: v for k, v in aha.items() if len(v) == 1}
# converting it from string to integer reduces from 408k to 125 k ??
mismatch = {k: v for k, v in aha.items() if len(v) > 1} # 290

# create (medicare,year) to aha crosswalks
x_walk_1_mcr = defaultdict(list)
for _, row in x_walk_1_data.iterrows():
    key = (clean_and_convert(row['pn']), row['year'])
    x_walk_1_mcr[key].append(clean_and_convert(row['id']))

x_walk_2_mcr = defaultdict(list)
for _, row in x_walk_2_data.iterrows():
    key = (clean_and_convert(row['mcrnum']), row['year'])
    x_walk_2_mcr[key].append(clean_and_convert(row['ahaid_noletter']))

x_walk_3_mcr = defaultdict(list)
for _, row in aha_data_csv.iterrows():
    key = (hlp.clean_and_convert(row['MCRNUM']), row['YEAR'])
    x_walk_3_mcr[key].append(hlp.clean_and_convert(row['ID']))

# create master
mcr = defaultdict(set)
    
for d in [x_walk_1_mcr, x_walk_2_mcr, x_walk_3_mcr]:
    for key, values in d.items():
        for v in values:
            try:
                number = clean_and_convert(v)
                if number is not None:
                    mcr[key].add(number)
            except:
                continue

medicare_to_aha = {k: v for k, v in mcr.items() if len(v) == 1}
mcr_mismatch = {k: v for k, v in mcr.items() if len(v) > 1} # 475

# xwalks with 1:1 mappings between aha & medicare
master_aha_xwalk = {}
master_mcr_walk = {}
for (aha, year), medicare_set in aha_to_medicare.items():
    medicare = clean_and_convert(next(iter(medicare_set)))  # extract the only item in the set
    if (medicare, year) in medicare_to_aha:
        reverse_aha_set = medicare_to_aha[(medicare, year)]
        reverse_aha = next(iter(reverse_aha_set))
        
        if reverse_aha == clean_and_convert(aha):
            master_aha_xwalk[(aha, year)] = medicare
            master_mcr_walk[(medicare,year)] = aha

# investigate 1:m aha:medicare
# check xwalk 2 name, address and xwalk3 MNAME, MLOCADDR

# x_walk_2_data[
    #((x_walk_2_data['mcrnum']).apply(clean_and_convert) == 291311) &
   # (x_walk_2_data['year'] == 2011)][['name', 'address', 'mcrnum', 'ahaid_noletter']]

haentity = pd.read_feather('/Users/loaner/Desktop/github-archive/haentity2.feather')
ha_with_mcr = pd.read_csv('/Users/loaner/Desktop/github-archive/haentity_with_mcr.csv')

cleaned_mcr = ha_with_mcr['r_medicare'].apply(clean_and_convert)
ha_set = set(zip(cleaned_mcr, ha_with_mcr['year']))

true_mcr_mismatch = {k: v for k, v in mcr_mismatch.items() if k in ha_set}

# unaccounted
unaccounted = ha_set - medicare_to_aha.keys() - aha_matches.keys()


ha_mcr = set(ha_with_mcr['r_medicare'])

for (mcr, year), aha_set in mcr_mismatch.items():
    if mcr in ha_mcr:
        matched_rows = ha_with_mcr[(ha_with_mcr['r_medicare'] == mcr) & 
                                   (ha_with_mcr['year'] == year)]
        ha_names = matched_rows['entity_name'].dropna().unique().tolist()
        ha_adds = matched_rows['entity_address'].dropna().unique().tolist()


## MATCH ON ADDRESS
df1 = x_walk_2_data[['address', 'ahaid_noletter', 'year','mcrnum']].rename(
    columns={
        'address': 'address',
        'ahaid_noletter': 'ahaid', 
        'year': 'year',
        'mcrnum': 'mcrnum'
    }
)
df1['ahaid'] = df1['ahaid'].apply(clean_and_convert)
df1['mcrnum'] = df1['mcrnum'].apply(clean_and_convert)

df2 = aha_data_csv[['MLOCADDR', 'ID', 'YEAR', "MCRNUM", "MNAME", "MLOCZIP"]].rename(
    columns={
        'MLOCADDR': 'address',
        'ID': 'ahaid', 
        'YEAR': 'year',
        "MCRNUM": 'mcrnum',
        "MNAME": "name",
        "MLOCZIP": 'zipcode'
    }
)

df2['ahaid'] = df2['ahaid'].apply(clean_and_convert)
df2['mcrnum'] = df2['mcrnum'].apply(clean_and_convert)

ref_df = pd.concat([df1, df2], ignore_index=True)
ref_df = ref_df.dropna(subset=['address', 'ahaid', 'year', 'mcrnum'])


mcr_to_real_address = {
    (clean_and_convert(row['r_medicare']), row['year']): row['entity_address']
    for _, row in ha_with_mcr.iterrows()
}


aha_matches = {}

not_in_real_address = 0
# Loop through your dictionary
for (mcrnum, year) in mcr_mismatch:

    key = (mcrnum, year)
    if key in mcr_to_real_address:
        real_address = mcr_to_real_address[(mcrnum, year)]

        # Filter ref_df for the same (mcrnum, year)
        subset = ref_df[(ref_df['mcrnum'] == mcrnum) & 
                            (ref_df['year'] == year)]
            
        if subset.empty:
            aha_matches[(mcrnum, year)] = None
            continue

            # Get list of addresses from this subset
        addresses = subset['address'].tolist()


            # Perform fuzzy matching
        match_result = process.extractOne(real_address, addresses, 
                                            scorer=fuzz.token_sort_ratio)

        if match_result is None:
            aha_matches[(mcrnum, year)] = None
            continue

        matched_address, score, idx = match_result
        matched_row = subset.iloc[idx]
        matched_ahaid = matched_row['ahaid']

            # Save the best match
        aha_matches[(mcrnum, year)] = matched_ahaid

    else: 
        not_in_real_address += 1








ref_dict = defaultdict(list)
for _, row in ref_df.iterrows():
    key = (clean_and_convert(row['mcrnum']), row['year'])
    ref_dict[key].append((row['address'], clean_and_convert(row['ahaid'])))

def get_best_aha_match_dict(real_address, year, mcrnum, ref_dict):
    key = (mcrnum, year)

    if key not in ref_dict:
        return None, None, None

    address_list = [addr for addr, _ in ref_dict[key]]
    result = process.extractOne(real_address, address_list, scorer=fuzz.token_sort_ratio)

    if result is None:
        return None, None, None

    match_address, score, idx = result
    matched_ahaid = ref_dict[key][idx][1]  # get the ahaid from original tuple

    return matched_ahaid, match_address, score

def get_best_aha_match(real_address, year, medicare_number, reference_df):
    filtered = reference_df[
        (reference_df['year'] == year) &
        (reference_df['mcrnum'] == medicare_number)
    ]

    if filtered.empty:
        return None, None, None

    # Perform matching
    result = process.extractOne(
        real_address,
        filtered['address'],
        scorer=fuzz.token_sort_ratio
    )

    # Check if a result was returned
    if result is None:
        return None, None, None

    match, score, idx = result

    # Double-check that idx is safe
    if idx >= len(filtered):
        return None, None, None

    matched_row = filtered.iloc[idx]
    return matched_row['ahaid'], match, score

# Apply row-wise to ha_with_mcr
ha_with_mcr[['matched_ahaid', 'matched_address', 'match_score']] = ha_with_mcr.apply(
    lambda row: pd.Series(get_best_aha_match_dict(
        row['entity_address'],
        row['year'],
        row['r_medicare'],
        ref_dict
    )),
    axis=1
)

df