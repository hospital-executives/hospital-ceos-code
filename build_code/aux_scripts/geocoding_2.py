import pandas as pd
import numpy as np
## coordinate geocoding

## goal - if HIMSS AHA number is missing get lat/long

## create df with  
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
census_df = pd.DataFrame(list(census), columns=["address_clean", "entity_city", "entity_state", "zip"])

# Add a unique ID as the first column
census_df.insert(0, "id", range(1, len(census_df) + 1))

import math

chunk_size = 10_000
num_chunks = math.ceil(len(census_df) / chunk_size)

for i in range(num_chunks):
    start = i * chunk_size
    end = start + chunk_size
    chunk = census_df.iloc[start:end]
    
    chunk.to_csv(f"/Users/loaner/Desktop/github-archive/census_chunk_{i+1}.csv", index=False, header=False)


dfs = []

for i in range(1, 6):
    df = pd.read_csv(
    f"/Users/loaner/Dropbox/hospital_ceos/_data/supplemental/census_adds/census_results_{i}.csv",
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

remaining = combined_census[~((combined_census[2] == "Match")  &
                          ((combined_census[3] == "Exact") | 
                          (combined_census[3] == "Non_Exact")))]

export = census_df[census_df['id'].isin(remaining[0])]
#export.to_csv(f"/Users/loaner/Desktop/github-archive/census_r2.csv",
                #  index=False, header=False)

## create address to lat/long merge
df_full_renamed = cleaned.rename(columns={0: "id", 5: "latitude", 6: "longitude"})

df_merged = census_df.merge(df_full_renamed[['id', 'latitude', 'longitude']],
                             on='id', how='left')

# Step 1: Merge lat/lon from df_merged
hospitals_merged = hospitals.merge(
    df_merged[['address_clean', 'entity_city', 'entity_state', 'zip', 'latitude', 'longitude']],
    on=['address_clean', 'entity_city', 'entity_state', 'zip'],
    how='left',
    suffixes=('', '_new')  # prevent overwriting existing lat/lon columns immediately
)

# Step 2: Fill in only where current lat/lon is missing
hospitals_merged['latitude'] = hospitals_merged['latitude'].combine_first(hospitals_merged['latitude_new'])
hospitals_merged['longitude'] = hospitals_merged['longitude'].combine_first(hospitals_merged['longitude_new'])

# Step 3: Drop temporary columns
hospitals_merged = hospitals_merged.drop(columns=['latitude_new', 'longitude_new'])

lat_dict = {
        (row['address_clean'], str(row['zip'])): row['latitude']
        for _, row in hospitals_merged.iterrows()
    }
lon_dict = {
        (row['address_clean'], str(row['zip'])): row['longitude']
        for _, row in hospitals_merged.iterrows()
    }

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
ref_df['ahaid'] = ref_df['ahaid'].apply(hlp.clean_and_convert)

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