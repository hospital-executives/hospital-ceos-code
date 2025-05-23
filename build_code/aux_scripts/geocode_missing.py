
import pandas as pd
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import os
import pickle
from tqdm import tqdm
import ssl
import certifi

## set up paths
data_dir = "/Users/loaner/Dropbox/hospital_ceos/_data"
missing_census_path = '/Users/loaner/Desktop/github-archive/census_r2.csv'
# Load the input
missing_add_df = pd.read_csv(missing_census_path, header=None)
missing_add_df['address_clean'] = missing_add_df[1].astype(str).str.lower().str.strip()

# Create current address-zip set
missing_add_df['zip'] = missing_add_df[4]
missing_add_df['address_zip'] = list(zip(missing_add_df['address_clean'], missing_add_df['zip']))

missing_add_set = set(missing_add_df['address_zip'])

# Path to geocode cache
address_output = os.path.join(data_dir, "derived/auxiliary/geocoded_addresses_census.csv")

# Load existing geocoded cache
if os.path.exists(address_output):
    cache_df = pd.read_csv(address_output)

    # Normalize existing cache for comparison
    cache_df['address_clean'] = cache_df['0'].astype(str).str.lower().str.strip()
    cache_df['address_zip'] = list(zip(cache_df['address_clean'], cache_df['1']))

    # Add timed_out column if missing
    if "timed_out" not in cache_df.columns:
        cache_df["timed_out"] = False
        cache_df.to_csv(address_output, index=False)

    cached_set = set(cache_df['address_zip'])
else:
    cache_df = pd.DataFrame(columns=["address", "zip", "latitude", "longitude", "timed_out"])
    cached_set = set()

# Filter only addresses not already geocoded
new_to_geocode = missing_add_df[~missing_add_df['address_zip'].isin(cached_set)].copy()

# Optional cleanup
new_to_geocode = new_to_geocode.reset_index(drop=True)

ssl_context = ssl.create_default_context(cafile=certifi.where())
geolocator = Nominatim(user_agent="hospital_locator", ssl_context=ssl_context)

# Geocode with retry + append to CSV
for _, row in tqdm(new_to_geocode.iterrows(), total=new_to_geocode.shape[0], desc="Geocoding"):
    address = row["address_clean"]
    city = row[2]
    state = row[3]
    zip_code = row[4]

    # First try: full address including ZIP
    full_address = f"{address}, {city}, {state} {zip_code}"
    try:
        location = geolocator.geocode(full_address, timeout=20)
        #print(f"1: {location}")
        timed_out = False
    except GeocoderTimedOut:
        location = None
        timed_out = True

    # Second try: remove ZIP if first attempt failed (not timed out)
    if location is None and not timed_out:
        fallback_address = f"{address}, {city}, {state}"
        try:
            location = geolocator.geocode(fallback_address, timeout=20)
            #print(f"2: {location}")
        except GeocoderTimedOut:
            location = None
            timed_out = True

    lat = location.latitude if location else None
    lon = location.longitude if location else None

    result_row = pd.DataFrame([{
        "address": address,
        "zip": zip_code,
        "latitude": lat,
        "longitude": lon,
        "timed_out": timed_out
    }])

    result_row.to_csv(address_output, mode="a", header=False, index=False)
    time.sleep(1)
