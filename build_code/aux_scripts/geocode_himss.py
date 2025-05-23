
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

address_input = os.path.join(data_dir, "derived/auxiliary/address_unmatched.pkl")
with open(address_input, "rb") as f:
    address_zip_set = pickle.load(f)

missing_census_path = '/Users/loaner/Desktop/github-archive/census_r2.csv'
missing_add_df = pd.read_csv(missing_census_path, header = None)
missing_add_df['address_clean'] = missing_add_df[1].\
    astype(str).str.lower().str.strip()
missing_add_set = set(
    zip(
        missing_add_df['address_clean'],missing_add_df[4]
    )
)

address_output = os.path.join(data_dir, "derived/auxiliary/geocoded_addresses_census.csv")

if os.path.exists(address_output):
    cache_df = pd.read_csv(address_output)

    # Add timed_out column if it's missing
    if "timed_out" not in cache_df.columns:
        cache_df["timed_out"] = False  # assume all previous rows were successful
        cache_df.to_csv(address_output, index=False)  # overwrite with column added
else:
    cache_df = pd.DataFrame(columns=["address", "zip", "latitude", "longitude", "timed_out"])

## set up geocoding
to_geocode = pd.DataFrame(address_zip_set & missing_add_set, columns=["address", "zip"])
to_geocode["zip"] = pd.to_numeric(to_geocode["zip"], errors="coerce")

to_query = to_geocode.merge(cache_df, on=["address", "zip"], 
                            how="left", indicator=True)
to_query = to_query[to_query["_merge"] == "left_only"][["address", "zip"]]

ssl_context = ssl.create_default_context(cafile=certifi.where())
geolocator = Nominatim(user_agent="hospital_locator", ssl_context=ssl_context)

# Geocode with retry + append to CSV
for _, row in tqdm(to_query.iterrows(), total=to_query.shape[0], desc="Geocoding"):
    address = row["address"]
    zip_code = row["zip"]
    full_address = f"{address}, {zip_code}"

    try:
        location = geolocator.geocode(full_address, timeout=20)
        print(location)
        timed_out = False
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
