
import pandas as pd
import os
import sys
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import zipfile
from pathlib import Path
import requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from helper_scripts import xwalk_helper as hlp

cli_args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]

if cli_args:
    data_path = cli_args[0]
else: 
    data_path = "/Users/katherinepapen/Library/CloudStorage/Dropbox/hospital_ceos/_data"
    print('WARNING: not using Makefile inputs')

input_df = pd.read_csv(os.path.join(data_path, "derived/auxiliary/aha_himss_xwalk.csv"))
output_path = os.path.join(data_path,  "derived/auxiliary/aha_himss_xwalk_msa.csv")
shapefile_folder = os.path.join(data_path, "supplemental/cbsa_shapefiles")

# get distinct coordinates
distinct_coords = input_df[['latitude', 'longitude', 'year']].drop_duplicates()

# unzip shape files to temp
temp_dir = Path.cwd() / "temp"
temp_dir.mkdir(parents=True, exist_ok=True)

# Loop through all ZIP files
for zip_path in Path(shapefile_folder).glob("*.zip"):
    extract_to = temp_dir / zip_path.stem  # subfolder named after the ZIP
    extract_to.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_to)


def find_cbsa_shapefile(year:int, base_dir:Path) -> Path:
    """
    Find tl_{year}_us_cbsa.shp somewhere under base_dir (recursively).
    Returns a Path or None if not found.
    """
    patterns = [
        f"**/tl_{year}_us_cbsa*.shp",
        f"**/fe_{year}_us_cbsa*.shp"
    ]

    for pat in patterns:
        matches = list(base_dir.glob(pat))
        if matches:
            return matches[0]  # return the first found
    return None

CBSA_COLS_PREF = ["CBSAFP", "CBSAFP10", "GEOID", "GEOID10", "NAME", "NAME10"]

results = []
for yr in distinct_coords['year'].unique():
    # Use 2007 shapefile for all years ≤ 2007
    shapefile_year = 2007 if yr <= 2007 else yr

    shp = find_cbsa_shapefile(int(shapefile_year), Path("temp"))
    if shp is None:
        raise ValueError(f"No shapefile {shapefile_year}_us_cbsa.shp found")

    pts_df = distinct_coords.loc[distinct_coords["year"].eq(yr), ["latitude", "longitude", "year"]].dropna()
    if pts_df.empty:
        raise ValueError(f"No points found for year {yr} in distinct_coords.")

    gdf_poly = gpd.read_file(shp)
    keep_cols = [c for c in CBSA_COLS_PREF if c in gdf_poly.columns]
    gdf_poly = gdf_poly[keep_cols + ["geometry"]]

    gdf_pts = gpd.GeoDataFrame(
        pts_df.copy(),
        geometry=gpd.points_from_xy(pts_df["longitude"], pts_df["latitude"]),
        crs="EPSG:4326"
    )

    if gdf_poly.crs and gdf_poly.crs != gdf_pts.crs:
        gdf_pts = gdf_pts.to_crs(gdf_poly.crs)

    joined = gpd.sjoin(gdf_pts, gdf_poly, how="left", predicate="intersects").drop(columns=["index_right"], errors="ignore")

    results.append(joined)

    if results:
        pts_with_cbsa = pd.concat(results, ignore_index=True)
        # (optional) bring everything back to WGS84 for consistency downstream
        if isinstance(pts_with_cbsa, gpd.GeoDataFrame) and pts_with_cbsa.crs and pts_with_cbsa.crs.to_string() != "EPSG:4326":
            pts_with_cbsa = pts_with_cbsa.to_crs(4326)
    else:
        pts_with_cbsa = gpd.GeoDataFrame(columns=["latitude","longitude","year","geometry"], crs="EPSG:4326")

out_cols = ["latitude","longitude","year"] + [c for c in CBSA_COLS_PREF if c in pts_with_cbsa.columns]
final_shape_df = pts_with_cbsa[out_cols]

# merged df
merged_df = pd.merge(input_df, final_shape_df, how = "left", on = ["latitude", 'longitude', 'year'])
merged_df["cbsa_fips"]  = merged_df["CBSAFP"].combine_first(merged_df["CBSAFP10"])
merged_df["cbsa_geoid"] = merged_df["GEOID"].combine_first(merged_df["GEOID10"])
merged_df["cbsa_name"]  = merged_df["NAME"].combine_first(merged_df["NAME10"])
merged_df = merged_df.drop(columns=["CBSAFP","CBSAFP10","GEOID","GEOID10","NAME","NAME10"], errors="ignore")

#### add zip to ruca xwalk ####
## helper function to standardize zips
def clean_zip(series):
    # Convert to pandas string dtype (handles NaN nicely)
    s = series.astype("string")

    # Strip spaces
    s = s.str.strip()

    # Keep only digits
    s = s.str.replace(r"[^\d]", "", regex=True)

    # If there are more than 5 digits (e.g., 9-digit zips or "010101"), take first 5
    s = s.str[:5]

    # Turn empty strings back into NA
    s = s.replace("", pd.NA)

    # Zero-pad to 5 digits where not NA
    s = s.where(s.isna(), s.str.zfill(5))

    return s

## download xwalk
url = "https://ers.usda.gov/sites/default/files/_laserfiche/DataFiles/53241/RUCA2010zipcode.csv?v=14791"   # replace with your download link
ruca_xwalk = pd.read_csv(url)

## format xwalk for merge
ruca_xwalk.columns = ruca_xwalk.columns.str.replace("'", "")
ruca_mini = ruca_xwalk[['ZIP_CODE', "RUCA1", "RUCA2", "STATE"]]

ruca_mini["zip_key"] = clean_zip(ruca_mini["ZIP_CODE"])
merged_df["zip_key"] = clean_zip(merged_df["entity_zip"])

# there are cases where:
# a) there is no zip xwalk or 
# b) cases where state from xwalk doesnt line up (mostly due to borders)
# but these are rare, particularly for our primary sample
final = merged_df.merge(
    ruca_mini[["RUCA1", "RUCA2", "zip_key"]],
    how="left",
    left_on="zip_key",
    right_on="zip_key"
)

final.to_csv(output_path)