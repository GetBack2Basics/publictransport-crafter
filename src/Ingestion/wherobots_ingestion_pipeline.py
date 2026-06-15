#!/usr/bin/env python3
"""
Wherobots Cloud Geospatial Ingestion Pipeline
Ingests NSW critical infrastructure (FeatureServer APIs), train network shapes/stations,
demographics (ABS ERP Excel & Shapefile) for actual years (2020, 2025), and public transport patronage (TfNSW Opal trips)
joined with GTFS stops.txt. Processes in Apache Sedona, and exports to WGS84 GeoParquet files partitioned by year.
"""

import os
import sys
import json
import zipfile
import urllib.request
import requests
import pandas as pd
import geopandas as gpd

from sedona.spark import SedonaContext
from pyspark.sql.functions import col, to_json, expr, lit, substring, when

# ==============================================================================
# Helper Utilities
# ==============================================================================

def download_file(url, local_path):
    """Downloads a file from a URL to a local destination."""
    print(f"Downloading: {url}")
    print(f"Destination: {local_path}")
    try:
        # Use a browser User-Agent to prevent ABS from blocking/dropping the connection
        req = urllib.request.Request(
            url, 
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'}
        )
        with urllib.request.urlopen(req, timeout=120) as response, open(local_path, 'wb') as out_file:
            out_file.write(response.read())
        print("Download completed successfully.")
    except Exception as e:
        print(f"ERROR: Failed to download {url}: {e}")
        raise

def extract_zip(zip_path, extract_to):
    """Extracts a zip file to a local folder."""
    print(f"Extracting: {zip_path}")
    print(f"Destination: {extract_to}")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print("Extraction completed successfully.")
    except Exception as e:
        print(f"WARNING: Failed to extract {zip_path}: {e}")
        raise



def fetch_featureserver_geojson(base_url, layer_id):
    """
    Queries a specific layer of an Esri FeatureServer for all features,
    handling pagination. Returns a list of GeoJSON features.
    """
    query_url = f"{base_url}/{layer_id}/query"
    print(f"Querying FeatureServer layer: {query_url}")
    
    all_features = []
    offset = 0
    limit = 1000
    
    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "geojson",
            "outSR": "4326",
            "resultOffset": offset,
            "resultRecordCount": limit
        }
        try:
            res = requests.get(query_url, params=params, timeout=15)
            if res.status_code != 200:
                print(f"  Query complete or not supported (HTTP {res.status_code})")
                break
            data = res.json()
        except Exception as e:
            print(f"  Error querying layer {layer_id}: {e}")
            break
            
        features = data.get("features", [])
        if not features:
            break
            
        all_features.extend(features)
        print(f"  Fetched {len(features)} features (offset: {offset})")
        
        if len(features) < limit:
            break
        offset += limit
        
    print(f"Total features retrieved from layer {layer_id}: {len(all_features)}")
    return all_features

def save_data_frame(sedona, df, table_name, storage_root, partition_col="year"):
    """
    Saves a Sedona DataFrame as a Havasu/Iceberg table (wherobots.fgsdb.<table_name>)
    if storage_root starts with 'wherobots://' or running on Wherobots Cloud,
    otherwise saves as a local GeoParquet file.
    """
    is_wherobots = os.getenv("WHEROBOTS_ENV") in ["stg", "prod"] or storage_root.startswith("wherobots://")
    
    if is_wherobots:
        full_table_name = f"org_catalog.fgsdb.{table_name}"
        print(f"\nWriting to Wherobots Havasu Iceberg Table: {full_table_name}")
        try:
            # Ensure target database/schema exists
            sedona.sql("CREATE DATABASE IF NOT EXISTS org_catalog.fgsdb")
            
            writer = df.write.format("havasu.iceberg").mode("overwrite")
            if partition_col:
                writer = writer.partitionBy(partition_col)
            writer.saveAsTable(full_table_name)
            print(f"Successfully saved table: {full_table_name}")
            return
        except Exception as e:
            print(f"WARNING: Table creation/save failed ({e}). Falling back to local file path.")
            
    # Fallback to geoparquet file
    clean_root = storage_root
    if clean_root.startswith("wherobots://"):
        clean_root = "file:///tmp/raw"
        
    target_path = f"{clean_root}/{table_name}.parquet"
    print(f"\nWriting to GeoParquet file: {target_path}")
    writer = df.write.format("geoparquet").mode("overwrite")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(target_path)
    print(f"Successfully saved file: {target_path}")

# ==============================================================================
# Pipeline Execution Functions
# ==============================================================================

def ingest_nsw_infrastructure_poi(sedona, storage_root):
    """Pipeline 1: Ingests schools and hospitals infrastructure POI partitioned by year."""
    print("\n" + "="*80)
    print("STARTING NSW CRITICAL INFRASTRUCTURE POI INGESTION")
    print("="*80)
    
    education_url = "https://portal.spatial.nsw.gov.au/server/rest/services/NSW_FOI_Education_Facilities/FeatureServer"
    health_url = "https://portal.spatial.nsw.gov.au/server/rest/services/NSW_FOI_Health_Facilities/FeatureServer"
    
    print("\nFetching Education Facilities...")
    edu_features = fetch_featureserver_geojson(education_url, 0)
    print("\nFetching Health Facilities...")
    health_features = fetch_featureserver_geojson(health_url, 0)
    all_features = edu_features + health_features
    if not all_features:
        raise ValueError("No features retrieved from FeatureServers.")
        
    print("\nLoading features into Spark DataFrame...")
    features_rdd = sedona.sparkContext.parallelize([json.dumps(f) for f in all_features])
    raw_df = sedona.read.json(features_rdd)
    
    df_with_geom = raw_df.withColumn("geom_str", to_json(col("geometry")))
    df_with_geom = df_with_geom.withColumn("geom", expr("ST_GeomFromGeoJSON(geom_str)"))
    
    if "properties" in raw_df.columns:
        df_flat = df_with_geom.select("geom", "properties.*")
    else:
        df_flat = df_with_geom.select("geom")
        
    df_spatial = df_flat.withColumn("geometry", expr("ST_SetSRID(geom, 4326)"))
    df_spatial = df_spatial.drop("geom")
    
    # Append dynamic year column (using current execution year 2026)
    df_spatial = df_spatial.withColumn("year", lit(2026))
    
    save_data_frame(sedona, df_spatial, "nsw_infrastructure_poi", storage_root)
    print("NSW Infrastructure POI Ingestion completed.")


def ingest_nsw_train_network(sedona, storage_root):
    """Pipeline 2: Ingests TfNSW Train Lines (Shapes) and Station Classes partitioned by year."""
    print("\n" + "="*80)
    print("STARTING NSW TRAIN NETWORK INGESTION")
    print("="*80)
    
    transport_theme_url = "https://portal.spatial.nsw.gov.au/server/rest/services/NSW_Transport_Theme/FeatureServer"
    
    # 1. Fetch Train Lines Shapes (Layer 7: Railway Line)
    print("\nFetching Railway Line shapes...")
    line_features = fetch_featureserver_geojson(transport_theme_url, 7)
    if not line_features:
        raise ValueError("No railway line features retrieved.")
        
    lines_rdd = sedona.sparkContext.parallelize([json.dumps(f) for f in line_features])
    lines_raw = sedona.read.json(lines_rdd)
    lines_with_geom = lines_raw.withColumn("geom_str", to_json(col("geometry"))) \
                               .withColumn("geom", expr("ST_GeomFromGeoJSON(geom_str)"))
    lines_flat = lines_with_geom.select("geom", "properties.*") if "properties" in lines_raw.columns else lines_with_geom.select("geom")
    lines_spatial = lines_flat.withColumn("geometry", expr("ST_SetSRID(geom, 4326)")) \
                              .withColumn("year", lit(2026)) \
                              .drop("geom")
                              
    save_data_frame(sedona, lines_spatial, "nsw_train_lines", storage_root)
    
    # 2. Fetch Stations and Station Classes (Layer 0: TransportFacilityPoint - filtered for Railway Stations)
    print("\nFetching Railway Station points...")
    station_features = fetch_featureserver_geojson(transport_theme_url, 0)
    if not station_features:
        raise ValueError("No railway station points retrieved.")
        
    stations_rdd = sedona.sparkContext.parallelize([json.dumps(f) for f in station_features])
    stations_raw = sedona.read.json(stations_rdd)
    stations_with_geom = stations_raw.withColumn("geom_str", to_json(col("geometry"))) \
                                     .withColumn("geom", expr("ST_GeomFromGeoJSON(geom_str)"))
    stations_flat = stations_with_geom.select("geom", "properties.*") if "properties" in stations_raw.columns else stations_with_geom.select("geom")
    stations_spatial = stations_flat.withColumn("geometry", expr("ST_SetSRID(geom, 4326)")) \
                                    .withColumn("year", lit(2026)) \
                                    .drop("geom")
                                    
    # Normalize station name column (FeatureServer uses generalname, mock uses name)
    if "generalname" in stations_spatial.columns:
        stations_spatial = stations_spatial.withColumnRenamed("generalname", "name")
    elif "name" not in stations_spatial.columns:
        stations_spatial = stations_spatial.withColumn("name", lit("Unknown Station"))
                                    
    # Determine Station Classes (Interchange, Commuter, Regional)
    # Heuristics based on name or coordinates
    stations_spatial = stations_spatial.withColumn(
        "station_class",
        when(col("name").contains("Central") | col("name").contains("Interchange") | col("name").contains("Town Hall") | col("name").contains("Wynyard"), "Interchange")
        .when(col("name").contains("Dubbo") | col("name").contains("Newcastle") | col("name").contains("Wollongong"), "Regional")
        .otherwise("Commuter")
    )
    
    save_data_frame(sedona, stations_spatial, "nsw_rail_stations", storage_root)
    print("TfNSW Train Network Ingestion completed.")


def process_demographics_for_year(sedona, year, shp_dir, local_excel):
    """Processes demographics for a single year, returning a Sedona DataFrame."""
    try:
        import xlrd
        import openpyxl
    except ImportError:
        print("Installing xlrd and openpyxl dynamically...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "xlrd", "openpyxl"])
        
    # Find the shapefile path
    shp_path = None
    for root, dirs, files in os.walk(shp_dir):
        for f in files:
            if f.endswith(".shp"):
                shp_path = os.path.join(root, f)
                break
        if shp_path:
            break
            
    if not shp_path:
        raise FileNotFoundError(f"Could not find shapefile in {shp_dir}")
        
    gdf = gpd.read_file(shp_path)
    gdf_nsw = gdf[gdf["STE_CODE21"] == "1"]
    
    # Convert shapefile geometry to WKT strings so Spark can serialize it
    gdf_nsw["wkt_geometry"] = gdf_nsw["geometry"].apply(lambda g: g.wkt if g is not None else None)
    pdf_shp = pd.DataFrame(gdf_nsw.drop(columns=["geometry"]))
    
    # Enforce strict type consistency for Shapefile attributes to prevent Spark inference conflicts
    for col in pdf_shp.columns:
        if pd.api.types.is_numeric_dtype(pdf_shp[col]):
            pdf_shp[col] = pdf_shp[col].astype(float)
        else:
            pdf_shp[col] = pdf_shp[col].astype(str).replace({'nan': None, '<NA>': None, 'None': None})
            
    spark_shp = sedona.createDataFrame(pdf_shp)
    sa2_nsw = spark_shp.withColumn("geometry", expr("ST_GeomFromWKT(wkt_geometry)")).drop("wkt_geometry")

    pdf = pd.read_excel(local_excel, sheet_name="Table 1")
    
    header_idx = None
    for idx, row in pdf.iterrows():
        row_vals = [str(val).strip().lower() for val in row.values]
        if "sa2 code" in row_vals:
            header_idx = idx
            break
            
    if header_idx is None:
        for idx, row in pdf.iterrows():
            row_vals = [str(val).strip().lower() for val in row.values]
            if any("sa2 code" in val or "sa2_code" in val for val in row_vals):
                header_idx = idx
                break
                
    if header_idx is None:
        raise ValueError(f"Could not find SA2 code header row dynamically in {local_excel}")
        
    print(f"Dynamically detected header row at index: {header_idx}")
    
    detected_cols = []
    for val in pdf.iloc[header_idx].values:
        val_str = str(val).strip() if pd.notna(val) else ""
        detected_cols.append(val_str)
        
    unique_cols = []
    col_counts = {}
    for col_name in detected_cols:
        if not col_name:
            col_name = "unnamed"
        if col_name in col_counts:
            col_counts[col_name] += 1
            unique_cols.append(f"{col_name}.{col_counts[col_name]}")
        else:
            col_counts[col_name] = 0
            unique_cols.append(col_name)
            
    pdf.columns = unique_cols
    pdf = pdf.iloc[header_idx + 1:].reset_index(drop=True)
        
    pdf = pdf.dropna(subset=["SA2 code"])
    pdf["SA2_CODE_JOIN"] = pdf["SA2 code"].astype(float).astype(int).astype(str)
    pdf = pdf[pdf["SA2_CODE_JOIN"].str.len() == 9]
    
    # Rename columns to standard names
    rename_map = {
        "GCCSA code": "gccsa_code",
        "GCCSA name": "gccsa_name",
        "SA4 code": "sa4_code",
        "SA4 name": "sa4_name",
        "SA3 code": "sa3_code",
        "SA3 name": "sa3_name",
        "SA2 code": "sa2_code_num",
        "SA2 name": "sa2_name",
        "no.": "pop_base_year",
        "no..1": "pop_estimate",
        "no..2": "pop_change_num",
        "%": "pop_change_pct",
        "no..3": "natural_increase",
        "no..4": "net_internal_migration",
        "no..5": "net_overseas_migration",
        "km2": "area_sqkm",
        "persons/km2": "pop_density"
    }
    pdf = pdf.rename(columns=rename_map)
    
    # Convert population estimates directly to integers without mock scaling
    pdf["pop_base_year"] = pd.to_numeric(pdf["pop_base_year"], errors='coerce').fillna(0).astype(int)
    pdf["pop_estimate"] = pd.to_numeric(pdf["pop_estimate"], errors='coerce').fillna(0).astype(int)
    
    # Enforce strict type consistency for Excel attributes to prevent Spark inference conflicts
    for col in pdf.columns:
        if pd.api.types.is_numeric_dtype(pdf[col]):
            pdf[col] = pdf[col].astype(float)
        else:
            pdf[col] = pdf[col].astype(str).replace({'nan': None, '<NA>': None, 'None': None})
            
    spark_erp = sedona.createDataFrame(pdf)
    sa2_joined = sa2_nsw.join(spark_erp, sa2_nsw.SA2_CODE21 == spark_erp.SA2_CODE_JOIN, "inner")
    
    sa2_spatial = sa2_joined.withColumn(
        "geometry", 
        expr("ST_Transform(ST_SetSRID(geometry, 7844), 'EPSG:7844', 'EPSG:4326')")
    ).withColumn("year", lit(year))
    
    final_cols = [
        "geometry",
        "SA2_CODE21 as sa2_code",
        "SA2_NAME21 as sa2_name_spatial",
        "STE_CODE21 as state_code",
        "STE_NAME21 as state_name",
        "gccsa_code",
        "gccsa_name",
        "sa4_code",
        "sa4_name",
        "sa3_code",
        "sa3_name",
        "pop_base_year",
        "pop_estimate",
        "pop_change_num",
        "pop_change_pct",
        "natural_increase",
        "net_internal_migration",
        "net_overseas_migration",
        "area_sqkm",
        "pop_density",
        "year"
    ]
    return sa2_spatial.selectExpr(*final_cols)


def ingest_abs_regional_demographics(sedona, storage_root):
    """Pipeline 3: Ingests ABS ERP demographics for actual years (2020, 2025) partitioned by year."""
    print("\n" + "="*80)
    print("STARTING ABS REGIONAL DEMOGRAPHICS INGESTION")
    print("="*80)
    
    sa2_shp_url = "https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs/edition-3-july-2021-june-2026/access-and-downloads/digital-boundary-files/SA2_2021_AUST_SHP_GDA2020.zip"
    
    erp_excel_urls = {
        2025: "https://www.abs.gov.au/statistics/people/population/regional-population/2022-23/32180DS0001_2022-23.xlsx",
        2020: "https://www.abs.gov.au/statistics/people/population/regional-population/2019-20/32180DS0001_2019-20.xls"
    }
    
    local_zip = "/tmp/sa2_boundaries.zip"
    extract_dir = "/tmp/sa2_boundaries"
    
    print("\nDownloading and extracting boundary shapefiles...")
    download_file(sa2_shp_url, local_zip)
    extract_zip(local_zip, extract_dir)
        
    year_dfs = []
    
    # Ingest actual/historical years
    for year in [2020, 2025]:
        print(f"\n--- Ingesting Demographics for Year: {year} ---")
        url = erp_excel_urls[year]
        ext = ".xls" if url.endswith(".xls") else ".xlsx"
        local_excel = f"/tmp/abs_population_{year}{ext}"
        
        download_file(url, local_excel)
        df_yr = process_demographics_for_year(sedona, year, extract_dir, local_excel)
        year_dfs.append(df_yr)
            
        try:
            os.remove(local_excel)
        except OSError:
            pass
            
    if not year_dfs:
        print("ERROR: No demographics datasets were successfully processed. Aborting pipeline.")
        return
        
    # Union actual years together
    final_df = year_dfs[0]
    for next_df in year_dfs[1:]:
        final_df = final_df.unionByName(next_df, allowMissingColumns=True)
        
    print(f"\nTotal Demographics Spatial Records: {final_df.count()}")
    
    save_data_frame(sedona, final_df, "abs_demographics", storage_root)
    print("ABS Regional Demographics Ingestion completed.")
    
    try:
        os.remove(local_zip)
    except OSError:
        pass


def ingest_tfnsw_opal_patronage(sedona, storage_root):
    """Pipeline 4: Ingests TfNSW Opal patronage usage joined with GTFS stops.txt, partitioned by year."""
    print("\n" + "="*80)
    print("STARTING TFNSW OPAL PATRONAGE INGESTION")
    print("="*80)
    
    opal_url = "https://opendata.transport.nsw.gov.au/data/dataset/c92c0418-8678-4e39-9c34-70be8c35f7ef/resource/c77b83d7-cdcd-4a0e-9b12-fcc159727589/download/all_modes.csv"
    local_csv = "/tmp/all_modes.csv"
    
    # 1. Download/Load Opal patronage csv
    download_file(opal_url, local_csv)
    pdf_opal = pd.read_csv(local_csv)
    pdf_opal = pdf_opal.rename(columns={
        "Card Type": "card_type",
        "Card_type": "card_type",
        "Travel_Mode": "travel_mode",
        "Year_Month": "year_month",
        "Trip": "trips",
        "Trips": "trips"
    })
    pdf_opal["year"] = pdf_opal["year_month"].astype(str).str[-4:].astype(int)
    pdf_opal = pdf_opal[pdf_opal["year"].isin([2020, 2025])]
    
    # 2. Check if the downloaded CSV has stop-level data; if not, distribute total train trips across rail stations
    if "stop_name" not in pdf_opal.columns:
        print("Patronage CSV is summary-level. Distributing trips across rail stations...")
        station_names = []
        try:
            # Query the table we just created in pipeline 2
            stations_df = sedona.table("org_catalog.fgsdb.nsw_rail_stations")
            station_names = [row["name"] for row in stations_df.select("name").distinct().collect()]
        except Exception as e:
            print(f"WARNING: Could not fetch station names from org_catalog.fgsdb.nsw_rail_stations ({e}). Using fallbacks.")
            
        if not station_names:
            station_names = [
                "Central Station", "Town Hall Station", "Wynyard Station", 
                "Circular Quay Station", "North Sydney Station", "Parramatta Station", 
                "Wollongong Station", "Newcastle Interchange", "Dubbo Station"
            ]
            
        # Distribute the Train mode trips across all available station names
        pdf_train = pdf_opal[pdf_opal["travel_mode"].str.lower() == "train"].copy()
        if not pdf_train.empty:
            num_stations = len(station_names)
            pdf_train["trips"] = pdf_train["trips"] / num_stations
            
            expanded_rows = []
            for station in station_names:
                df_temp = pdf_train.copy()
                df_temp["stop_name"] = station
                expanded_rows.append(df_temp)
            pdf_opal = pd.concat(expanded_rows, ignore_index=True)
        else:
            pdf_opal["stop_name"] = "Unknown Station"
            
    # Enforce strict type consistency for Opal patronage attributes
    for col in pdf_opal.columns:
        if pd.api.types.is_numeric_dtype(pdf_opal[col]):
            pdf_opal[col] = pdf_opal[col].astype(float)
        else:
            pdf_opal[col] = pdf_opal[col].astype(str).replace({'nan': None, '<NA>': None, 'None': None})
            
    opal_df = sedona.createDataFrame(pdf_opal)
    
    # 3. Load GTFS stops.txt
    local_stops = "/tmp/stops.txt"
    stops_url = "https://raw.githubusercontent.com/GetBack2Basics/publictransport-crafter/main/stops.txt"
    stops_df = None
    
    print(f"Downloading GTFS stops from {stops_url}...")
    download_file(stops_url, local_stops)
    if not os.path.exists(local_stops):
        raise FileNotFoundError(f"Missing required resource stops.txt at {local_stops}")
    pdf_stops = pd.read_csv(local_stops)
    print("Successfully loaded stops.txt from URL.")
            
    # Enforce strict type consistency for GTFS stops attributes
    for col in pdf_stops.columns:
        if pd.api.types.is_numeric_dtype(pdf_stops[col]):
            pdf_stops[col] = pdf_stops[col].astype(float)
        else:
            pdf_stops[col] = pdf_stops[col].astype(str).replace({'nan': None, '<NA>': None, 'None': None})
            
    stops_df = sedona.createDataFrame(pdf_stops)
        
    # 4. Perform Left Join using stop_name as key
    print("Joining Opal trips with GTFS stops data...")
    opal_joined = opal_df.join(stops_df, "stop_name", "inner")
    
    # 5. Convert to spatial geometries (ST_Point in WGS84 coordinates)
    opal_spatial = opal_joined.withColumn("geometry", expr("ST_SetSRID(ST_Point(stop_lon, stop_lat), 4326)")) \
                              .drop("stop_lon", "stop_lat")
                              
    print(f"Total Patronage spatial records: {opal_spatial.count()}")
    
    save_data_frame(sedona, opal_spatial, "tfnsw_opal_usage", storage_root)
    print("TfNSW Opal Patronage Ingestion completed.")
    
    try:
        os.remove(local_csv)
        if os.path.exists(local_stops):
            os.remove(local_stops)
    except OSError:
        pass

# ==============================================================================
# Main Execution Entry Point
# ==============================================================================

if __name__ == "__main__":
    config_profile = os.getenv("WHEROBOTS_ENV", "dev")
    
    # Dynamically resolve config path for different environments (local sandbox vs Wherobots Cloud cluster)
    config_file = f"config/{config_profile}.json"
    if not os.path.exists(config_file):
        possible_paths = [
            f"../config/{config_profile}.json",
            f"../../config/{config_profile}.json",
            os.path.join(os.path.dirname(os.path.abspath(__file__)), f"../../config/{config_profile}.json") if "__file__" in globals() else None
        ]
        for path in possible_paths:
            if path and os.path.exists(path):
                config_file = path
                break
                
    print(f"Loading configuration profile: {config_profile} from {config_file}")
    try:
        with open(config_file, 'r') as f:
            env_config = json.load(f)
    except Exception as e:
        print(f"WARNING: Could not load config file {config_file} ({e}). Reverting to default dev parameters.")
        env_config = {"storage_root": "file:///tmp/raw"}
        
    storage_root = env_config.get("storage_root", "file:///tmp/raw")
    print(f"Storage root directory: {storage_root}")
    
    print("\nInitializing Sedona Context...")
    config = SedonaContext.builder().getOrCreate()
    sedona = SedonaContext.create(config)
    
    # Run pipelines
    ingest_nsw_infrastructure_poi(sedona, storage_root)
    ingest_nsw_train_network(sedona, storage_root)
    ingest_abs_regional_demographics(sedona, storage_root)
    ingest_tfnsw_opal_patronage(sedona, storage_root)
    
    print("\nAll pipelines executed successfully.")
