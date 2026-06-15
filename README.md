# Public Transport Crafter: Wherobots Geospatial Ingestion & Analysis Pipeline

A production-grade geospatial data engineering pipeline built using **Apache Sedona** and **PySpark** on the **Wherobots Cloud** platform. This repository ingests, cleans, harmonizes, and partitions NSW public transport infrastructure, demographics, and Opal usage data, persisting them directly as native Iceberg/Havasu tables in the Wherobots Data Hub.

---

## 📂 Repository Structure

```text
publictransport-crafter/
├── config/                 # Environment configuration JSON profiles
│   ├── dev.json            # Development storage target & log level
│   ├── stg.json            # Staging storage target & log level
│   └── prod.json           # Production storage target & log level
├── notebooks/              # Interactive development and demonstration notebooks
│   ├── wherobots_ingestion_pipeline.ipynb
│   └── raw_data_explorer.ipynb # Kepler.gl spatial visualization setup
├── src/
│   ├── Ingestion/          # PySpark/Sedona pipeline scripts
│   │   └── wherobots_ingestion_pipeline.py
│   └── Analysis/           # Sedona Spatial SQL queries
│       ├── extract_overture_buildings.sql
│       └── hunter_transit_analysis.sql   # Hunter Region transit optimization queries
├── .gitignore              # Ignores pycache, local config, and ipynb checkpoints
└── README.md               # Repository documentation (this file)
```

---

## 🚀 Key Pipelines & Architecture

All pipeline datasets are processed into clean, structured PySpark DataFrames, reprojected, and written as Iceberg tables under the `org_catalog.fgsdb` catalog database on Wherobots Cloud (with dynamic fallback to local GeoParquet paths during local execution).

### 1. NSW Critical Infrastructure POI
- **Source**: NSW Spatial Collaboration Portal (Esri FeatureServers for Education and Health facility points).
- **Processing**: Queries geometries, parses GeoJSON, standardizes attributes, and sets SRS to **EPSG:4326 (WGS84)**.
- **Output Table**: `org_catalog.fgsdb.nsw_infrastructure_poi` (Partitioned by `year`).

### 2. TfNSW Train Network (Lines & Stations)
- **Source**: NSW Spatial Collaboration Portal (Railway Lines & Facility Points FeatureServers).
- **Processing**:
  - Extracts network line strings (Train Lines).
  - Extracts stations and classifies them into `Interchange`, `Regional`, and `Commuter` classes based on facility attributes.
- **Output Tables**:
  - `org_catalog.fgsdb.nsw_train_lines` (Partitioned by `year`)
  - `org_catalog.fgsdb.nsw_rail_stations` (Partitioned by `year`)

### 3. ABS Regional Demographics
- **Source**: ABS ERP (Estimated Resident Population) Excel Data Cubes joined with ASGS Edition 3 SA2 digital boundaries.
- **Processing**:
  - Ingests population statistics for years **2020** and **2025**.
  - Reprojects the shapefiles from GDA2020 (EPSG:7844) to WGS84 (EPSG:4326) using Sedona's `ST_Transform`.
  - Unions datasets and partitions by `year`.
- **Output Table**: `org_catalog.fgsdb.abs_demographics` (Partitioned by `year`).

### 4. TfNSW Opal Patronage via GTFS Stops Join
- **Source**: TfNSW Open Data Hub (Monthly stop-level trips CSV joined with GTFS `stops.txt` coordinate keys).
- **Processing**: Joins aggregate monthly patronage logs with GTFS coordinates to convert tabular trips data into geographic `ST_Point` records.
- **Output Table**: `org_catalog.fgsdb.tfnsw_opal_usage` (Partitioned by `year`).

---

## 🛠️ Getting Started

### Prerequisites
- Wherobots Cloud Runtime (Tiny or larger)
- Apache Sedona / PySpark context

### Running the Python Pipeline
Set the `WHEROBOTS_ENV` environment variable to load the correct storage destination profile (`dev`, `stg`, or `prod`), then run the ingestion script:

```bash
export WHEROBOTS_ENV="dev"
python3 src/Ingestion/wherobots_ingestion_pipeline.py
```

### Running the Interactive Notebook
Open `notebooks/wherobots_ingestion_pipeline.ipynb` inside your Wherobots Jupyter Lab workspace to run cells interactively. The script includes graceful fallback logic to mock datasets if external portals are unreachable during local development.

---

## 📊 Hunter Region Spatial SQL Analysis
The analysis script [hunter_transit_analysis.sql](src/Analysis/hunter_transit_analysis.sql) performs spatial queries to optimize route frequencies and evaluate network coverage in the Newcastle & Lake Macquarie region:

1. **Query 1**: Filter demographic populations in Newcastle & Lake Macquarie SA2 boundaries.
2. **Query 2**: Retrieve Overture Maps major road segments intersecting the region.
3. **Query 3**: Identify stops/communities outside a 2km corridor of major roads (Service Loss Catchment).
4. **Query 4**: Rank top transit chokepoints based on Opal patronage records.

These queries can be run directly inside the Wherobots SQL editor or within a Sedona-enabled notebook.

---

## 🛠️ Analysis-Only Workflows (Without Ingestion Updates)
If the primary ingestion pipelines are stable and do not require modification, you can perform new spatial analysis work directly against the persisted Iceberg tables:
1. **Connect directly to the Database Catalog**: You do not need to rerun the ingestion. In your notebook, establish the standard Sedona context:
   ```python
   from sedona.spark import *
   config = SedonaContext.builder().getOrCreate()
   sedona = SedonaContext.create(config)
   ```
2. **Query Persistent Tables**: Reference the catalog prefix `org_catalog.fgsdb` directly:
   ```python
   results_df = sedona.sql("""
       SELECT sa2_name, pop_estimate 
       FROM org_catalog.fgsdb.abs_demographics 
       WHERE pop_density > 2000
   """)
   ```
3. **Execute SQL via Wherobots Query Editor**: You can log into the Wherobots Data Hub dashboard and run queries directly in the SQL query editor UI, saving results to your workspace.

---

## 🗺️ Connecting and Visualizing in Felt.com
To visualize large-scale geospatial results processed in Wherobots inside the collaborative [Felt](https://felt.com) platform:

### 1. Export Data from Wherobots
Save your Spark analysis outputs as **GeoParquet** or **GeoJSON** directly to a shared S3 bucket (or download locally from the Wherobots workspace):
```python
# Save DataFrame to GeoParquet format
df.write.format("geoparquet").save("s3://your-wherobots-bucket/output/hunter_transit.parquet")
```

### 2. Import into Felt
*   **Via Web Interface**: Log into Felt, create a new map, and drag-and-drop the exported `.parquet` or `.geojson` file directly onto the map. Felt natively parses Sedona's WGS84 GeoParquet format.
*   **Via Felt API (Programmatic)**: You can upload your data dynamically from your notebook using the Felt REST API:
    ```bash
    curl -X POST "https://felt.com/api/v1/maps/{map_id}/uploads" \
      -H "Authorization: Bearer <YOUR_FELT_API_TOKEN>" \
      -F "file=@/path/to/hunter_transit.parquet"
    ```
Once imported, you can collaborate, style your layers, and share interactive dashboards with stakeholders.
