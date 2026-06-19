# NSW Public Transport Crafter - Project Status & Quick Access

This file provides a quick-access summary of the project state, parameters, and commands, allowing any future session to load context instantly.

## 1. Environment & Wherobots Credentials
* **Active Notebook ID**: `9m1storvdykcar`
* **Wherobots API Key**: `wbk_user_kvrrww1y9lyecmjv77pyl7qub0607owg8ypp310ctr7xagwvt6wucwtqkduj8n68`
* **Wherobots Base URL**: `https://aws-us-west-2.compute.cloud.wherobots.com/jupyter/ltq5l3obgb/9m1storvdykcar`
* **Catalog Database**: `org_catalog.fgsdb`

## 2. Ingested Iceberg Tables
* `org_catalog.fgsdb.abs_demographics`: Suburb-level population estimates and densities.
* `org_catalog.fgsdb.nsw_rail_stations`: Spatial coordinates of NSW rail network stations.
* `org_catalog.fgsdb.tfnsw_opal_usage`: Trip volumes and patronage counts mapped to spatial stop coordinates.

## 3. Quick-Run Orchestration Commands
- **Upload local changes & trigger Kepler Map generation**:
  ```bash
  python3 /home/george-corea/.gemini/antigravity/scratch/run_new_notebook.py
  ```
- **Execute spatial SQL verification suite**:
  ```bash
  python3 /home/george-corea/.gemini/antigravity/scratch/verify_queries.py
  ```

## 4. Current Pipeline Status
* **Ingestion Status**: **Halted** due to missing `stops.txt` GTFS dependency.
* **Downstream Analysis Status**: **Ready**. Database tables are populated and queries run successfully.
