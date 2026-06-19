import http.server
import socketserver
import json
import os
import urllib.request
import urllib.parse
import uuid
import websocket
import threading
import time

PORT = 8080
DIRECTORY = os.path.dirname(os.path.abspath(__file__))

API_KEY = 'wbk_user_kvrrww1y9lyecmjv77pyl7qub0607owg8ypp310ctr7xagwvt6wucwtqkduj8n68'
NOTEBOOK_ID = '9m1storvdykcar'  # Default fallback ID

# In-memory jobs registry
JOBS = {}

SOURCES = {
    "education": "https://portal.spatial.nsw.gov.au/server/rest/services/NSW_FOI_Education_Facilities/FeatureServer/0?f=json",
    "health": "https://portal.spatial.nsw.gov.au/server/rest/services/NSW_FOI_Health_Facilities/FeatureServer/0?f=json",
    "transport": "https://portal.spatial.nsw.gov.au/server/rest/services/NSW_Transport_Theme/FeatureServer/7?f=json",
    "opal": "https://opendata.transport.nsw.gov.au/data/dataset/c92c0418-8678-4e39-9c34-70be8c35f7ef/resource/c77b83d7-cdcd-4a0e-9b12-fcc159727589/download/all_modes.csv",
    "abs": "https://www.abs.gov.au/statistics/people/population/regional-population"
}

def fetch_url_metadata(url):
    # Try HEAD first
    try:
        req = urllib.request.Request(url, method='HEAD')
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36')
        with urllib.request.urlopen(req, timeout=8) as response:
            return {
                "etag": response.getheader('ETag'),
                "content_length": response.getheader('Content-Length'),
                "last_modified": response.getheader('Last-Modified')
            }
    except Exception:
        # Fallback to GET range request
        try:
            req = urllib.request.Request(url, method='GET')
            req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36')
            req.add_header('Range', 'bytes=0-10')
            with urllib.request.urlopen(req, timeout=8) as response:
                return {
                    "etag": response.getheader('ETag'),
                    "content_length": response.getheader('Content-Length'),
                    "last_modified": response.getheader('Last-Modified')
                }
        except Exception as e:
            print(f"Error fetching URL metadata for {url}: {e}")
            return None

def check_currency(log_callback=None):
    def log(msg):
        if log_callback:
            log_callback(msg)
        else:
            print(msg, end="")

    meta_file = os.path.join(DIRECTORY, "currency_meta.json")
    saved_meta = {}
    if os.path.exists(meta_file):
        try:
            with open(meta_file, 'r', encoding='utf-8') as f:
                saved_meta = json.load(f)
        except Exception as e:
            log(f"Warning: failed to read {meta_file}: {e}\n")

    # Get local modification times of the pipeline files
    base_proj_dir = os.path.dirname(os.path.dirname(DIRECTORY))
    nb_path = os.path.join(base_proj_dir, "notebooks", "wherobots_ingestion_pipeline.ipynb")
    script_path = os.path.join(base_proj_dir, "src", "Ingestion", "wherobots_ingestion_pipeline.py")
    
    nb_mtime = os.path.getmtime(nb_path) if os.path.exists(nb_path) else 0.0
    script_mtime = os.path.getmtime(script_path) if os.path.exists(script_path) else 0.0
    
    saved_nb_mtime = saved_meta.get("notebook_mtime", 0.0)
    saved_script_mtime = saved_meta.get("script_mtime", 0.0)
    
    notebook_updated = (nb_mtime != saved_nb_mtime) or (script_mtime != saved_script_mtime)
    
    # Query current sources metadata
    log("Checking remote data sources currency...\n")
    current_sources = {}
    for name, url in SOURCES.items():
        meta = fetch_url_metadata(url)
        if meta:
            current_sources[name] = meta
        else:
            # Carry over saved if we fail to fetch
            current_sources[name] = saved_meta.get("sources_meta", {}).get(name, {})
            
    saved_sources = saved_meta.get("sources_meta", {})
    
    data_updated = False
    if not saved_sources:
        data_updated = True
    else:
        for name in SOURCES:
            curr = current_sources.get(name, {})
            sav = saved_sources.get(name, {})
            if curr.get("etag") and sav.get("etag"):
                if curr.get("etag") != sav.get("etag"):
                    log(f"New ETag detected for source: {name} (Saved: {sav.get('etag')} vs Current: {curr.get('etag')})\n")
                    data_updated = True
            elif curr.get("content_length") != sav.get("content_length") or curr.get("last_modified") != sav.get("last_modified"):
                log(f"New length or date detected for source: {name} (Saved: Len={sav.get('content_length')}, Date={sav.get('last_modified')} vs Current: Len={curr.get('content_length')}, Date={curr.get('last_modified')})\n")
                data_updated = True
                
    return notebook_updated, data_updated, nb_mtime, script_mtime, current_sources

def upload_file_via_urllib(local_path, remote_path, notebook_id, api_key):
    with open(local_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    url = f"https://aws-us-west-2.compute.cloud.wherobots.com/jupyter/ltq5l3obgb/{notebook_id}/api/contents/{remote_path}"
    payload = {
        "name": os.path.basename(remote_path),
        "path": remote_path,
        "type": "file",
        "format": "text",
        "content": content
    }
    
    remote_dir = os.path.dirname(remote_path)
    if remote_dir:
        dir_url = f"https://aws-us-west-2.compute.cloud.wherobots.com/jupyter/ltq5l3obgb/{notebook_id}/api/contents/{remote_dir}"
        req_dir = urllib.request.Request(
            dir_url,
            data=json.dumps({"type": "directory"}).encode('utf-8'),
            headers={'x-api-key': api_key, 'Content-Type': 'application/json'},
            method='PUT'
        )
        try:
            with urllib.request.urlopen(req_dir) as response:
                pass
        except Exception:
            pass
            
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'x-api-key': api_key, 'Content-Type': 'application/json'},
        method='PUT'
    )
    with urllib.request.urlopen(req) as response:
        return response.getcode() in [200, 201]

def upload_notebook_via_urllib(local_path, remote_path, notebook_id, api_key):
    with open(local_path, 'r', encoding='utf-8') as f:
        content = json.load(f)
    
    url = f"https://aws-us-west-2.compute.cloud.wherobots.com/jupyter/ltq5l3obgb/{notebook_id}/api/contents/{remote_path}"
    payload = {
        "name": os.path.basename(remote_path),
        "path": remote_path,
        "type": "notebook",
        "format": "json",
        "content": content
    }
    
    remote_dir = os.path.dirname(remote_path)
    if remote_dir:
        dir_url = f"https://aws-us-west-2.compute.cloud.wherobots.com/jupyter/ltq5l3obgb/{notebook_id}/api/contents/{remote_dir}"
        req_dir = urllib.request.Request(
            dir_url,
            data=json.dumps({"type": "directory"}).encode('utf-8'),
            headers={'x-api-key': api_key, 'Content-Type': 'application/json'},
            method='PUT'
        )
        try:
            with urllib.request.urlopen(req_dir) as response:
                pass
        except Exception:
            pass

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode('utf-8'),
        headers={'x-api-key': api_key, 'Content-Type': 'application/json'},
        method='PUT'
    )
    with urllib.request.urlopen(req) as response:
        return response.getcode() in [200, 201]

def execute_wherobots_code(code, notebook_id, log_callback=None):
    def log(msg_text):
        if log_callback:
            log_callback(msg_text)
        else:
            print(msg_text, end="")

    base_url = f"https://aws-us-west-2.compute.cloud.wherobots.com/jupyter/ltq5l3obgb/{notebook_id}"
    ws_base_url = f"wss://aws-us-west-2.compute.cloud.wherobots.com/jupyter/ltq5l3obgb/{notebook_id}"
    local_headers = {
        'x-api-key': API_KEY,
        'Content-Type': 'application/json'
    }

    log("Starting kernel on Wherobots...\n")
    req = urllib.request.Request(
        f"{base_url}/api/kernels",
        data=json.dumps({"name": "python3"}).encode('utf-8'),
        headers=local_headers,
        method='POST'
    )
    try:
        with urllib.request.urlopen(req) as response:
            res_data = json.loads(response.read().decode('utf-8'))
    except Exception as e:
        log(f"Failed to start kernel: {e}\n")
        raise

    kernel_id = res_data["id"]
    log(f"Kernel started with ID: {kernel_id}\n")

    stdout_output = []
    error_output = []

    try:
        ws_url = f"{ws_base_url}/api/kernels/{kernel_id}/channels"
        ws = websocket.create_connection(ws_url, header=[f"x-api-key: {API_KEY}"])

        session_id = str(uuid.uuid4())
        msg_id = str(uuid.uuid4())
        execute_msg = {
            "header": {"msg_id": msg_id, "username": "wherobots", "session": session_id, "msg_type": "execute_request", "version": "5.3"},
            "metadata": {},
            "content": {"code": code, "silent": False, "store_history": True, "user_expressions": {}, "allow_stdin": False, "stop_on_error": True},
            "parent_header": {},
            "channel": "shell"
        }
        ws.send(json.dumps(execute_msg))
        log("Code sent. Awaiting results...\n")

        while True:
            msg = json.loads(ws.recv())
            msg_type = msg.get("header", {}).get("msg_type")
            channel = msg.get("channel")
            
            if msg_type == "stream":
                text = msg.get("content", {}).get("text", "")
                stdout_output.append(text)
                log(text)
            elif msg_type == "error":
                traceback = msg.get("content", {}).get("traceback", [])
                error_output.append("\n".join(traceback))
                log("\n".join(traceback) + "\n")
            elif msg_type == "execute_reply" and channel == "shell":
                break

        ws.close()
    finally:
        log(f"Stopping kernel {kernel_id} on Wherobots...\n")
        try:
            req_del = urllib.request.Request(
                f"{base_url}/api/kernels/{kernel_id}",
                headers=local_headers,
                method='DELETE'
            )
            with urllib.request.urlopen(req_del) as response:
                pass
            log(f"Kernel {kernel_id} stopped successfully.\n")
        except Exception as e_del:
            log(f"Failed to stop kernel {kernel_id}: {e_del}\n")
    
    if error_output:
        raise Exception("Execution error:\n" + "\n".join(error_output))
        
    return "".join(stdout_output)

def download_kepler_map(notebook_id):
    print("Downloading Kepler map html from Wherobots...")
    base_url = f"https://aws-us-west-2.compute.cloud.wherobots.com/jupyter/ltq5l3obgb/{notebook_id}"
    map_url = f"{base_url}/files/raw_data_explorer.html"
    req = urllib.request.Request(map_url, headers={'x-api-key': API_KEY})
    local_path = os.path.join(DIRECTORY, "raw_data_explorer.html")
    
    try:
        with urllib.request.urlopen(req) as response:
            with open(local_path, 'wb') as f:
                f.write(response.read())
        print(f"Kepler map saved to {local_path}")
        return True
    except Exception as e:
        print(f"Failed to download Kepler map: {e}")
        return False

def extract_json_from_output(output):
    json_start = output.find("RESULT_START")
    json_end = output.find("RESULT_END")
    if json_start != -1 and json_end != -1:
        raw_content = output[json_start + len("RESULT_START"):json_end]
        first_brace = raw_content.find('{')
        last_brace = raw_content.rfind('}')
        if first_brace != -1 and last_brace != -1:
            try:
                return json.loads(raw_content[first_brace:last_brace+1])
            except Exception as e:
                print(f"Error decoding JSON string: {e}")
    return None

def run_analysis_thread(job_id, min_lon, min_lat, max_lon, max_lat, notebook_id):
    job = JOBS[job_id]
    lat_center = (min_lat + max_lat) / 2
    lon_center = (min_lon + max_lon) / 2
    
    code_template = f"""
import json
from sedona.spark import SedonaContext
from sedona.spark.maps.SedonaKepler import SedonaKepler

config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

bbox = "POLYGON (({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"

# 1. Demographics
demo_df = sedona.sql(f"SELECT sa2_code, sa2_name_spatial AS sa2_name, pop_estimate, pop_density FROM org_catalog.fgsdb.abs_demographics WHERE year = 2025 AND ST_Contains(ST_GeomFromText('{{bbox}}'), geometry)")
demo_pdf = demo_df.toPandas()

# 2. Chokepoints
choke_df = sedona.sql(f"SELECT stop_name, travel_mode, SUM(trips) AS total_trips FROM org_catalog.fgsdb.tfnsw_opal_usage WHERE year = 2025 AND ST_Within(geometry, ST_GeomFromText('{{bbox}}')) GROUP BY stop_name, travel_mode ORDER BY total_trips DESC LIMIT 10")
choke_pdf = choke_df.toPandas()

# 3. Service Loss Catchment
loss_df = sedona.sql(f'''
    SELECT 
        p.stop_name, 
        p.travel_mode, 
        SUM(p.trips) AS total_trips
    FROM 
        org_catalog.fgsdb.tfnsw_opal_usage p
    LEFT JOIN (
        SELECT DISTINCT p2.stop_name
        FROM (
            SELECT stop_name, geometry 
            FROM org_catalog.fgsdb.tfnsw_opal_usage 
            WHERE year = 2025 AND ST_Within(geometry, ST_GeomFromText('{{bbox}}'))
        ) p2
        JOIN (
            SELECT geometry 
            FROM wherobots_open_data.overture_maps_foundation.transportation_segment 
            WHERE subtype = 'road' 
              AND ST_Contains(ST_GeomFromText('{{bbox}}'), geometry)
        ) r
        ON ST_Distance(p2.geometry, r.geometry) <= 0.018
    ) roads
    ON p.stop_name = roads.stop_name
    WHERE 
        p.year = 2025
        AND ST_Within(p.geometry, ST_GeomFromText('{{bbox}}'))
        AND roads.stop_name IS NULL
    GROUP BY 
        p.stop_name, p.travel_mode
''')
loss_pdf = loss_df.toPandas()

# 4. Generate Kepler Map
map_config = {{
    "mapState": {{
        "bearing": 0,
        "dragRotate": False,
        "latitude": {lat_center},
        "longitude": {lon_center},
        "pitch": 0,
        "zoom": 10,
        "isSplit": False
    }}
}}
map_vis = SedonaKepler.create_map(df=demo_df, name="Hunter Demographics (SA2)", config=map_config)

stations_df = sedona.sql(f"SELECT name, station_class, geometry FROM org_catalog.fgsdb.nsw_rail_stations WHERE ST_Contains(ST_GeomFromText('{{bbox}}'), geometry)")
roads_df = sedona.sql(f"SELECT id, subtype, geometry FROM wherobots_open_data.overture_maps_foundation.transportation_segment WHERE ST_Contains(ST_GeomFromText('{{bbox}}'), geometry) AND subtype = 'road' LIMIT 1000")
patronage_df = sedona.sql(f"SELECT stop_name, travel_mode, SUM(trips) AS total_trips, geometry FROM org_catalog.fgsdb.tfnsw_opal_usage WHERE year = 2025 AND ST_Within(geometry, ST_GeomFromText('{{bbox}}')) GROUP BY stop_name, travel_mode, geometry")
loss_catchment_df = sedona.sql(f'''
    SELECT 
        p.stop_name, 
        p.travel_mode, 
        SUM(p.trips) AS total_trips, 
        p.geometry
    FROM 
        org_catalog.fgsdb.tfnsw_opal_usage p
    LEFT JOIN (
        SELECT DISTINCT p2.stop_name
        FROM (
            SELECT stop_name, geometry 
            FROM org_catalog.fgsdb.tfnsw_opal_usage 
            WHERE year = 2025 AND ST_Within(geometry, ST_GeomFromText('{{bbox}}'))
        ) p2
        JOIN (
            SELECT geometry 
            FROM wherobots_open_data.overture_maps_foundation.transportation_segment 
            WHERE subtype = 'road' 
              AND ST_Contains(ST_GeomFromText('{{bbox}}'), geometry)
        ) r
        ON ST_Distance(p2.geometry, r.geometry) <= 0.018
    ) roads
    ON p.stop_name = roads.stop_name
    WHERE 
        p.year = 2025
        AND ST_Within(p.geometry, ST_GeomFromText('{{bbox}}'))
        AND roads.stop_name IS NULL
    GROUP BY 
        p.stop_name, p.travel_mode, p.geometry
''')

SedonaKepler.add_df(map_vis, df=stations_df, name="NSW Rail Stations")
SedonaKepler.add_df(map_vis, df=roads_df, name="Hunter Road Network")
SedonaKepler.add_df(map_vis, df=patronage_df, name="Opal Patronage Trips")
SedonaKepler.add_df(map_vis, df=loss_catchment_df, name="Service Loss Catchment")

map_vis.save_to_html(file_name="raw_data_explorer.html")

print("RESULT_START")
print(json.dumps({{
    "demographics": demo_pdf.to_dict(orient="records"),
    "chokepoints": choke_pdf.to_dict(orient="records"),
    "loss_catchment": loss_pdf.to_dict(orient="records")
}}))
print("RESULT_END")
"""

    def append_log(text):
        job["logs"].append(text)

    try:
        output = execute_wherobots_code(code_template, notebook_id, append_log)
        
        results = extract_json_from_output(output)
        if not results:
            raise Exception("Could not parse query output from Wherobots. Output prefix:\n" + output[:300])
        
        append_log("Downloading updated Kepler map canvas from Wherobots...\n")
        download_success = download_kepler_map(notebook_id)
        results["map_downloaded"] = download_success
        
        job["status"] = "completed"
        job["results"] = results
        append_log("Job completed successfully!\n")
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)
        append_log(f"Job failed with error: {e}\n")

def run_redeploy_thread(job_id, notebook_id):
    job = JOBS[job_id]
    
    def log(text):
        job["logs"].append(text)
        print(f"[{job_id}] {text.strip()}")
        
    log("Starting Currency Verification...\n")
    
    try:
        notebook_updated, data_updated, nb_mtime, script_mtime, current_sources = check_currency(log)
    except Exception as e:
        log(f"Currency check failed: {e}. Defaulting to full redeployment.\n")
        notebook_updated = True
        data_updated = True
        nb_mtime = 0.0
        script_mtime = 0.0
        current_sources = {}
        
    trigger_ingestion = notebook_updated or data_updated
    
    # 1. Wake Container
    log("Waking Wherobots container...\n")
    url = f"https://aws-us-west-2.compute.cloud.wherobots.com/jupyter/ltq5l3obgb/{notebook_id}"
    req = urllib.request.Request(url, headers={'x-api-key': API_KEY})
    container_ready = False
    consecutive_502s = 0

    for i in range(30):
        log(f"Pinging Wherobots container {notebook_id} (Attempt {i+1}/30)...\n")
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                status = response.getcode()
                if status == 200:
                    log("Container is awake and active!\n")
                    container_ready = True
                    break
                else:
                    log(f"Container status: {status}. Waiting 10s...\n")
                    if status == 502:
                        consecutive_502s += 1
        except Exception as e:
            if hasattr(e, 'code'):
                status = e.code
                if status == 200:
                    log("Container is awake and active!\n")
                    container_ready = True
                    break
                if status == 502:
                    consecutive_502s += 1
                log(f"Attempt {i+1} status: {status}\n")
            else:
                log(f"Attempt {i+1} error: {e}\n")
        # After 10 consecutive 502s, the instance is likely permanently dead
        if consecutive_502s >= 10:
            log(f"Instance has returned 502 {consecutive_502s} times consecutively. Instance may be permanently terminated.\n")
            break
        time.sleep(10)

    if not container_ready:
        job["status"] = "failed"
        job["error"] = (
            f"Failed to wake Wherobots notebook instance '{notebook_id}'. "
            "The instance may have been permanently terminated (Community Edition auto-retires after 4 hours of inactivity). "
            "Please start a new notebook instance at https://cloud.wherobots.com and update the notebook_id in the dashboard."
        )
        log("Redeployment failed: Container could not be reached.\n")
        return

    # 2. Sync files
    base_proj_dir = os.path.dirname(os.path.dirname(DIRECTORY))
    files_to_sync = [
        (os.path.join(base_proj_dir, "config", "dev.json"), "config/dev.json", False),
        (os.path.join(base_proj_dir, "config", "stg.json"), "config/stg.json", False),
        (os.path.join(base_proj_dir, "config", "prod.json"), "config/prod.json", False),
        (os.path.join(base_proj_dir, "src", "Ingestion", "wherobots_ingestion_pipeline.py"), "wherobots_ingestion_pipeline.py", False),
        (os.path.join(base_proj_dir, "notebooks", "raw_data_explorer.ipynb"), "notebooks/raw_data_explorer.ipynb", True),
        (os.path.join(base_proj_dir, "notebooks", "raw_data_explorer.ipynb"), "raw_data_explorer.ipynb", True),
        (os.path.join(base_proj_dir, "notebooks", "wherobots_ingestion_pipeline.ipynb"), "notebooks/wherobots_ingestion_pipeline.ipynb", True),
        (os.path.join(base_proj_dir, "notebooks", "hunter_transit_analysis.ipynb"), "notebooks/hunter_transit_analysis.ipynb", True),
        (os.path.join(base_proj_dir, "notebooks", "export_to_contabo.ipynb"), "notebooks/export_to_contabo.ipynb", True)
    ]
    
    for local_p, remote_p, is_nb in files_to_sync:
        if not os.path.exists(local_p):
            log(f"Warning: local file {local_p} not found, skipping sync.\n")
            continue
        log(f"Syncing {os.path.basename(local_p)} to remote {remote_p}...\n")
        try:
            if is_nb:
                success = upload_notebook_via_urllib(local_p, remote_p, notebook_id, API_KEY)
            else:
                success = upload_file_via_urllib(local_p, remote_p, notebook_id, API_KEY)
            if success:
                log(f"Synced {remote_p} successfully.\n")
            else:
                log(f"Failed to sync {remote_p}.\n")
        except Exception as e:
            log(f"Error syncing {remote_p}: {e}\n")
            job["status"] = "failed"
            job["error"] = f"Failed to sync file {remote_p}: {e}"
            return

    # 3. Trigger remote execution
    if trigger_ingestion:
        if notebook_updated:
            log("Notebook code has been updated. Running full ingestion pipeline & Kepler Map generation...\n")
        elif data_updated:
            log("Source data has been updated. Running full ingestion pipeline & Kepler Map generation...\n")
            
        run_code = """
import os
os.environ['WHEROBOTS_ENV'] = 'dev'
import wherobots_ingestion_pipeline
from sedona.spark import SedonaContext
from sedona.spark.maps.SedonaKepler import SedonaKepler
import json

print("Initializing Sedona Context...")
config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

storage_root = "wherobots://fgsdb/raw"

print("Running Ingestion Pipelines...")
try:
    print("- Ingesting NSW Critical Infrastructure POI...")
    wherobots_ingestion_pipeline.ingest_nsw_infrastructure_poi(sedona, storage_root)
except Exception as e:
    print(f"POI Ingestion warning/error: {e}")

try:
    print("- Ingesting NSW Train Network...")
    wherobots_ingestion_pipeline.ingest_nsw_train_network(sedona, storage_root)
except Exception as e:
    print(f"Train Network Ingestion warning/error: {e}")

try:
    print("- Ingesting ABS Regional Demographics...")
    wherobots_ingestion_pipeline.ingest_abs_regional_demographics(sedona, storage_root)
except Exception as e:
    print(f"Demographics Ingestion warning/error: {e}")

try:
    print("- Ingesting TfNSW Opal Patronage...")
    wherobots_ingestion_pipeline.ingest_tfnsw_opal_patronage(sedona, storage_root)
except Exception as e:
    print(f"Opal Patronage Ingestion warning/error: {e}")

print("Pre-rendering Kepler Map for default Newcastle bounds...")
min_lon, min_lat, max_lon, max_lat = 151.10, -33.15, 151.85, -32.70
bbox = f"POLYGON (({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"

# 1. Demographics
demo_df = sedona.sql(f"SELECT sa2_code, sa2_name_spatial AS sa2_name, pop_estimate, pop_density FROM org_catalog.fgsdb.abs_demographics WHERE year = 2025 AND ST_Contains(ST_GeomFromText('{bbox}'), geometry)")

# 2. Stations & Roads & Patronage
stations_df = sedona.sql(f"SELECT name, station_class, geometry FROM org_catalog.fgsdb.nsw_rail_stations WHERE ST_Contains(ST_GeomFromText('{bbox}'), geometry)")
roads_df = sedona.sql(f"SELECT id, subtype, geometry FROM wherobots_open_data.overture_maps_foundation.transportation_segment WHERE ST_Contains(ST_GeomFromText('{bbox}'), geometry) AND subtype = 'road' LIMIT 1000")
patronage_df = sedona.sql(f"SELECT stop_name, travel_mode, SUM(trips) AS total_trips, geometry FROM org_catalog.fgsdb.tfnsw_opal_usage WHERE year = 2025 AND ST_Within(geometry, ST_GeomFromText('{bbox}')) GROUP BY stop_name, travel_mode, geometry")

# 3. Create Kepler
map_config = {
    "mapState": {
        "bearing": 0,
        "dragRotate": False,
        "latitude": -32.925,
        "longitude": 151.475,
        "pitch": 0,
        "zoom": 10,
        "isSplit": False
    }
}
map_vis = SedonaKepler.create_map(df=demo_df, name="Hunter Demographics (SA2)", config=map_config)
SedonaKepler.add_df(map_vis, df=stations_df, name="NSW Rail Stations")
SedonaKepler.add_df(map_vis, df=roads_df, name="Hunter Road Network")
SedonaKepler.add_df(map_vis, df=patronage_df, name="Opal Patronage Trips")
map_vis.save_to_html(file_name="raw_data_explorer.html")

print("Pipeline Ingestion and Kepler map generation completed successfully!")
"""
    else:
        log("No changes detected in code or source data. Skipping remote Ingestion.\n")
        log("Directly accessing existing Havasu tables and pre-rendering Kepler map (Data Access Mode)...\n")
        
        run_code = """
import os
os.environ['WHEROBOTS_ENV'] = 'dev'
from sedona.spark import SedonaContext
from sedona.spark.maps.SedonaKepler import SedonaKepler
import json

print("Initializing Sedona Context (Skipping Ingestion)...")
config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

print("Pre-rendering Kepler Map for default Newcastle bounds from existing tables...")
min_lon, min_lat, max_lon, max_lat = 151.10, -33.15, 151.85, -32.70
bbox = f"POLYGON (({min_lon} {min_lat}, {max_lon} {min_lat}, {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"

# 1. Demographics
demo_df = sedona.sql(f"SELECT sa2_code, sa2_name_spatial AS sa2_name, pop_estimate, pop_density FROM org_catalog.fgsdb.abs_demographics WHERE year = 2025 AND ST_Contains(ST_GeomFromText('{bbox}'), geometry)")

# 2. Stations & Roads & Patronage
stations_df = sedona.sql(f"SELECT name, station_class, geometry FROM org_catalog.fgsdb.nsw_rail_stations WHERE ST_Contains(ST_GeomFromText('{bbox}'), geometry)")
roads_df = sedona.sql(f"SELECT id, subtype, geometry FROM wherobots_open_data.overture_maps_foundation.transportation_segment WHERE ST_Contains(ST_GeomFromText('{bbox}'), geometry) AND subtype = 'road' LIMIT 1000")
patronage_df = sedona.sql(f"SELECT stop_name, travel_mode, SUM(trips) AS total_trips, geometry FROM org_catalog.fgsdb.tfnsw_opal_usage WHERE year = 2025 AND ST_Within(geometry, ST_GeomFromText('{bbox}')) GROUP BY stop_name, travel_mode, geometry")

# 3. Create Kepler
map_config = {
    "mapState": {
        "bearing": 0,
        "dragRotate": False,
        "latitude": -32.925,
        "longitude": 151.475,
        "pitch": 0,
        "zoom": 10,
        "isSplit": False
    }
}
map_vis = SedonaKepler.create_map(df=demo_df, name="Hunter Demographics (SA2)", config=map_config)
SedonaKepler.add_df(map_vis, df=stations_df, name="NSW Rail Stations")
SedonaKepler.add_df(map_vis, df=roads_df, name="Hunter Road Network")
SedonaKepler.add_df(map_vis, df=patronage_df, name="Opal Patronage Trips")
map_vis.save_to_html(file_name="raw_data_explorer.html")

print("Kepler map generation from existing tables completed successfully!")
"""

    try:
        execute_wherobots_code(run_code, notebook_id, log)
        log("Downloading pre-rendered Kepler map...\n")
        download_success = download_kepler_map(notebook_id)
        
        log("Fetching default data for Newcastle area...\n")
        fetch_code = """
from sedona.spark import SedonaContext
import json
config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)
bbox = "POLYGON ((151.10 -33.15, 151.85 -33.15, 151.85 -32.70, 151.10 -32.70, 151.10 -33.15))"
demo_df = sedona.sql(f"SELECT sa2_code, sa2_name_spatial AS sa2_name, pop_estimate, pop_density FROM org_catalog.fgsdb.abs_demographics WHERE year = 2025 AND ST_Contains(ST_GeomFromText('{bbox}'), geometry)")
choke_df = sedona.sql(f"SELECT stop_name, travel_mode, SUM(trips) AS total_trips FROM org_catalog.fgsdb.tfnsw_opal_usage WHERE year = 2025 AND ST_Within(geometry, ST_GeomFromText('{bbox}')) GROUP BY stop_name, travel_mode ORDER BY total_trips DESC LIMIT 10")
loss_df = sedona.sql(f'''
    SELECT 
        p.stop_name, 
        p.travel_mode, 
        SUM(p.trips) AS total_trips
    FROM 
        org_catalog.fgsdb.tfnsw_opal_usage p
    LEFT JOIN (
        SELECT DISTINCT p2.stop_name
        FROM (
            SELECT stop_name, geometry 
            FROM org_catalog.fgsdb.tfnsw_opal_usage 
            WHERE year = 2025 AND ST_Within(geometry, ST_GeomFromText('{bbox}'))
        ) p2
        JOIN (
            SELECT geometry 
            FROM wherobots_open_data.overture_maps_foundation.transportation_segment 
            WHERE subtype = 'road' 
              AND ST_Contains(ST_GeomFromText('{bbox}'), geometry)
        ) r
        ON ST_Distance(p2.geometry, r.geometry) <= 0.018
    ) roads
    ON p.stop_name = roads.stop_name
    WHERE 
        p.year = 2025
        AND ST_Within(p.geometry, ST_GeomFromText('{bbox}'))
        AND roads.stop_name IS NULL
    GROUP BY 
        p.stop_name, p.travel_mode
''')
print("RESULT_START")
print(json.dumps({
    "demographics": demo_df.toPandas().to_dict(orient="records"),
    "chokepoints": choke_df.toPandas().to_dict(orient="records"),
    "loss_catchment": loss_df.toPandas().to_dict(orient="records")
}))
print("RESULT_END")
"""
        fetch_output = execute_wherobots_code(fetch_code, notebook_id, log)
        
        results = extract_json_from_output(fetch_output)
        if not results:
            results = {"demographics": [], "chokepoints": [], "loss_catchment": []}
            
        results["map_downloaded"] = download_success
        
        # Save updated metadata to local currency meta file
        meta_file = os.path.join(DIRECTORY, "currency_meta.json")
        try:
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "notebook_mtime": nb_mtime,
                    "script_mtime": script_mtime,
                    "sources_meta": current_sources,
                    "last_ingestion_timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                }, f, indent=2)
            log("Saved updated currency metadata.\n")
        except Exception as em:
            log(f"Warning: Failed to save currency meta: {em}\n")

        job["status"] = "completed"
        job["results"] = results
        log("Redeployment process complete. Wherobots is now active and ready to test!\n")
    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)
        log(f"Redeployment failed: {e}\n")

class DashboardHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        if parsed_url.path == '/job_status':
            query_params = urllib.parse.parse_qs(parsed_url.query)
            job_id = query_params.get('id', [None])[0]
            
            if job_id and job_id in JOBS:
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(JOBS[job_id]).encode('utf-8'))
            else:
                self.send_response(404)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Job not found"}).encode('utf-8'))
        elif parsed_url.path == '/health':
            # Check if the Wherobots notebook instance is reachable
            query_params = urllib.parse.parse_qs(parsed_url.query)
            nb_id = query_params.get('notebook_id', [NOTEBOOK_ID])[0].strip() or NOTEBOOK_ID
            health_url = f"https://aws-us-west-2.compute.cloud.wherobots.com/jupyter/ltq5l3obgb/{nb_id}"
            health_req = urllib.request.Request(health_url, headers={'x-api-key': API_KEY})
            try:
                with urllib.request.urlopen(health_req, timeout=10) as resp:
                    status = resp.getcode()
                    healthy = (status == 200)
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "notebook_id": nb_id,
                        "status": "healthy" if healthy else "degraded",
                        "http_code": status,
                        "message": "Instance is alive" if healthy else f"Instance returned HTTP {status}"
                    }).encode('utf-8'))
            except Exception as e:
                code = getattr(e, 'code', 0)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    "notebook_id": nb_id,
                    "status": "unhealthy",
                    "http_code": code,
                    "message": (
                        "Instance is unreachable. It may have been permanently terminated "
                        "(Community Edition auto-retires after 4 hours). "
                        "Start a new instance at https://cloud.wherobots.com"
                    )
                }).encode('utf-8'))
        else:
            super().do_GET()

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        params = json.loads(post_data.decode('utf-8'))
        notebook_id = params.get('notebook_id', NOTEBOOK_ID).strip()
        if not notebook_id:
            notebook_id = NOTEBOOK_ID

        if self.path == '/start_analysis':
            min_lon = float(params.get('min_lon', 151.10))
            min_lat = float(params.get('min_lat', -33.15))
            max_lon = float(params.get('max_lon', 151.85))
            max_lat = float(params.get('max_lat', -32.70))
            
            job_id = str(uuid.uuid4())
            JOBS[job_id] = {
                "status": "running",
                "logs": [],
                "results": None,
                "error": None
            }
            
            thread = threading.Thread(
                target=run_analysis_thread,
                args=(job_id, min_lon, min_lat, max_lon, max_lat, notebook_id)
            )
            thread.daemon = True
            thread.start()
            
            self.send_response(202) # Accepted
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"job_id": job_id}).encode('utf-8'))

        elif self.path == '/redeploy':
            job_id = str(uuid.uuid4())
            JOBS[job_id] = {
                "status": "running",
                "logs": [],
                "results": None,
                "error": None
            }
            
            thread = threading.Thread(
                target=run_redeploy_thread,
                args=(job_id, notebook_id)
            )
            thread.daemon = True
            thread.start()
            
            self.send_response(202) # Accepted
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"job_id": job_id}).encode('utf-8'))
        else:
            super().do_POST()

if __name__ == "__main__":
    os.makedirs(DIRECTORY, exist_ok=True)
    
    placeholder_path = os.path.join(DIRECTORY, "raw_data_explorer.html")
    if not os.path.exists(placeholder_path):
        with open(placeholder_path, 'w', encoding='utf-8') as f:
            f.write("""<!DOCTYPE html>
<html>
<head>
    <style>
        body {
            background-color: #0f172a;
            color: #94a3b8;
            font-family: 'Inter', sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            height: 100vh;
            margin: 0;
            text-align: center;
        }
        .container {
            padding: 2rem;
            border: 1px dashed #334155;
            border-radius: 8px;
            max-width: 400px;
        }
        h2 {
            color: #f8fafc;
            margin-bottom: 0.5rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <h2>Kepler.gl Map Canvas</h2>
        <p>No map data is currently loaded. Click "Redeploy & Sync Code" or "Execute Wherobots SQL Model" to build the map.</p>
    </div>
</body>
</html>
""")

    handler = DashboardHTTPRequestHandler
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), handler) as httpd:
        print(f"Dashboard server running locally at http://localhost:{PORT}")
        print("Press Ctrl+C to terminate.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server.")
