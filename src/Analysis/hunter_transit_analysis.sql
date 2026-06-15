-- ==============================================================================
-- Hunter Region Transit Optimization & Impact Analysis (Newcastle & Lake Macquarie)
-- ==============================================================================

-- 1. Create Bounding Box / Spatial Filter for Newcastle & Lake Macquarie LGAs
-- Newcastle Bounding Box envelope: POLYGON ((151.4 -33.15, 151.85 -33.15, 151.85 -32.8, 151.4 -32.8, 151.4 -33.15))

-- 2. Query 1: Filter SA2 demographic population within Newcastle & Lake Macquarie
SELECT 
    sa2_code,
    sa2_name_spatial AS sa2_name,
    pop_estimate,
    pop_density,
    geometry
FROM 
    abs_demographics
WHERE 
    year = 2025
    AND (sa3_name = 'Newcastle' OR sa3_name = 'Lake Macquarie' OR sa3_name = 'Hunter Valley' OR 
         ST_Contains(
             ST_GeomFromText('POLYGON ((151.4 -33.15, 151.85 -33.15, 151.85 -32.8, 151.4 -32.8, 151.4 -33.15))'), 
             geometry
         ));

-- 3. Query 2: Retrieve Overture Maps Major Road Segments in the Hunter Region
-- Uses the native Wherobots Overture Maps catalog
SELECT 
    id,
    subtype,
    ST_Buffer(geometry, 0.018) AS buffer_geometry,
    geometry
FROM 
    wherobots_open_data.overture_maps_foundation.transportation_segment
WHERE 
    ST_Contains(
        ST_GeomFromText('POLYGON ((151.4 -33.15, 151.85 -33.15, 151.85 -32.8, 151.4 -32.8, 151.4 -33.15))'), 
        geometry
    );

-- 4. Query 3: Identify communities/stops outside the 2km buffer of major road networks (Service Loss Catchment)
-- Finds stops that do not intersect the 2km corridor buffer zone.
WITH RoadBuffers AS (
    SELECT ST_Union_Aggr(ST_Buffer(geometry, 0.018)) AS unioned_buffer
    FROM wherobots_open_data.overture_maps_foundation.transportation_segment
    WHERE ST_Contains(
        ST_GeomFromText('POLYGON ((151.4 -33.15, 151.85 -33.15, 151.85 -32.8, 151.4 -32.8, 151.4 -33.15))'), 
        geometry
    )
)
SELECT 
    p.stop_name,
    p.travel_mode,
    SUM(p.trips) AS total_trips,
    p.geometry
FROM 
    tfnsw_opal_usage p,
    RoadBuffers b
WHERE 
    p.year = 2025
    AND ST_Within(p.geometry, ST_GeomFromText('POLYGON ((151.4 -33.15, 151.85 -33.15, 151.85 -32.8, 151.4 -32.8, 151.4 -33.15))'))
    AND NOT ST_Intersects(p.geometry, b.unioned_buffer)
GROUP BY 
    p.stop_name, p.travel_mode, p.geometry;

-- 5. Query 4: Identify Top 10 Transit Chokepoints (High Patronage Locations)
-- Ranks locations by trips count within Newcastle & Lake Macquarie
SELECT 
    stop_name,
    travel_mode,
    SUM(trips) AS total_trips,
    geometry
FROM 
    tfnsw_opal_usage
WHERE 
    year = 2025
    AND ST_Within(geometry, ST_GeomFromText('POLYGON ((151.4 -33.15, 151.85 -33.15, 151.85 -32.8, 151.4 -32.8, 151.4 -33.15))'))
GROUP BY 
    stop_name, travel_mode, geometry
ORDER BY 
    total_trips DESC
LIMIT 10;
