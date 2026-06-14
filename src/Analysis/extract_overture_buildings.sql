-- Sedona Spatial SQL Query to extract building footprints from Wherobots Overture catalog for NSW
-- Bounding box for NSW: Longitude 141.0 to 154.0, Latitude -38.0 to -28.0

SELECT 
    id,
    names.primary AS name,
    class,
    height,
    num_floors,
    geometry
FROM 
    wherobots_open_data.overture_maps_foundation.building
WHERE 
    ST_Contains(
        ST_GeomFromText('POLYGON ((141.0 -38.0, 154.0 -38.0, 154.0 -28.0, 141.0 -28.0, 141.0 -38.0))'), 
        geometry
    );
