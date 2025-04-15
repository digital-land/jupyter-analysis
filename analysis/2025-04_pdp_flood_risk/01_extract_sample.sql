LOAD spatial;

-- get sample from 2023 data
COPY(
    SELECT * FROM ST_Read(
        'data/124a62c973429e80bb59ded0f049f5237ad8c6906edb4c73ac5749761febca79.json', 
        spatial_filter = ST_AsWKB(ST_GeomFromText('POLYGON ((-2.367897 51.363421, -2.282753 51.363421, -2.282753 51.40124, -2.367897 51.40124, -2.367897 51.363421))')))
    ) 
TO 'processed/sample_ea-2023_frz2.geojson'
WITH (FORMAT gdal, DRIVER 'GeoJSON');

-- get sample from 2025 data
COPY(
    SELECT ST_Transform(Shape, 'EPSG:27700', 'EPSG:4326', always_xy := true) as geometry, 
    FROM ST_Read(
        'data/FMfP_Flood_Zones_v202503.gdb',
        spatial_filter = ST_AsWKB(
            ST_Transform(
                ST_GeomFromText('POLYGON ((-2.367897 51.363421, -2.282753 51.363421, -2.282753 51.40124, -2.367897 51.40124, -2.367897 51.363421))'),
                'EPSG:4326',
                'EPSG:27700',
                always_xy := true
            ))
        )
    WHERE Flood_Zone = 'FZ2' 
    )
TO 'processed/sample_ea-2025_frz2.geojson'
WITH (FORMAT gdal, DRIVER 'GeoJSON');


