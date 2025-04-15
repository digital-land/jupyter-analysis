LOAD spatial;

COPY(

    WITH calc as (
        SELECT 
            ea23.objectid as ref_23,
            ST_Area(ST_Intersection(ea23.geom, ea25.geom)) / ST_Area(ST_Union(ea23.geom, ea25.geom)) as pct_area_overlap,
            ST_Area(ea23.geom) as area_geom_ea,
            ST_Area(ea25.geom) as area_geom_pdp,
            ea23.geom as geom_23,
            ea25.geom as geom_25,
        FROM ST_Read('processed/sample_ea-2023_frz2.geojson') ea23
        JOIN ST_Read('processed/sample_ea-2025_frz2.geojson') ea25 
            ON ST_Intersects(ea25.geom, ea23.geom)
        )

    SELECT ref_23, pct_area_overlap, geom_23, geom_25
    FROM calc
    WHERE pct_area_overlap > 0.9
    ORDER BY  pct_area_overlap DESC

)
TO 'output/calc_overlap.csv';

COPY(
    WITH good_matches as (
        SELECT ref_23, max(pct_area_overlap) as pct_area_overlap
        FROM 'output/calc_overlap.csv'
        GROUP BY ref_23
    )

    SELECT distinct o.ref_23, o.pct_area_overlap, o.geom_23
    FROM 'output/calc_overlap.csv' o
    INNER JOIN good_matches gm ON gm.ref_23 = o.ref_23 AND gm.pct_area_overlap = o.pct_area_overlap

)
TO 'output/calc_top_matches.csv'