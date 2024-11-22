import geopandas as gpd
import shapely.wkt

def plot_map(geoms,points= None):
    if type(geoms) != list:
        geoms = [geoms]

    geoms_gdf = gpd.GeoDataFrame({'geometry':geoms})
    # take the point co-ordinates from the same as above
    base = geoms_gdf.explore()

    if points:
        if type(points) != list:
            points = [points]
        points_gdf = gpd.GeoDataFrame({'geometry':points})
        points_gdf['wkt'] = points_gdf.geometry.to_wkt()
        return points_gdf.explore(m = base,tooltip = "wkt")

    return base