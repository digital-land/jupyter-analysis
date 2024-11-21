import geopandas as gpd
import shapely.wkt
import logging

def plot_map(gdf:gpd.GeoDataFrame):
    if type(gdf) != gpd.GeoDataFrame:
        logging.error('input is not a GeodataFrame')        

    # take the point co-ordinates from the same as above
    base = gdf.explore()

    return base