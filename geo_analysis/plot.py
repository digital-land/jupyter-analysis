import geopandas as gpd
import shapely.wkt
import logging

def plot_map(gdf:gpd.GeoDataFrame):
    if type(gdf) != gpd.GeoDataFrame:
        logging.error('input is not a GeodataFrame')        

    # take the point co-ordinates from the same as above
    base = gdf.explore()

    return base

def plot_issues_map(gdf:gpd.GeoDataFrame, entity_list, chloro_var, palette):

    if type(gdf) != gpd.GeoDataFrame:
        logging.error('input is not a GeodataFrame')
    
    base = gdf[gdf["entity"].isin(entity_list)].explore(
        column = chloro_var,  # make choropleth based on "BoroName" column
        cmap = palette,
        tooltip = False,
        popup = ["organisation_name", "entity", "name", "reference"],
        tiles = "CartoDB positron",  # use "CartoDB positron" tiles
        highlight = False,
        style_kwds = {
        "fillOpacity" : "0.1"
        }
    )
    
    return base