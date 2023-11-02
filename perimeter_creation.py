import pickle
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from sklearn import tree, preprocessing
import sklearn.ensemble as ske
from sklearn.model_selection import train_test_split

from perimeter_data_class import WildfirePerimeter

import geopandas as gpd

from shapely.geometry import Point


## Constants and file paths
filtered_data_file = 'data_filtered/filtered_data_2015.pkl'
perimeter_data_file = 'data/perimeters/US_HIST_FIRE_PERIM_2015_DD83.shp'
fid = '2015-AKTAD-000333'

## Load data
with open (filtered_data_file, 'rb') as file:
    filtered_data = pickle.load(file)

perimeters = gpd.read_file(perimeter_data_file,  engine='pyogrio')

## Functions
def create_wildfire_perimeters(joined_gdf,perimeters):
    """
    creates a dictionary of WildfirePerimeter objects for each unique FID.
    """
    wildfire_perimeters = {}

    # group the joined dataframe by FID
    grouped = joined_gdf.groupby('uniquefire')

    for uniquefire, group in grouped:
        # extracting relevant rows for FID
        perimeter_row = perimeters[perimeters['uniquefire'] == uniquefire]
        perimeter_row['area'] = perimeter_row.geometry.area
        indices_of_largest = perimeter_row.groupby('uniquefire')['area'].idxmax()
        perimeter_row = perimeter_row.loc[indices_of_largest]
        perimeter_row = perimeter_row.drop(columns=['area'])
        
        # viirs_observations for this group is just the group without the perimeter details.
        # drop unnecessary columns (columns that are also present in perimeter_row).
        columns_to_drop = set(perimeter_row.columns) - set(['uniquefire']) 
        viirs_observations = group.drop(columns=columns_to_drop)
        print(uniquefire)
        # create object
        wildfire_perimeters[uniquefire] = WildfirePerimeter(perimeter_row, viirs_observations, None, None)
        # wildfire_perimeters[uniquefire].save(f"perimeter_{uniquefire.replace('-','_')}.pkl")

    return wildfire_perimeters

def plot_single_perimeter_with_obvs(fid, perimeters_dict, nx=False):
    """
    plots a single perimeter with the viirs observations within it.
    """

    wildfire_perim = perimeters_dict[fid]

    graph = wildfire_perim.initialize_graph()
    print(graph.nodes(data=True))

    fig, ax = plt.subplots()

    wildfire_perim.perimeter.plot(ax=ax, color='lightgray', edgecolor='black')

    geometry = [Point(xy) for xy in zip(wildfire_perim.viirs_observations['LONGITUDE'], wildfire_perim.viirs_observations['LATITUDE'])]
    gdf_obs = gpd.GeoDataFrame(wildfire_perim.viirs_observations, geometry=geometry)
    gdf_obs.plot(ax=ax, markersize=5, color='red')

    plt.title(f"Perimeter for CA fire ID {fid} and VIIRS Obs Within")
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')

    plt.show()

    if nx:
        pos = {(x, y): (y, x) for x, y in graph.nodes()} 
        nx.draw(graph, pos, with_labels=False, node_size=30)
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.title("Graph from VIIRS Observations")
        plt.show()

perimeters_dict = create_wildfire_perimeters(filtered_data, perimeters)

plot_single_perimeter_with_obvs(fid, perimeters_dict)