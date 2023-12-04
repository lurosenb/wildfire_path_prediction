import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import pickle
from datetime import datetime
import networkx as nx
from shapely.geometry import Point

class WildfirePerimeter:
    def __init__(self, perimeter, viirs_observations, climate_data, geographic_info):
        # perimeter GeoDataFrame
        self.perimeter = perimeter
        # viirs_observations GeoDataFrame
        self.viirs_observations = viirs_observations
        # climate observation dataframe
        self.climate_data = climate_data
        # geographic and elevation dataframe
        self.geographic_info = geographic_info

    def display_perimeter(self, with_observations=False):
        """Display the perimeter and optionally the observations within it."""
        fig, ax = plt.subplots()
        self.perimeter.geometry.boundary.plot(ax=ax, color='black', linewidth=2)
        
        if with_observations:
            self.viirs_observations.plot(ax=ax, marker='o', color='red', markersize=5)

        plt.show()

    def animate_fire_spread(self, save=True):
        """Create a gif that animates the fire spread based on datetime of each observation."""
        days = self.viirs_observations['date_time'].dt.date.unique()

        fig, ax = plt.subplots()
        geometry = [Point(xy) for xy in zip(self.viirs_observations['LONGITUDE'], self.viirs_observations['LATITUDE'])]
        gdf_obs = gpd.GeoDataFrame(self.viirs_observations, geometry=geometry)

        self.perimeter.plot(ax=ax, color='lightgray', edgecolor='black')

        def animate(day):
            ax.clear()
            self.perimeter.plot(ax=ax, color='lightgray', edgecolor='black')
            day_points = gdf_obs[gdf_obs['date_time'].dt.date == day]
            day_points.plot(ax=ax, markersize=5, color='red')
            
            ax.set_title(f"Observations on {day}")

        ani = animation.FuncAnimation(fig, animate, frames=days, repeat=False, interval=1000)

        if save:
            ani.save(f'fire_spread_{self.perimeter.uniquefire}.gif', writer='pillow', fps=1)

        
    def _get_cell_coords(self, lat, lon):
        # assume that 0.00335 degree of latitude is approximately 375 meters
        # and for simplicity, take longitude conversion as the same
        # we could convert these such that it aligns with earths curvature better
        cell_lat = round(lat / 0.00335) * 0.00335
        cell_lon = round(lon / 0.00335) * 0.00335
        return cell_lat, cell_lon
    
    def _features_lookup(self, observation_id):
        return {"feature": 0}
    
    def initialize_graph(self):
        G = nx.Graph()
        
        for _, row in self.viirs_observations.iterrows():
            cell_lat, cell_lon = self._get_cell_coords(row['LATITUDE'], row['LONGITUDE'])
            
            if (cell_lat, cell_lon) not in G:
                additional_data = self._features_lookup(row['observation_id'])
                G.add_node((cell_lat, cell_lon), **row.to_dict(), **additional_data)
                
        self.graph = G
        return self.graph
    
    def plot_graph(self):
        pos = {(x, y): (y, x) for x, y in self.graph.nodes()}  # flip x and y for lat/lon to get lon/lat
        nx.draw(self.graph, pos, with_labels=True, node_size=30)
        
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.title("Graph from VIIRS Observations")
        plt.show()
    
    def save(self, filename):
        """Save the object using pickle."""
        with open(filename, 'wb') as file:
            pickle.dump(self, file)

    @classmethod
    def load(cls, filename):
        """Load the object from a pickle file."""
        with open(filename, 'rb') as file:
            return pickle.load(file)



