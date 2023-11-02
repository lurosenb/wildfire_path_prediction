import ee
import geemap.core as geemap
import geopandas as gpd
import pandas as pd
import numpy as np

from shapely.geometry import Polygon, box

CELL_WIDTH = 375
CELL_HEIGHT = 375
PERFORM_COUNT = False

ee.Authenticate()
ee.Initialize()

perimeter_data_file = 'data/perimeters/US_HIST_FIRE_PERIM_2015_DD83.shp'
perimeters = gpd.read_file(perimeter_data_file,  engine='pyogrio')

def count_rectangles_within(geometry, cell_width_meters, cell_height_meters, plot_rects=False):
    km_per_degree = 111.1 # oversimplified conversion from degrees to km
    avg_latitude = geometry.centroid.y
    cell_width_deg = cell_width_meters / (km_per_degree * 1000 * abs(np.cos(np.radians(avg_latitude))))
    cell_height_deg = cell_height_meters / (km_per_degree * 1000)
    
    minx, miny, maxx, maxy = geometry.bounds
    
    # generate grid of rectangles within the bounding box, cound how many
    # are contained within the geometry
    # NOTE: plot_rects is for visualizing along with count, not for efficiency
    #      (it's much slower)
    if plot_rects:
        rects = []
        x = minx
        while x < maxx:
            y = miny
            while y < maxy:
                rects.append(box(x, y, x + cell_width_deg, y + cell_height_deg))
                y += cell_height_deg
            x += cell_width_deg
        
        count = sum(1 for rect in rects if geometry.intersects(rect))
        gpd.GeoSeries([rect for rect in rects if geometry.intersects(rect)]).plot()
    else:
        count = 0
        x = minx
        while x < maxx:
            y = miny
            while y < maxy:
                rect = box(x, y, x + cell_width_deg, y + cell_height_deg)
                if geometry.intersects(rect):  # using intersects for efficiency
                    count += 1
                y += cell_height_deg
            x += cell_width_deg
    
    return count

def gpd_to_ee(perimeters, polygon=False):
    data = []

    for idx, geometry in perimeters.iterrows():
        centroid = geometry.geometry.centroid
        ee_centroid = ee.Geometry.Point([centroid.x, centroid.y])
        
        if polygon:
            if geometry.geom_type == 'MultiPolygon':
                polygons = [ee.Geometry.Polygon(list(p.exterior.coords)) for p in geometry.geometry.geoms]
                data.append((idx, polygons, True, ee_centroid))
            elif geometry.geom_type == 'Polygon':
                ee_polygon = ee.Geometry.Polygon(list(geometry.geometry.exterior.coords))
                data.append((idx, [ee_polygon], False, ee_centroid))
            else:
                bounds = geometry.geometry.bounds
                ee_rect = ee.Geometry.Rectangle([bounds[0], bounds[1], bounds[2], bounds[3]])
                data.append((idx, [ee_rect], False, ee_centroid))
        else:
            bounds = geometry.geometry.bounds
            ee_rect = ee.Geometry.Rectangle([bounds[0], bounds[1], bounds[2], bounds[3]])
            data.append((idx, [ee_rect], False, ee_centroid))

    ee_geometries_df = pd.DataFrame(data, columns=['index', 'geometry', 'multi', 'center'])
    ee_geometries_df.set_index('index', inplace=True)

    return ee_geometries_df

if PERFORM_COUNT:
    rect_counts = perimeters.geometry.apply(lambda p: count_rectangles_within(p, CELL_WIDTH, CELL_HEIGHT))
    print(rect_counts.describe())
    # For Data from 2015:
    # count     7609.000000
    # mean       701.751216
    # std       1351.951292
    # min          1.000000
    # 25%         25.000000
    # 50%        172.000000
    # 75%        703.000000
    # max      16542.000000
else:
    count_rectangles_within(perimeters.geometry.iloc[7607], CELL_WIDTH, CELL_HEIGHT)

def grab_wind_image_from_geometry(date_str, region):
    date = ee.Date(date_str)
    
    era5_one_day = ee.ImageCollection('ECMWF/ERA5_LAND/HOURLY').filterDate(date.advance(-1, 'day'), date.advance(1, 'day')).first()

    wind_u = era5_one_day.select('u_component_of_wind_10m')

    newScale = 375

    reduced_wind_u = wind_u.reduceResolution(
        reducer=ee.Reducer.mean(),
        maxPixels=1024,
        bestEffort=True
    ).reproject(
        crs="EPSG:3857", #temperature_celsius.projection(),
        scale=newScale
    )

    wind_u_array = reduced_wind_u.sampleRectangle(region=region).get('u_component_of_wind_10m')
    return wind_u_array.getInfo()


ee_geometries = gpd_to_ee(perimeters)

idx_max = perimeters.gisacres.idxmax()
idx_min = perimeters.gisacres.idxmin()
region = ee_geometries.iloc[idx_max].geometry[0]
center = ee_geometries.iloc[idx_max].center.coordinates().getInfo()

date_str = '2015-11-07'

wind_sample = grab_wind_image_from_geometry(date_str, region)
print(wind_sample)