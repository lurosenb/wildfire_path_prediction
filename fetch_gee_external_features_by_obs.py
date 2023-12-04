import pickle
import sqlite3
import pandas as pd
import numpy as np

from sklearn import tree, preprocessing
import sklearn.ensemble as ske
from sklearn.model_selection import train_test_split

import geopandas as gpd

import ee
import pandas as pd

import threading
import time
from IPython.display import clear_output

service_account = 'ping-gee@ee-supercharge-naturesnotebook.iam.gserviceaccount.com'
credentials = ee.ServiceAccountCredentials(service_account, 'ee-supercharge-naturesnotebook-5b165b3dae23.json')
ee.Initialize(credentials)

with open ('data_filtered/filtered_data_2015_protocol4.pkl', 'rb') as file:
    filtered_data = pickle.load(file)

def extract_dict(dict):
    return list(dict.keys())[0], list(dict.values())[0]

def get_region_and_point(lat, lon):
    half_pixel = 375 / 2  
    region = ee.Geometry.Rectangle([lon - half_pixel, lat - half_pixel, lon + half_pixel, lat + half_pixel])
    point = ee.Geometry.Point([lon, lat])
    return region, point

def retrieve_external_features(date_str, lat, lon):
    date = ee.Date(date_str)
    region, point = get_region_and_point(lat, lon)

    elevation = ee.Image('CGIAR/SRTM90_V4').select('elevation')
    era5_one_day = ee.ImageCollection('ECMWF/ERA5/DAILY').filterDate(date.advance(-1, 'day'), date.advance(1, 'day')).first()
    if era5_one_day is None:
        raise ValueError("No ERA5 data available for the specified date.")
    
    temperature = era5_one_day.select('mean_2m_air_temperature')
    u_wind = era5_one_day.select('u_component_of_wind_10m')
    v_wind = era5_one_day.select('v_component_of_wind_10m')

    date = ee.Date(date_str)
    era5_daily = ee.ImageCollection('ECMWF/ERA5/DAILY').filterDate(date.advance(-7, 'day'), date.advance(1, 'day')).toList(7)
    if era5_daily.length().getInfo() == 0:
        raise ValueError("No ERA5 data available for the specified date.")

    precipitation_daily = []
    humidity_daily = []
    for i in range(7):
        daily_image = ee.Image(era5_daily.get(i))
        precipitation_daily.append(daily_image.select('total_precipitation'))
        humidity_daily.append(daily_image.select('dewpoint_2m_temperature'))
    
    date = ee.Date(date_str)
    modis_ndvi = ee.ImageCollection("MODIS/006/MOD13A2").filterDate(date.advance(-1, 'month'), date).first().select('NDVI')
    if modis_ndvi is None:
        raise ValueError("No MODIS NDVI data available for the specified date.")
    
    date = ee.Date(date_str)
    pop_density = ee.ImageCollection("CIESIN/GPWv411/GPW_Population_Density").filterDate(date.advance(-5, 'year'), date).first().select('population_density')
    if pop_density is None:
        raise ValueError("No pop data available for the specified date.")

    date = ee.Date(date_str)
    ndwi_band = ee.ImageCollection('LANDSAT/LC08/C01/T1_32DAY_NDWI').filterDate(date.advance(-2, 'month'), date).first().select('NDWI')
    if ndwi_band is None:
        raise ValueError("No NDWI data available for the specified date.")
    
    modis_ndvi_sample = modis_ndvi.sample(point, 375).first()
    if modis_ndvi_sample and modis_ndvi_sample.get('NDVI'):
        modis_ndvi_value = modis_ndvi_sample.get('NDVI').getInfo()
    else:
        modis_ndvi_value = None

    pop_density_sample = pop_density.sample(point, 375).first()
    if pop_density_sample and pop_density_sample.get('population_density'):
        pop_density_value = pop_density_sample.get('population_density').getInfo()
    else:
        pop_density_value = None

    ndwi_band_sample = ndwi_band.sample(point, 375).first()
    if ndwi_band_sample and ndwi_band_sample.get('NDWI'):
        ndwi_band_value = ndwi_band_sample.get('NDWI').getInfo()
    else:
        ndwi_band_value = None
    
    date = ee.Date(date_str)
    burn_severity_col = ee.ImageCollection("USFS/GTAC/MTBS/annual_burn_severity_mosaics/v1").filterDate(date.advance(-3, 'year'), date)

    if burn_severity_col.size().getInfo() > 0:
        burn_severity = burn_severity_col.mosaic().select("Severity")
        burn_severity_sample = burn_severity.sample(point, 375).first()

        if burn_severity_sample:
            burn_severity_info = burn_severity_sample.getInfo()
            properties = burn_severity_info.get('properties') if burn_severity_info else None
            burn_severity_value = properties.get('Severity', None) if properties else None
        else:
            burn_severity_value = None
    else:
        burn_severity_value = None

    previous_year = date.advance(-1, 'year')
    days_gone_in_previous_year = previous_year.getRelative('day', 'year')
    days_to_subtract = days_gone_in_previous_year.subtract(1)
    start_of_previous_year = previous_year.advance(-1 * days_to_subtract.getInfo(), 'day')
    end_of_previous_year = start_of_previous_year.advance(1, 'year').advance(-1, 'day')

    fire_history_collection = ee.ImageCollection("ESA/CCI/FireCCI/5_1").filterDate(start_of_previous_year, end_of_previous_year)
    if fire_history_collection.size().getInfo() == 0:
        raise ValueError("No images available in the specified date range.")
    fire_history = fire_history_collection.mosaic().select("BurnDate")
    fire_history_sample = fire_history.sample(point, 375).first()
    if fire_history_sample:
        properties = fire_history_sample.getInfo()
        if properties:
            properties = properties.get('properties', {})
            fire_history_value = properties.get('BurnDate', None)
        else:
            fire_history_value = None
    else:
        fire_history_value = None
    
    glc30 = ee.Image("USGS/NLCD_RELEASES/2020_REL/NALCMS").select('landcover')

    # road networks
    region = point.buffer(375).bounds()
    road_networks = ee.FeatureCollection('TIGER/2016/Roads').filterBounds(region)
    total_length = road_networks.geometry().length().getInfo()
    area_km2 = region.area(30).multiply(1e-6).getInfo()
    road_density = total_length / area_km2

    datasets_region = [elevation, temperature, u_wind, v_wind, glc30] + precipitation_daily + humidity_daily

    ret_values = {}
    precipitation_index = 1
    humidity_index = 1

    for img in datasets_region:
        if img is None:
            continue
        try:
            img = img.reproject('EPSG:4326')
            value = img.reduceRegion(reducer=ee.Reducer.first(), geometry=region, scale=375, bestEffort=True).getInfo()
            k, v = extract_dict(value)
            # Handling names for daily values
            if 'total_precipitation' in k:
                k = f'precipitation_day_{precipitation_index}'
                precipitation_index += 1
            elif 'dewpoint_2m_temperature' in k:
                k = f'humidity_day_{humidity_index}'
                humidity_index += 1
            ret_values[k] = [v]
            # print(f"{k}:", v)
        except ee.EEException:
            print(f"Failed to retrieve data for {img.getInfo()['bands'][0]['id']}")
            ret_values[img.getInfo()['bands'][0]['id']] = [None]

    ret_values['NDVI'] = [modis_ndvi_value]
    ret_values['population_density'] = [pop_density_value]
    ret_values['BurnDate_PreviousYear'] = [fire_history_value]
    ret_values['ndwi_band_value'] = [ndwi_band_value]
    ret_values['burn_severity_value'] = [burn_severity_value]
    ret_values['road_density'] = road_density
    
    return pd.DataFrame(ret_values)


sem = threading.Semaphore(40)

completed_count = 0
start_time = time.time()
counter_lock = threading.Lock()

def update_progress():
    global completed_count
    global start_time

    with counter_lock:
        completed_count += 1

        elapsed_time = time.time() - start_time
        average_time_per_request = elapsed_time / completed_count
        remaining_requests = len(filtered_data) - completed_count
        estimated_remaining_time = average_time_per_request * remaining_requests

        minutes, seconds = divmod(estimated_remaining_time, 60)

        if completed_count % 100 == 0 or completed_count == len(filtered_data): 
            clear_output(wait=True)
            print(f"Estimated Time to Completion: {int(minutes)}m {int(seconds)}s")
            print(f"Completed: {completed_count}/{len(filtered_data)}")

def retrieve(observation, max_retries=3):
    with sem:
        observation_id, lat, lon, acq_date = observation
        acq_date = ee.Date(acq_date)
        
        retries = 0
        backoff_time = 1  # start with 1 second, but this can be adjusted
        while retries <= max_retries:
            try:
                features_df = retrieve_external_features(acq_date, lat, lon)
                features_df['observation_id'] = observation_id
                
                update_progress()
                
                return features_df
            except Exception as e:
                retries += 1
                if retries > max_retries:
                    print(f"Failed for observation {observation_id} after {max_retries} retries with error: {e}")
                    update_progress()
                    return None
                
                print(f"Attempt {retries} failed for observation {observation_id} with error: {e}. Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)  # sleep for backoff_time seconds
                backoff_time *= 2  # double the backoff_time for next potential retry

def main(filtered_data):
    tasks = []
    
    from multiprocessing.pool import ThreadPool
    pool = ThreadPool(40)
    
    for _, row in filtered_data.iterrows():
        observation = (row['observation_id'], row['LATITUDE'], row['LONGITUDE'], row['ACQ_DATE'])
        task = pool.apply_async(retrieve, (observation,))
        tasks.append(task)

    dfs = [task.get() for task in tasks]

    valid_dfs = [df for df in dfs if df is not None]
    result_df = pd.concat(valid_dfs, ignore_index=True)
    
    return result_df

external_obs_df = main(filtered_data)
external_obs_df.to_pickle("external_obs.pkl")

