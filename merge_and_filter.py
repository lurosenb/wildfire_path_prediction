import os

import geopandas as gpd
import pandas as pd
from tqdm import tqdm

DATA_DIR = "./data"
SAVE_DIR = "./data_filtered"

YEARS = [2015] # ...

print(f"Loading VIIRS data...")
viirs_data = gpd.read_file(os.path.join(DATA_DIR, "fire_archive_SV-C2_390000.shp"), engine="pyogrio")

for year in tqdm(YEARS, desc="Merging and filtering data by year"):
    with tqdm(total=8, leave=False) as pbar:

        # Filter to year and add observation ID column
        pbar.set_description("Adding observation ID column")
        year_data = viirs_data[viirs_data["ACQ_DATE"].apply(lambda timestamp: timestamp.year) == year]
        year_data.insert(0, "observation_id", range(len(year_data)))
        pbar.update(1)

        pbar.set_description(f"Loading perimeter data for {year}")
        perimeter_data = gpd.read_file(os.path.join(DATA_DIR, "perimeters", f"US_HIST_FIRE_PERIM_{year}_DD83.shp"), engine="pyogrio")

        pbar.set_description(f"Maintain only largest perimeter for each uniquefire.")
        perimeter_data['area'] = perimeter_data.geometry.area
        indices_of_largest = perimeter_data.groupby('uniquefire')['area'].idxmax()
        perimeter_data = perimeter_data.loc[indices_of_largest]
        perimeter_data = perimeter_data.drop(columns=['area'])

        # Rename perimeter geometry so it gets kept in the join
        # perimeter_data = perimeter_data.rename(columns={"geometry": "perimiter_geometry"})
        # perimeter_data["perimeter_geometry"] = perimeter_data["geometry"]
        pbar.update(1)

        pbar.set_description("Joining VIIRS and perimeter data")
        joined_data = gpd.sjoin(year_data, perimeter_data, how="inner", predicate="within")

        # Add combined date_time column
        times = [f"{str(time)[:2]}:{str(time)[2:]}" for time in joined_data['ACQ_TIME'].astype(str).str.zfill(4)]
        dates = joined_data['ACQ_DATE'].astype(str)
        date_times = [f"{date} {time}" for date, time in zip(dates, times)]
        joined_data['date_time'] = pd.to_datetime(date_times)

        pbar.update(1)

        pbar.set_description("Computing mean observation time per perimeter")
        average_observation_day_by_perimeter = {}
        for perimeter in joined_data["uniquefire"].unique():

            date_times = joined_data[joined_data['uniquefire'] == perimeter]['date_time']
            value_counts = date_times.value_counts()
            average_obs_day = (value_counts.index.day_of_year * value_counts).sum() / value_counts.sum()

            average_observation_day_by_perimeter[perimeter] = average_obs_day

        pbar.update(1)
        
        pbar.set_description("Mapping observations to date_times")
        observation_id_to_date_time = dict(joined_data.apply(lambda row: (row["observation_id"], row["date_time"]), axis=1).values)
        pbar.update(1)

        pbar.set_description("Mapping observations to perimeters")
        observation_id_to_perimeters = joined_data.groupby("observation_id")["uniquefire"].apply(list).to_dict()
        pbar.update(1)

        def find_temporally_closest_perimeter(observation_id):
            observation_date_time = observation_id_to_date_time[observation_id]

            possible_perimeters = observation_id_to_perimeters[observation_id]

            best_avg_time_delta = 1000 # in days
            best_perimeter_id = None

            for perimeter_id in possible_perimeters:

                average_obs_day = average_observation_day_by_perimeter[perimeter_id]

                avg_time_delta = abs(observation_date_time.day_of_year - average_obs_day)

                if avg_time_delta < best_avg_time_delta:
                    best_avg_time_delta = avg_time_delta
                    best_perimeter_id = perimeter_id

            return best_perimeter_id
        
        pbar.set_description("Filtering observations to nearest perimeter")
        filtered_data = joined_data[joined_data["observation_id"].apply(find_temporally_closest_perimeter) == joined_data["uniquefire"]]
        pbar.update(1)

        # Save the dataframe
        pbar.set_description("Saving dataframe")
        filtered_data.to_pickle(os.path.join(SAVE_DIR, f"filtered_data_{year}.pkl"))
        pbar.update(1)

        # Cleanup
        del year_data
        del joined_data
        del filtered_data
