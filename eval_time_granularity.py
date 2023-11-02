import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pickle

file = 'data_filtered/filtered_data_2015.pkl'

with open (file, 'rb') as file:
    filtered_data = pickle.load(file)
grouped = filtered_data.groupby('uniquefire')['date_time'].unique()

def calculate_time_diffs(dates):
    sorted_dates = np.sort(dates)
    diffs = np.diff(sorted_dates)
    return diffs

time_diffs = grouped.apply(calculate_time_diffs)

all_diffs = np.concatenate(time_diffs.to_list())

hours_diffs = all_diffs.astype('timedelta64[h]').astype(int)

mean_diff = np.median(hours_diffs)
print(f"Average difference between observations: {mean_diff} hours")

hd = pd.DataFrame(hours_diffs, columns=['diffs'])
plt.hist(hd[hd['diffs'] < 100], bins=50, edgecolor='k', alpha=0.7)
plt.xlabel('Hours between observations')
plt.ylabel('Count')
plt.title('Distribution of hours between observations')
plt.show()