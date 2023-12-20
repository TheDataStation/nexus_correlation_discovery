import os
import pandas as pd

def count_rows(directory_path: str):
    # Initialize a variable to store the total number of rows
    total_rows = 0

    # Loop through all files in the directory
    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            # Read the CSV file and count rows
            df = pd.read_csv(os.path.join(directory_path, filename))
            total_rows += len(df)

    # Print the total number of rows
    print(f'Total number of rows in CSV files: {total_rows}')

# Specify the directory containing CSV files
directory_path = '/home/cc/resolution_aware_spatial_temporal_alignment/data/chicago_open_data_1m'
print(count_rows(directory_path))