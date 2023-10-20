# Concat logs to df, then save to csv. 
# Use this if your file is large, or ram capacity is limited

# Note: This script is an variant of preprocess_jsonlogs.py for
# handling extremely large logs. CSV module write as you go ensures
# the ram usage is checked. Specify your partition if you want multiple csvs!

import os
import json
from tqdm import tqdm
import csv

folder_path = 'data/usdt'
contract_name = "usdt"

parent_name = os.path.basename(os.path.dirname(folder_path))
num_partitions = 2  # Adjust as needed: you want a single csv or multiple csvs?


def divide_into_chunks(lst, num_partitions):
    N = len(lst)
    r = N % num_partitions
    base_chunk_size = (N - r) // num_partitions
    
    # For all partitions except the last
    chunks = [lst[i*base_chunk_size : (i+1)*base_chunk_size] for i in range(num_partitions - 1)]
    
    # Add the last partition which includes the remainder
    start_index = (num_partitions - 1) * base_chunk_size
    chunks.append(lst[start_index:])
    
    return chunks

file_paths = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path)]
print(f"Text files concatenation started.")

def count_all_lines(files):
    line_counts = {}
    for file in tqdm(files, desc="Counting lines in"):
        with open(file, 'r') as f:
            line_counts[file] = sum(1 for _ in f)
    return line_counts

line_counts = count_all_lines(file_paths)

for idx, chunk in enumerate(divide_into_chunks(file_paths, num_partitions), start=1):
    output_csv = f"{parent_name}/{contract_name}_logs_raw_part{idx}.csv"
    
    with open(output_csv, 'w', newline='') as csvfile:
        with open(chunk[0], 'r') as f:
            first_line = json.loads(f.readline())
            writer = csv.DictWriter(csvfile, fieldnames=first_line.keys())
            writer.writeheader()
        
        for file_path in tqdm(chunk, desc=f"Processing files for partition {idx}"):
            with open(file_path, 'r') as text_file:
                data_list = []
                for line in tqdm(text_file, total=line_counts[file_path], desc="Lines in file", leave=False):
                    data = json.loads(line)
                    data_list.append(data)
                writer.writerows(data_list)
    
    print(f"Data written to {output_csv}")
