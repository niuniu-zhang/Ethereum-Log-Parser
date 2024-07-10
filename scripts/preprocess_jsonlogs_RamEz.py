# preprocess_jsonlogs_RamEz.py
# Purpose: Concatenates large Ethereum contract logs into a CSV file with controlled RAM usage, then adds event name column
# Note: 
# - This script is a variant of preprocess_jsonlogs.py, optimized for handling extremely large logs.
# - It uses CSV module, df chunking, and explicit garbage collection for efficient, write-as-you-go operations to manage RAM usage effectively.

import os
import json
from tqdm import tqdm
import csv
import ast
import pandas as pd
from web3 import Web3
from utils import get_proxy_address, get_cached_abi, count_lines_in_file
from eth_utils import event_abi_to_log_topic
import gc 
import math
from pandarallel import pandarallel

pandarallel.initialize(progress_bar=False)

# Initialize Web3 with the provided URL
url = "" # your Ethereum node
w3 = Web3(Web3.HTTPProvider(url))

# Configuration settings for data folder and contract name
# Reads all files of type 
folder_path = "path/to/data"
file_type = ".json"
contract_name = "CONTRACT-NAME"
# Convert the provided contract address to checksum address
# add your contract address for abi fetching, if processing contract of the same type, say ERC20, whichever contract works fine.
contract_address = Web3.to_checksum_address("ABI CONTRACT ADDRESS") 

# Define the output CSV file paths
output_csv = f"" # aggregated csv
processed_output_csv = f"" # processed csv

parent_name = os.path.basename(os.path.dirname(folder_path))
# Gather all file paths from the specified folder
# file_paths = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path)]
file_paths = list()
for filename in os.listdir(folder_path):
    if filename.endswith(file_type):
        file_paths.append(os.path.join(folder_path, filename))


# Function to count all lines in each file
def count_all_lines(files):
    """
    Counts the number of lines in each file.
    Args:
        files: List of file paths.
    Returns:
        Dictionary with file paths as keys and line counts as values.
    """
    line_counts = {}
    for file in tqdm(files, desc="Counting lines in"):
        with open(file, 'r') as f:
            line_counts[file] = sum(1 for _ in f)
    return line_counts

# Main execution block
if __name__ == "__main__":
    tqdm.write("Text files concatenation started.")
    line_counts = count_all_lines(file_paths)

    # Writing data to CSV in a memory-efficient way
    with open(output_csv, 'w', newline='') as csvfile:
        with open(file_paths[0], 'r') as f:
            first_line = json.loads(f.readline())
            writer = csv.DictWriter(csvfile, fieldnames=first_line.keys())
            writer.writeheader()

        for file_path in tqdm(file_paths, desc=f"Processing files:"):
            with open(file_path, 'r') as text_file:
                for line in tqdm(text_file, total=line_counts[file_path], desc="Lines in file", leave=False):
                    data = json.loads(line)
                    writer.writerow(data)
        
    tqdm.write(f"Data wrote to {output_csv}. Mapping event names next.")

    ####################
    # ABIs & Events
    ####################

    # Retrieve ABI for the contract (different methods for proxy and non-proxy contracts)
    # a) non-proxy
    abi = get_cached_abi(contract_address)
    contract = w3.eth.contract(address=contract_address, abi=abi)
    # b) proxy
    # proxy = get_proxy_address(w3, contract_address)
    # abi = get_cached_abi(proxy)
    # contract = w3.eth.contract(address=contract_address, abi=abi)

    events = [obj for obj in abi if obj['type'] == 'event']
    event_signatures = {('0x' + event_abi_to_log_topic(evt).hex()): evt['name'] for evt in events}


    chunk_size = 10**5  # Adjust based on your system's capability
    total_rows = count_lines_in_file(output_csv)  # Total rows including header
    total_chunks = math.ceil((total_rows - 1) / chunk_size)  # Subtract 1 for header, then calculate total chunks

    # Initialize a boolean to control header writing
    first_chunk = True

    # Process each chunk with tqdm progress bar
    for chunk in tqdm(pd.read_csv(output_csv, 
                                  chunksize=chunk_size, 
                                  dtype={
                                      'log_index':'int', 
                                      'transaction_hash':'str',
                                      'transaction_index':'int', 
                                      'address':'str', 
                                      'data':'str', 
                                      'topics':'str', 
                                      'block_timestamp':'str', 
                                      'block_number':'int', 
                                      'block_hash':'str'}), total=total_chunks):

        # Convert 'topics' column to list and assign event names
        chunk['topics'] = chunk['topics'].parallel_apply(ast.literal_eval)
        chunk['event'] = chunk['topics'].parallel_apply(lambda x: event_signatures.get(x[0], 'Unknown'))

        # Append the processed chunk to the output CSV
        mode = 'a' if not first_chunk else 'w'
        chunk.to_csv(processed_output_csv, mode=mode, index=False, header=first_chunk)

        # Update the flag so that header is not written in the next iterations
        first_chunk = False

        # Free memory
        del chunk
        gc.collect()