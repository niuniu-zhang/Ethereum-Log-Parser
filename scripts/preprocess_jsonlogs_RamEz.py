# preprocess_jsonlogs_RamEz.py
# Purpose: Concatenates large Ethereum contract logs into a CSV file with controlled RAM usage.
# Note: 
# - This script is a variant of preprocess_jsonlogs.py, optimized for handling extremely large logs.
# - It uses CSV module for efficient, write-as-you-go operations to manage RAM usage effectively.

import os
import json
from tqdm import tqdm
import csv
import ast
import pandas as pd
from web3 import Web3
from utils import get_proxy_address, get_cached_abi
from eth_utils import event_abi_to_log_topic

# Configuration settings for data folder and contract name
folder_path = 'data/your platform'
contract_name = "your platform"

# Define the output CSV file path
parent_name = os.path.basename(os.path.dirname(folder_path))
output_csv = f"{parent_name}/{contract_name}_logs_raw.csv"

# Gather all file paths from the specified folder
file_paths = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path)]

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
    print("Text files concatenation started.")
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
        
    print(f"Data wrote to {output_csv}. Mapping event names next.")

    ####################
    # ABIs & Events
    ####################

    # Load the data from the CSV file and process 'topics' column
    df = pd.read_csv(output_csv, dtype={'log_index':'int', 'transaction_hash':'str',
                                        'transaction_index':'int', 'address':'str',
                                        'data':'str', 'topics':'str', 'block_timestamp':'str',
                                        'block_number':'int', 'block_hash':'str'}, engine='pyarrow')

    # Convert 'topics' column to list using ast.literal_eval
    df['topics'] = df['topics'].parallel_apply(ast.literal_eval)

    print("Data loaded.")

    # Initialize Web3 and retrieve contract ABI
    url = "" # your eth node, e.g. Alchemy, Infura, needed for proxy contract 
    w3 = Web3(Web3.HTTPProvider(url))

    contract_address = Web3.to_checksum_address(df['address'][0])

    # Retrieve ABI for proxy or non-proxy contract
    proxy = get_proxy_address(w3, contract_address)
    print(f"Your proxy address is: {proxy}")
    abi = get_cached_abi(proxy)

    # If your contract is not proxy
    # abi = get_cached_abi(contract_address)

    contract = w3.eth.contract(address=contract_address, abi=abi)

    # Extract event ABIs and compute their signatures
    events = [obj for obj in abi if obj['type'] == 'event']
    event_signatures = {('0x' + event_abi_to_log_topic(evt).hex()): evt['name'] for evt in events}
    for evt_name, sig in event_signatures.items():
        print(f"{evt_name} - {sig}")

    ##################
    # Mapping Event
    ##################

    # Assigning event names to each log entry
    print('Assigning names to each event.')
    df['event'] = df['topics'].parallel_apply(lambda x: event_signatures.get(x[0], 'Unknown'))

    # Print the count of each event type
    print('Event counts:')
    event_count = df['event'].value_counts()
    print(event_count)

    # Overwriting logs with event names
    print(f'Saving to {output_csv} (this may take a while)')
    df.to_csv(output_csv, index=False)
