# preprocess_jsonlogs.py
# Purpose: Concatenates Ethereum contract logs into a CSV file, enriching the logs with event names and printing event statistics.
# Note: 
# - Specify 'folder_path' for the location of Google BigQuery results and 'contract_name' for the output CSV file.
# - The function 'get_cached_abi' does not support proxy contract addresses.

import os
import json
import pandas as pd
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from web3 import Web3
from utils import get_proxy_address, get_cached_abi
from eth_utils import event_abi_to_log_topic
from pandarallel import pandarallel
import ast 

# Initialize Pandarallel for efficient parallel processing
pandarallel.initialize(progress_bar=False)

# Configuration: Define the data folder and contract name
folder_path = "data/your platform"
contract_name = "your platform"

# Define the output file path for the consolidated logs
parent_name = os.path.basename(os.path.dirname(folder_path))
output_csv = f"{parent_name}/{contract_name}_logs_raw.csv"

def parse_json_file(file_path):
    """
    Parses a JSON file and returns a list of data.
    Args:
        file_path (str): Path to the JSON file.
    Returns:
        list: A list of data extracted from the JSON file.
    """
    data_list = []
    with open(file_path, "r") as text_file:
        for line in text_file:
            try:
                data = json.loads(line)
                data_list.append(data)
            except json.JSONDecodeError as e:
                print(f"Error parsing {file_path}: {str(e)}")
    return data_list

if __name__ == "__main__":

    ####################
    # Concatenation 
    ####################

    # Determine the number of processes based on CPU count
    num_processes = cpu_count()
    
    with Pool(num_processes) as pool:
        file_paths = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path)]
        
        # Progress bar for tracking file processing
        print("Text files concatenation started.")
        with tqdm(total=len(file_paths)) as pbar:
            data_lists = []
            
            def update(*a):
                pbar.update()
            
            # Process each file in parallel and update progress bar
            for data_list in pool.imap_unordered(parse_json_file, file_paths):
                data_lists.append(data_list)
                update()
    
    pool.close()
    pool.join() 

    # Flatten the list of data lists and create a DataFrame
    combined_data_list = [item for sublist in data_lists for item in sublist]
    df = pd.DataFrame(combined_data_list)

    # Define data types for DataFrame columns
    df = df.astype({'log_index':'int', 'transaction_hash':'str', 'transaction_index':'int', 
                    'address':'str', 'data':'str', 'topics':'str', 'block_timestamp':'str', 
                    'block_number':'int', 'block_hash':'str'})

    # Convert 'topics' column to list using ast.literal_eval
    df['topics'] = df['topics'].parallel_apply(ast.literal_eval)

    ####################
    # ABIs & Events
    ####################

    # Initialize Web3 (requires a valid node for proxy address resolution)
    w3 = Web3()

    # Get the checksummed contract address from the DataFrame
    contract_address = Web3.to_checksum_address(df['address'][0])

    # Retrieve ABI for the contract (different methods for proxy and non-proxy contracts)
    abi = get_cached_abi(contract_address)
    contract = w3.eth.contract(address=contract_address, abi=abi)

    # Extract event ABIs and compute their signatures
    events = [obj for obj in abi if obj['type'] == 'event']
    event_signatures = {('0x' + event_abi_to_log_topic(evt).hex()): evt['name'] for evt in events}
    for evt_name, sig in event_signatures.items():
        print(f"{evt_name} - {sig}")

    ##################
    # Mapping Event
    ##################

    # Assign event names to each log entry based on 'topics' column
    print('Assigning names to each event.')
    df['event'] = df['topics'].parallel_apply(lambda x: event_signatures.get(x[0], 'Unknown'))

    # Print the count of each event type
    print('Event counts:')
    event_count = df['event'].value_counts()
    print(event_count)

    # Save the enriched DataFrame to a CSV file
    print(f'Saving to {output_csv} (this may take a while)')
    df.to_csv(output_csv, index=False)
