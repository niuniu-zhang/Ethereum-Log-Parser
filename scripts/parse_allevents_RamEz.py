# parse_allevents_RamEz.py
# Purpose: Processes all Ethereum contract events (excluding 'Unknown' ones) from the consolidated logs and saves them into separate CSV files per event type.
# Note: 
# - This script is optimized for scenarios with large volumes of contract logs, ensuring efficient processing and memory management.
# - It employs DataFrame chunking and explicit garbage collection to handle large datasets effectively while maintaining optimal RAM usage.

import pandas as pd 
from utils import df_log_to_receipt, flatten_attribute_dict, get_cached_abi, count_lines_in_file
from web3 import Web3
from tqdm import tqdm 
from pandarallel import pandarallel
import gc
import os
from preprocess_jsonlogs_RamEz import output_csv, contract_name, parent_name
import math 

# Initialize Pandarallel for efficient parallel processing
pandarallel.initialize(progress_bar=False) 

# Initialize Web3 and set up contract
w3 = Web3()
contract_address = Web3.to_checksum_address("") # Contract address (leave empty if ABI is cached in previous script)
abi = get_cached_abi(contract_address)
contract = w3.eth.contract(address=contract_address, abi=abi)

# Define the chunk size for processing
chunk_size = 500000  # Adjust based on performance and available memory
total_rows = count_lines_in_file(output_csv)  # Total rows including header
total_chunks = math.ceil((total_rows - 1) / chunk_size)  # Subtract 1 for header, then calculate total chunks

# Process data in chunks
for df_chunk in tqdm(pd.read_csv(output_csv, dtype={'log_index':'int', 'transaction_hash':'str', 
                                                    'transaction_index':'int', 'address':'str', 
                                                    'data':'str', 'topics':'str', 'block_timestamp':'str', 
                                                    'block_number':'int', 'block_hash':'str', 'event':'str'}, 
                                            chunksize=chunk_size), total=total_chunks, desc=f"Parsing {contract_name} logs:"):
    
    # Group by 'event'
    for event_name, group in df_chunk.groupby('event'):
        # Process timestamps and merge with group data
        df_timestamp = (
            group[['transaction_hash', 'block_timestamp']]
            .drop_duplicates('transaction_hash')
            .rename(columns={'transaction_hash': 'transactionHash'})
        )

        # Process each log entry
        processed_data = group.parallel_apply(lambda row: flatten_attribute_dict(df_log_to_receipt(row, contract, event_name)), axis=1)
        df_temp = pd.DataFrame(processed_data.tolist())

        # Convert hex columns to hex strings
        hex_columns = ['transactionHash', 'address', 'blockHash']
        df_temp[hex_columns] = df_temp[hex_columns].parallel_applymap(lambda x: x.hex() if x else x)
        df_temp = pd.merge(df_temp, df_timestamp, on='transactionHash', how='inner')

        # File path for the event
        event_file = f"{parent_name}\{contract_name}_{event_name}.csv"

        # Write processed data to CSV
        with open(event_file, mode='a' if os.path.exists(event_file) else 'w', newline='') as file:
            df_temp.to_csv(file, index=False, header=not os.path.exists(event_file))

        print(f'CSV file for {event_name} saved')

        # Clear memory
        del group, df_temp, processed_data
        gc.collect()

    # Clear memory
    del df_chunk
    gc.collect()

print('All files processed')
