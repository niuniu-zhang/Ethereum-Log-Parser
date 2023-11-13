# parse_event_RamEz.py
# Purpose: Parses a single, very large event from Ethereum contract logs.
# Note: 
# - Ideal for scenarios where one event type constitutes the majority of the log file.
# - Uses DataFrame chunking and explicit garbage collection to manage RAM during parallel processing.

import pandas as pd 
from utils import df_log_to_receipt, flatten_attribute_dict, get_cached_abi, count_lines_in_file
from web3 import Web3
from tqdm import tqdm 
from pandarallel import pandarallel
import gc

# Initialize Pandarallel for efficient parallel processing
pandarallel.initialize(progress_bar=False)

# Initialize Web3 and set up contract
w3 = Web3()
contract_address = Web3.to_checksum_address("") # Contract address (leave empty if ABI is cached)
abi = get_cached_abi(contract_address)
contract = w3.eth.contract(address=contract_address, abi=abi)

# Define the chunk size for processing
chunk_size = 500000  # Adjust based on performance and available memory

# File paths for input and output CSV files
input_csv = "" # Path to the CSV file containing logs of the single event
output_csv = "" # Output file path
first_chunk = True

# Calculate total number of chunks needed
total_rows = count_lines_in_file(input_csv)  # Including header
total_data_rows = total_rows - 1  # Excluding header
total_chunks = total_data_rows // chunk_size + (total_data_rows % chunk_size > 0)

# Process data in chunks
for df_chunk in tqdm(pd.read_csv(input_csv, dtype={'log_index':'int', 'transaction_hash':'str', 
                                                   'transaction_index':'int', 'address':'str', 
                                                   'data':'str', 'topics':'str', 'block_timestamp':'str', 
                                                   'block_number':'int', 'block_hash':'str', 'event':'str'}, 
                                  chunksize=chunk_size), total=total_chunks):

    # Process timestamps and merge with chunk data
    df_timestamp = (
        df_chunk[['transaction_hash', 'block_timestamp']]
        .drop_duplicates('transaction_hash')
        .rename(columns={'transaction_hash': 'transactionHash'})
    )

    # Process each log entry
    processed_data = df_chunk.parallel_apply(lambda row: flatten_attribute_dict(df_log_to_receipt(row, contract, "Transfer")), axis=1)
    df_temp = pd.DataFrame(processed_data.tolist())

    # Convert hex columns to hex strings
    hex_columns = ['transactionHash', 'address', 'blockHash']
    df_temp[hex_columns] = df_temp[hex_columns].parallel_applymap(lambda x: x.hex() if x else x)
    df_temp = pd.merge(df_temp, df_timestamp, on='transactionHash', how='inner')

    # Write processed data to CSV
    with open(output_csv, mode='a' if not first_chunk else 'w', newline='') as file:
        df_temp.to_csv(file, index=False, header=first_chunk)
    first_chunk = False

    # Clear memory
    del df_chunk, df_temp, processed_data
    gc.collect()

print('CSV file saved')
