# parse_allevents.py
# Purpose: Processes all Ethereum contract events (excluding 'Unknown' ones) from the consolidated logs and saves them into separate CSV files per event type.
# Note: 
# - Run preprocess_jsonlogs.py before executing this script.

import pandas as pd 
from utils import df_log_to_receipt, flatten_attribute_dict
from pandarallel import pandarallel
from web3 import Web3
from datetime import datetime
from hexbytes import HexBytes
from tqdm import tqdm 
from preprocess_jsonlogs import output_csv, contract_name, parsed_output, abi
import ast 

# Initialize Pandarallel for parallel processing without a progress bar
pandarallel.initialize(progress_bar=False)

# Loading the raw log data with event names
df = pd.read_csv(output_csv, dtype={'log_index':'int', 'transaction_hash':'str', 'transaction_index':'int', 
                                    'address':'str', 'data':'str', 'topics':'str', 'block_timestamp':'str', 
                                    'block_number':'int', 'block_hash':'str', 'event':'str', 'msg_sender':'str'}, engine='pyarrow')

# Removing logs where the event type is 'Unknown'
df.drop(df[df['event'] == 'Unknown'].index, inplace=True)
df.reset_index(inplace=True)

# Extracting timestamp and ensuring one-to-one relation with transaction hash
df_timestamp = (
    df[['transaction_hash', 'block_timestamp', 'msg_sender']]
    .drop_duplicates('transaction_hash')
    .rename(columns={'transaction_hash': 'transactionHash'})
)

# Convert the 'block_timestamp' column to datetime
# Assuming the timezone is always UTC
df_timestamp['block_timestamp'] = pd.to_datetime(df_timestamp['block_timestamp'], utc=True)

# Convert the datetime to Unix timestamp (seconds since the Unix epoch)
df_timestamp['block_timestamp_unix'] = df_timestamp['block_timestamp'].parallel_apply(lambda x: int(x.timestamp()))

# # Function to parse and format the date from the timestamp (optional if you need less time precision)
# def parse_date(timestamp):
#     return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S %Z").strftime("%Y-%m-%d") # Modify here for different date formats or precision

# # Apply the parse_date function to 'block_timestamp'
# df_timestamp['date'] = df_timestamp['block_timestamp'].parallel_apply(parse_date)
# df_timestamp.drop(columns=['block_timestamp'], inplace=True)

# Initialize the Web3 contract object (local operations only, no node required)
w3 = Web3()
contract_address = Web3.to_checksum_address(df['address'][0])
contract = w3.eth.contract(address=contract_address, abi=abi)

# Processing each unique event
grouped_df = df.groupby('event')

for evt, group in tqdm(grouped_df, desc='Processing Events', unit='event'):
    tqdm.write(f'Parsing {evt} event:')

    # Process logs and flatten the resulting AttributeDict
    flattened_result = group.parallel_apply(lambda row: flatten_attribute_dict(df_log_to_receipt(row, contract, evt)), axis=1)

    # Convert the list of dictionaries into a DataFrame
    df_temp = pd.DataFrame(flattened_result.tolist())

    # Converting hex byte columns to hexadecimal strings
    hex_columns = ['transactionHash', 'address', 'blockHash']
    df_temp[hex_columns] = df_temp[hex_columns].parallel_applymap(lambda x: x.hex())

    # Merging with the timestamp data
    df_temp = pd.merge(df_temp, df_timestamp, on='transactionHash', how='inner')

    # Check each cell in the first row for a backslash to identify un-hexed value
    byte_string_columns = []
    for col in df_temp.columns:
        if '\\' in str(df_temp[col].iloc[0]):
            byte_string_columns.append(col)
    
    # Byte string to byte then hex it
    for col in byte_string_columns:
        df_temp[col] = df_temp[col].parallel_apply(lambda x: ast.literal_eval(str(x)))
        df_temp[col] = df_temp[col].parallel_apply(lambda x: HexBytes(x).hex())

    # Saving the processed data to a CSV file
    tqdm.write(f'{evt} event parsing finished, saving to {parsed_output}/{contract_name}_{evt}.csv:')
    df_temp.to_csv(f'{parsed_output}/{contract_name}_{evt}.csv', index=False)

    tqdm.write(f'{evt} event saved to {parsed_output}/{contract_name}_{evt}.csv.')