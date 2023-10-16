# Parse all events besides Unknown ones, then save to csv

# Note: you must run preprocess_jsonlogs.py first

import pandas as pd 
from utils import df_log_to_receipt, flatten_attribute_dict, get_cached_abi
from pandarallel import pandarallel
from web3 import Web3
from datetime import datetime
from tqdm import tqdm 
from preprocess_jsonlogs import output_csv, contract_name, parent_name

pandarallel.initialize(progress_bar=False)

# Raw log with event names
df = pd.read_csv(output_csv, dtype={'log_index':'int',
                                                  'transaction_hash':'str',
                                                  'transaction_index':'int',
                                                  'address':'str',
                                                  'data':'str',
                                                  'topics':'str',
                                                  'block_timestamp':'str',
                                                  'block_number':'int',
                                                  'block_hash':'str',
                                                  'event':'str'},
                                                  engine='pyarrow')


# Make sure abis event and log event one to one 
df.drop(df[df['event'] == 'Unknown'].index, inplace=True)

# Need time stamp
df_timestamp = (
    df[['transaction_hash', 'block_timestamp']]
    .drop_duplicates('transaction_hash')
    .rename(columns={'transaction_hash': 'transactionHash'})
)
def parse_date(timestamp):
    return datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S %Z").strftime("%Y-%m-%d") # modify here if you need higher precision

df_timestamp['date'] = df_timestamp['block_timestamp'].parallel_apply(parse_date)
df_timestamp.drop(columns=['block_timestamp'], inplace=True)



# Initiate contract object 
w3 = Web3()
contract_address = Web3.to_checksum_address(df['address'][0])
abi = get_cached_abi(contract_address)
contract = w3.eth.contract(address=contract_address, abi=abi)


# Loop through unique event
grouped_df = df.groupby('event')


for evt, group in tqdm(grouped_df, desc='Processing Events', unit='event'):
    tqdm.write(f'parsing {evt} event:')

    # Process_log return attributedict, we flatten it
    result = group.parallel_apply(lambda row: df_log_to_receipt(row, contract, evt), axis=1)
    flattened_result = result.parallel_apply(flatten_attribute_dict)

    # Pandas automatically handles col names
    df_temp = pd.DataFrame(flattened_result.tolist())

    # hex the hexbyte cols
    hex_columns = ['transactionHash', 'address', 'blockHash']
    df_temp[hex_columns] = df_temp[hex_columns].parallel_applymap(lambda x: x.hex())
    df_temp = pd.merge(df_temp, df_timestamp, on='transactionHash', how='inner')

    tqdm.write(f'{evt} event parsing finished, saving to {parent_name}/{contract_name}_{evt}_raw.csv:')

    df_temp.to_csv(f'{parent_name}/{contract_name}_{evt}_raw.csv', index=False)

    tqdm.write(f'{evt} event saved to {parent_name}/{contract_name}_{evt}_raw.csv.')





