# Concat logs to df, get event names per row, print event stats, then save to csv

# Note: 
# 1. You need to specify folder_path and contract_name
# 2. get_cached_abi does not work for proxy contract address

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

pandarallel.initialize(progress_bar=False)

folder_path = "data/aave" # specify your Google BigQuery result location
contract_name = "aave" # specify your contract name (will be used for later event csv name, i.e, {contract_name}_{evt}_raw.csv)

parent_name = os.path.basename(os.path.dirname(folder_path))
output_csv = f"{parent_name}/{contract_name}_logs_raw.csv"

def parse_json_file(file_path):
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

    num_processes = cpu_count() 
    
    with Pool(num_processes) as pool:
        file_paths = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path)]
        
        # Create a tqdm progress bar to track file processing
        print(f"Text files concatenation started.")
        with tqdm(total=len(file_paths)) as pbar:
            data_lists = []
            
            def update(*a):
                pbar.update()
            
            # Use the tqdm callback to update the progress bar
            for data_list in pool.imap_unordered(parse_json_file, file_paths):
                data_lists.append(data_list)
                update()
    
    # Ensure all processes have completed
    pool.close()
    pool.join()

    # Concatenate the data lists from each process
    combined_data_list = [item for sublist in data_lists for item in sublist]

    df = pd.DataFrame(combined_data_list)

    df = df.astype({'log_index':'int',
                    'transaction_hash':'str',
                    'transaction_index':'int',
                    'address':'str',
                    'data':'str',
                    'topics':'str',
                    'block_timestamp':'str',
                    'block_number':'int',
                    'block_hash':'str'})
    
    df['topics'] = df['topics'].parallel_apply(ast.literal_eval)

    ####################
    # ABIS & Events
    ####################

    w3 = Web3()

    contract_address = Web3.to_checksum_address('0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9')

    # proxy = get_proxy_address(w3, contract_address)
    abi = get_cached_abi(contract_address)

    contract = w3.eth.contract(address=contract_address, abi=abi)

    events = [obj for obj in abi if obj['type'] == 'event']
    event_signatures = {}
    for evt in events:
        sig = '0x' + event_abi_to_log_topic(evt).hex()
        event_signatures[sig] = evt['name']
        # print( f"{evt['name']} - {sig}" )


    ##################
    # Processing Logs
    ##################

    # Naming the events for each rows
    print('assign names to each event.')
    df['event'] = df['topics'].parallel_apply(lambda x: event_signatures.get(x[0], 'Unknown'))

    # Some stats
    print('event counts.')
    event_count = df['event'].value_counts()
    print(event_count)

    # Save to csv
    print(f'saving to {output_csv} (Could take a while)')
    df.to_csv(output_csv, index=False)



